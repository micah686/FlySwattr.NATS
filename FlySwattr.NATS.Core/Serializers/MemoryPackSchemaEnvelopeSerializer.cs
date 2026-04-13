using System.Buffers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using MemoryPack;

namespace FlySwattr.NATS.Core.Serializers;

internal static class MemoryPackSchemaEnvelopeSerializer
{
    private static readonly byte[] Magic = [0x46, 0x53, 0x4D, 0x50, 0x01, 0x00, 0x00, 0x00];

    /// <summary>
    /// When true (default), fingerprint mismatches throw an exception.
    /// When false, fingerprint mismatches emit a warning log instead.
    /// </summary>
    internal static bool EnforceSchemaFingerprint { get; set; } = true;

    /// <summary>
    /// Logger used for fingerprint mismatch warnings when enforcement is disabled.
    /// </summary>
    internal static ILogger Logger { get; set; } = NullLogger.Instance;

    public static int GetLogicalPayloadSize(ReadOnlySpan<byte> data, MemoryPackSerializerOptions? options = null)
    {
        if (!HasEnvelopePrefix(data))
        {
            return data.Length;
        }

        var envelope = MemoryPackSerializer.Deserialize<MemoryPackSchemaEnvelope>(data[Magic.Length..], options)
                       ?? throw new MemoryPackSerializationException("Missing schema envelope while measuring payload size.");

        return envelope.Payload.Length;
    }

    public static void Serialize<T>(IBufferWriter<byte> writer, T value, MemoryPackSerializerOptions? options = null)
    {
        var descriptor = MemoryPackSchemaMetadata.GetDescriptor<T>();
        var payloadBuffer = new ArrayBufferWriter<byte>();
        MemoryPackSerializer.Serialize(payloadBuffer, value, options);

        writer.Write(Magic);
        var envelope = new MemoryPackSchemaEnvelope
        {
            SchemaId = descriptor.SchemaId,
            SchemaVersion = descriptor.SchemaVersion,
            SchemaFingerprint = descriptor.SchemaFingerprint,
            Payload = payloadBuffer.WrittenSpan.ToArray()
        };

        MemoryPackSerializer.Serialize(writer, envelope, options);
    }

    public static T? Deserialize<T>(ReadOnlySpan<byte> data, MemoryPackSerializerOptions? options = null)
    {
        var descriptor = MemoryPackSchemaMetadata.GetDescriptor<T>();

        if (!HasEnvelopePrefix(data))
        {
            throw new MemoryPackSerializationException(
                $"Schema envelope missing for {descriptor.SchemaId}. This payload was produced by an older incompatible wire format.");
        }

        var envelope = MemoryPackSerializer.Deserialize<MemoryPackSchemaEnvelope>(data[Magic.Length..], options)
                       ?? throw new MemoryPackSerializationException($"Missing schema envelope for {descriptor.SchemaId}.");

        if (!string.Equals(envelope.SchemaId, descriptor.SchemaId, StringComparison.Ordinal))
        {
            throw new MemoryPackSerializationException(
                $"Schema mismatch for {descriptor.SchemaId}. Incoming schema '{envelope.SchemaId}' cannot be deserialized as '{descriptor.SchemaId}'.");
        }

        // Version-aware check: allow backward compat (newer reader, older writer)
        // but reject messages that are too old or too new.
        if (envelope.SchemaVersion > descriptor.SchemaVersion)
        {
            throw new SchemaVersionTooNewException(descriptor.SchemaId, envelope.SchemaVersion, descriptor.SchemaVersion);
        }

        if (envelope.SchemaVersion < descriptor.MinSupportedVersion)
        {
            throw new MemoryPackSerializationException(
                $"Schema version too old for {descriptor.SchemaId}. " +
                $"Incoming version {envelope.SchemaVersion} is below the minimum supported version {descriptor.MinSupportedVersion}. " +
                "The producer needs to be upgraded.");
        }

        // Fingerprint check: configurable strict vs. warning-only mode
        if (!string.Equals(envelope.SchemaFingerprint, descriptor.SchemaFingerprint, StringComparison.Ordinal))
        {
            if (EnforceSchemaFingerprint)
            {
                throw new MemoryPackSerializationException(
                    $"Schema fingerprint mismatch for {descriptor.SchemaId}. The MemoryPack contract changed without a compatible migration path.");
            }

            Logger.LogWarning(
                "Schema fingerprint mismatch for {SchemaId} (incoming: {IncomingFingerprint}, local: {LocalFingerprint}). " +
                "Proceeding with deserialization because EnforceSchemaFingerprint is disabled. " +
                "Ensure MemoryPack positional rules are followed to avoid data corruption.",
                descriptor.SchemaId, envelope.SchemaFingerprint, descriptor.SchemaFingerprint);
        }

        return MemoryPackSerializer.Deserialize<T>(envelope.Payload, options);
    }

    private static bool HasEnvelopePrefix(ReadOnlySpan<byte> data)
    {
        return data.Length >= Magic.Length && data[..Magic.Length].SequenceEqual(Magic);
    }
}

/// <summary>
/// Thrown when a message's schema version is newer than the consumer's local version,
/// indicating the consumer needs to be upgraded.
/// </summary>
public class SchemaVersionTooNewException : MemoryPackSerializationException
{
    public SchemaVersionTooNewException(string schemaId, int incomingVersion, int localVersion)
        : base($"Schema version too new for {schemaId}. " +
               $"Incoming version {incomingVersion} exceeds local version {localVersion}. " +
               "The consumer needs to be upgraded to handle this message version.")
    {
        SchemaId = schemaId;
        IncomingVersion = incomingVersion;
        LocalVersion = localVersion;
    }

    public string SchemaId { get; }
    public int IncomingVersion { get; }
    public int LocalVersion { get; }
}

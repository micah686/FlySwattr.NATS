using System.Buffers;
using MemoryPack;

namespace FlySwattr.NATS.Core.Serializers;

internal static class MemoryPackSchemaEnvelopeSerializer
{
    private static readonly byte[] Magic = [0x46, 0x53, 0x4D, 0x50, 0x01, 0x00, 0x00, 0x00];

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

        if (envelope.SchemaVersion != descriptor.SchemaVersion)
        {
            throw new MemoryPackSerializationException(
                $"Schema version mismatch for {descriptor.SchemaId}. Incoming version {envelope.SchemaVersion} does not match local version {descriptor.SchemaVersion}.");
        }

        if (!string.Equals(envelope.SchemaFingerprint, descriptor.SchemaFingerprint, StringComparison.Ordinal))
        {
            throw new MemoryPackSerializationException(
                $"Schema fingerprint mismatch for {descriptor.SchemaId}. The MemoryPack contract changed without a compatible migration path.");
        }

        return MemoryPackSerializer.Deserialize<T>(envelope.Payload, options);
    }

    private static bool HasEnvelopePrefix(ReadOnlySpan<byte> data)
    {
        return data.Length >= Magic.Length && data[..Magic.Length].SequenceEqual(Magic);
    }
}

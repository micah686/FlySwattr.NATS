using System.Buffers;
using System.Text.Json;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Abstractions.Exceptions;
using MemoryPack;
using Microsoft.Extensions.Logging;

namespace FlySwattr.NATS.Core.Serializers;

/// <summary>
/// Hybrid serializer that uses MemoryPack for high-performance internal communication
/// (types decorated with [MemoryPackable]) and falls back to System.Text.Json for
/// external integration or legacy types.
/// </summary>
public class HybridNatsSerializer : IMessageSerializer
{
    private readonly MemoryPackSerializerOptions? _memoryPackOptions;
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly int _maxPayloadSize;
    private readonly MemoryPackSchemaEnvelopeSerializer _envelopeSerializer;

    public const string ContentTypeMemoryPack = "application/x-memorypack; v=1";
    public const string ContentTypeJson = "application/json";

    public HybridNatsSerializer(
        MemoryPackSerializerOptions? memoryPackOptions = null,
        JsonSerializerOptions? jsonOptions = null,
        int maxPayloadSize = 10 * 1024 * 1024, // 10MB default
        bool enforceSchemaFingerprint = true,
        ILogger<HybridNatsSerializer>? logger = null)
    {
        _memoryPackOptions = memoryPackOptions;
        _jsonOptions = jsonOptions ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };
        if (maxPayloadSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxPayloadSize), "Max payload size must be greater than zero.");
        }
        _maxPayloadSize = maxPayloadSize;

        // Create an instance-scoped serializer with the configured settings.
        // This avoids mutating global static state, allowing different consumers
        // to use different fingerprint enforcement policies concurrently.
        _envelopeSerializer = new MemoryPackSchemaEnvelopeSerializer(
            enforceSchemaFingerprint,
            logger);
    }

    public void Serialize<T>(IBufferWriter<byte> writer, T message)
    {
        if (MemoryPackSchemaMetadata.IsMemoryPackable<T>())
        {
            // Fast path: MemoryPack for [MemoryPackable] types
            var limitingWriter = new SizeLimitingBufferWriter(writer, _maxPayloadSize);
            MemoryPackSchemaEnvelopeSerializer.Serialize(limitingWriter, message, _memoryPackOptions);
        }
        else
        {
            // Compatible path: System.Text.Json for all other types
            var limitingWriter = new SizeLimitingBufferWriter(writer, _maxPayloadSize);
            var jsonWriter = new Utf8JsonWriter(limitingWriter);
            JsonSerializer.Serialize(jsonWriter, message, _jsonOptions);
            jsonWriter.Flush();
        }
    }

    public T Deserialize<T>(ReadOnlyMemory<byte> data)
    {
        if (MemoryPackSchemaMetadata.IsMemoryPackable<T>())
        {
            try
            {
                // Fast path: MemoryPack using instance serializer with configured enforcement
                return _envelopeSerializer.Deserialize<T>(data.Span, _memoryPackOptions)
                       ?? throw new MemoryPackSerializationException($"MemoryPack deserialization returned null for type {typeof(T).Name}");
            }
            catch (Exception ex) when (ex is not MemoryPackSerializationException)
            {
                 throw new MemoryPackSerializationException($"Failed to deserialize {typeof(T).Name} with MemoryPack", ex);
            }
        }
        else
        {
            // Compatible path: System.Text.Json
            return JsonSerializer.Deserialize<T>(data.Span, _jsonOptions)
                   ?? throw new NullMessagePayloadException(
                       $"JSON deserialization returned null for type {typeof(T).Name}", null);
        }
    }

    public string GetContentType<T>()
    {
        return MemoryPackSchemaMetadata.IsMemoryPackable<T>() ? ContentTypeMemoryPack : ContentTypeJson;
    }

    /// <summary>
    /// Attempts to parse the wire format version from a content type header value.
    /// Returns true if a version parameter was found.
    /// </summary>
    public static bool TryParseContentTypeVersion(string? contentType, out int version)
    {
        version = 0;
        if (string.IsNullOrEmpty(contentType))
            return false;

        const string versionPrefix = "v=";
        var idx = contentType.IndexOf(versionPrefix, StringComparison.OrdinalIgnoreCase);
        if (idx < 0)
            return false;

        var versionSpan = contentType.AsSpan(idx + versionPrefix.Length).Trim();
        // Find the end of the version number (next ';' or end of string)
        var endIdx = versionSpan.IndexOf(';');
        if (endIdx >= 0)
            versionSpan = versionSpan[..endIdx].Trim();

        return int.TryParse(versionSpan, out version);
    }
}

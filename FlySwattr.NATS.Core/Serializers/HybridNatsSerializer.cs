using System.Buffers;
using System.Collections.Concurrent;
using System.Text.Json;
using FlySwattr.NATS.Abstractions;
using MemoryPack;

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
    
    // Cache for attribute lookups to avoid reflection on every call
    private static readonly ConcurrentDictionary<Type, bool> IsMemoryPackableCache = new();
    
    public const string ContentTypeMemoryPack = "application/x-memorypack";
    public const string ContentTypeJson = "application/json";

    public HybridNatsSerializer(
        MemoryPackSerializerOptions? memoryPackOptions = null,
        JsonSerializerOptions? jsonOptions = null,
        int maxPayloadSize = 10 * 1024 * 1024) // 10MB default
    {
        _memoryPackOptions = memoryPackOptions;
        _jsonOptions = jsonOptions ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };
        _maxPayloadSize = maxPayloadSize;
    }

    public void Serialize<T>(IBufferWriter<byte> writer, T message)
    {
        if (IsMemoryPackable<T>())
        {
            // Fast path: MemoryPack for [MemoryPackable] types
            var limitingWriter = new SizeLimitingBufferWriter(writer, _maxPayloadSize);
            MemoryPackSerializer.Serialize(limitingWriter, message, _memoryPackOptions);
        }
        else
        {
            // Compatible path: System.Text.Json for all other types
            var jsonWriter = new Utf8JsonWriter(writer);
            JsonSerializer.Serialize(jsonWriter, message, _jsonOptions);
            jsonWriter.Flush();
        }
    }

    public T Deserialize<T>(ReadOnlyMemory<byte> data)
    {
        if (IsMemoryPackable<T>())
        {
            // Fast path: MemoryPack
            return MemoryPackSerializer.Deserialize<T>(data.Span, _memoryPackOptions)
                   ?? throw new InvalidOperationException($"MemoryPack deserialization returned null for type {typeof(T).Name}");
        }
        else
        {
            // Compatible path: System.Text.Json
            return JsonSerializer.Deserialize<T>(data.Span, _jsonOptions)
                   ?? throw new InvalidOperationException($"JSON deserialization returned null for type {typeof(T).Name}");
        }
    }

    public string GetContentType<T>()
    {
        return IsMemoryPackable<T>() ? ContentTypeMemoryPack : ContentTypeJson;
    }

    /// <summary>
    /// Checks if a type is decorated with [MemoryPackable] attribute.
    /// Results are cached for performance.
    /// </summary>
    private static bool IsMemoryPackable<T>()
    {
        return IsMemoryPackableCache.GetOrAdd(typeof(T), type =>
            type.IsDefined(typeof(MemoryPackableAttribute), inherit: false));
    }
}

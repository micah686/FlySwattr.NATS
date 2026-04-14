using System.Buffers;
using System.Text.Json;
using NATS.Client.Core;

namespace FlySwattr.NATS.Core.Serializers;

/// <summary>
/// A NATS serializer registry that uses MemoryPack for types decorated with [MemoryPackable]
/// and falls back to System.Text.Json for all other types.
/// This allows mixing high-performance binary serialization for internal types with
/// JSON compatibility for DTOs and external integration.
/// </summary>
public class HybridSerializerRegistry : INatsSerializerRegistry
{
    public static readonly HybridSerializerRegistry Default = new();

    public INatsSerialize<T> GetSerializer<T>() => HybridNatsTypeSerializer<T>.Default;
    public INatsDeserialize<T> GetDeserializer<T>() => HybridNatsTypeSerializer<T>.Default;

    internal static bool IsMemoryPackable<T>()
    {
        return MemoryPackSchemaMetadata.IsMemoryPackable<T>();
    }
}

/// <summary>
/// Type-specific serializer that chooses between MemoryPack and JSON based on type attributes.
/// </summary>
internal class HybridNatsTypeSerializer<T> : INatsSerialize<T>, INatsDeserialize<T>
{
    public static readonly HybridNatsTypeSerializer<T> Default = new();

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    public void Serialize(IBufferWriter<byte> bufferWriter, T value)
    {
        if (HybridSerializerRegistry.IsMemoryPackable<T>())
        {
            MemoryPackSchemaEnvelopeSerializer.Serialize(bufferWriter, value);
        }
        else
        {
            var jsonWriter = new Utf8JsonWriter(bufferWriter);
            JsonSerializer.Serialize(jsonWriter, value, JsonOptions);
            jsonWriter.Flush();
        }
    }

    public T? Deserialize(in ReadOnlySequence<byte> buffer)
    {
        if (HybridSerializerRegistry.IsMemoryPackable<T>())
        {
            // Use the default instance (strict fingerprint enforcement, no logger).
            // Consumer-specific enforcement settings are handled by HybridNatsSerializer instances.
            return MemoryPackSchemaEnvelopeSerializer.Default.Deserialize<T>(buffer.ToArray());
        }
        else
        {
            var reader = new Utf8JsonReader(buffer);
            return JsonSerializer.Deserialize<T>(ref reader, JsonOptions);
        }
    }
}

using System.Buffers;
using MemoryPack;
using FlySwattr.NATS.Abstractions;

namespace FlySwattr.NATS.Core.Serializers;

public class MemPackSerializer(MemoryPackSerializerOptions? options = null, int maxPayloadSize = 10 * 1024 * 1024)
    : ISerializer
{
    // default 10MB

    public string ContentType => "application/x-memorypack";

    public void Serialize<T>(IBufferWriter<byte> writer, T message)
    {
        var limitingWriter = new SizeLimitingBufferWriter(writer, maxPayloadSize);
        MemoryPack.MemoryPackSerializer.Serialize(limitingWriter, message, options);
    }

    public T Deserialize<T>(ReadOnlySpan<byte> data)
    {
        return MemoryPack.MemoryPackSerializer.Deserialize<T>(data, options)!;
    }
}
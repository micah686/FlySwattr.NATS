using System.Buffers;
using NATS.Client.Core;

namespace FlySwattr.NATS.Core.Serializers;

internal class MemoryPackSerializerRegistry : INatsSerializerRegistry
{
    public INatsSerialize<T> GetSerializer<T>() => MemoryPackNatsSerializer<T>.Default;
    public INatsDeserialize<T> GetDeserializer<T>() => MemoryPackNatsSerializer<T>.Default;
}
// Helper that implements NATS interfaces using MemoryPack
internal class MemoryPackNatsSerializer<T> : INatsSerialize<T>, INatsDeserialize<T>
{
    public static readonly MemoryPackNatsSerializer<T> Default = new();
    
    public void Serialize(IBufferWriter<byte> bufferWriter, T value)
        => MemoryPack.MemoryPackSerializer.Serialize(bufferWriter, value);
    public T? Deserialize(in ReadOnlySequence<byte> buffer)
        => MemoryPack.MemoryPackSerializer.Deserialize<T>(buffer);
}
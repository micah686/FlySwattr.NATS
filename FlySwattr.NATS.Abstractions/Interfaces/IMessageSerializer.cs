using System.Buffers;
// ReSharper disable CheckNamespace
// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Abstractions;

public interface IMessageSerializer
{
    void Serialize<T>(IBufferWriter<byte> writer, T message);
    T Deserialize<T>(ReadOnlyMemory<byte> data);
    string GetContentType<T>();
}

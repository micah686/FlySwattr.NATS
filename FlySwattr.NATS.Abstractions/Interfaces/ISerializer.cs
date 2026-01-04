using System.Buffers;
// ReSharper disable CheckNamespace
// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Abstractions;

/// <summary>
/// Abstraction for pluggable serialization.
/// Allows switching between JSON, MessagePack, and MemoryPack at runtime.
/// </summary>
public interface ISerializer
{
    /// <summary>
    /// Serializes the value to a byte array.
    /// </summary>
    void Serialize<T>(IBufferWriter<byte> writer, T message);

    /// <summary>
    /// Deserializes data from a byte span.
    /// </summary>
    T Deserialize<T>(ReadOnlySpan<byte> data);

    /// <summary>
    /// The MIME content type for this serializer (e.g., "application/json").
    /// </summary>
    string ContentType { get; }
}
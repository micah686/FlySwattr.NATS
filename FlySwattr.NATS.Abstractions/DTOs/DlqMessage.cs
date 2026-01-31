using MemoryPack;

// ReSharper disable CheckNamespace
// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Abstractions;

[MemoryPackable]
public partial record DlqMessage
{
    public string OriginalStream { get; init; } = string.Empty;
    public string OriginalConsumer { get; init; } = string.Empty;
    public string OriginalSubject { get; init; } = string.Empty;
    public ulong OriginalSequence { get; init; }
    public int DeliveryCount { get; init; }
    public DateTimeOffset FailedAt { get; init; }
    public byte[] Payload { get; init; } = Array.Empty<byte>();
    public string? PayloadEncoding { get; init; }
    public string? ErrorReason { get; init; }
    public Dictionary<string, string>? OriginalHeaders { get; init; }
    public string? OriginalMessageType { get; init; }
    public string? SerializerType { get; init; }
}
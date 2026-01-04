// ReSharper disable CheckNamespace
// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Abstractions;

public interface IMessageContext<T>
{
    T Message { get; }
    string Subject { get; }
    MessageHeaders Headers { get; }
    string? ReplyTo { get; }

    Task RespondAsync<TResponse>(TResponse response, CancellationToken cancellationToken = default);
}

public interface IJsMessageContext<T> : IMessageContext<T>
{
    Task AckAsync(CancellationToken cancellationToken = default);
    Task NackAsync(TimeSpan? delay = null, CancellationToken cancellationToken = default);
    Task TermAsync(CancellationToken cancellationToken = default);
    Task InProgressAsync(CancellationToken cancellationToken = default);

    // JetStream specific metadata
    ulong Sequence { get; }
    DateTimeOffset Timestamp { get; }
    bool Redelivered { get; }
    uint NumDelivered { get; }
}

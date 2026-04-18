namespace FlySwattr.NATS.Abstractions;

/// <summary>
/// Implemented by JetStream message contexts that support registering callbacks to run
/// after the message has been successfully acknowledged. Enables after-ack cleanup
/// (e.g. claim-check object deletion) without coupling middleware to a specific context subtype.
/// </summary>
public interface IPostAckLifecycle
{
    /// <summary>
    /// Registers a callback to be invoked after the message ack succeeds.
    /// Callbacks are invoked in registration order.
    /// Best-effort: exceptions are logged and swallowed so they never suppress the ack result.
    /// </summary>
    void RegisterAfterAckCallback(Func<CancellationToken, Task> callback);
}

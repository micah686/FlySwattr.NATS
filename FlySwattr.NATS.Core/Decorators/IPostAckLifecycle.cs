namespace FlySwattr.NATS.Core.Decorators;

/// <summary>
/// Implemented by JetStream message contexts that support registering callbacks to run
/// after the message has been successfully acknowledged. This enables after-ack cleanup
/// (e.g. claim-check object deletion) without tightly coupling middleware to a specific
/// context subtype.
/// </summary>
internal interface IPostAckLifecycle
{
    /// <summary>
    /// Registers a callback to be invoked after the message ack succeeds.
    /// Callbacks are invoked in registration order.
    /// Best-effort: exceptions thrown by callbacks are logged and swallowed so they
    /// never suppress the ack result.
    /// </summary>
    void RegisterAfterAckCallback(Func<CancellationToken, Task> callback);
}

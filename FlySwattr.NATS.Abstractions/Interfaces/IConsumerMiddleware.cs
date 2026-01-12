// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Abstractions;

/// <summary>
/// Defines a middleware component for the consumer message processing pipeline.
/// Middleware components form a "Russian Doll" pipeline where each middleware
/// can execute logic before and after calling the next component.
/// </summary>
/// <typeparam name="T">The type of message being processed.</typeparam>
public interface IConsumerMiddleware<T>
{
    /// <summary>
    /// Invokes the middleware logic.
    /// </summary>
    /// <param name="context">The message context containing the message and metadata.</param>
    /// <param name="next">The delegate to invoke the next middleware or handler in the pipeline.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task InvokeAsync(IJsMessageContext<T> context, Func<Task> next, CancellationToken ct);
}

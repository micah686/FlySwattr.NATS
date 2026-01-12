using System;
using System.Threading;
using System.Threading.Tasks;

namespace FlySwattr.NATS.Abstractions;

/// <summary>
/// Defines a strategy for handling messages that cannot be processed successfully (poison messages).
/// </summary>
/// <typeparam name="T">The type of the message payload.</typeparam>
public interface IPoisonMessageHandler<T>
{
    /// <summary>
    /// Handles a poison message.
    /// </summary>
    /// <param name="context">The message context containing the message and metadata.</param>
    /// <param name="streamName">The name of the stream.</param>
    /// <param name="consumerName">The name of the consumer.</param>
    /// <param name="maxDeliveries">The maximum number of deliveries allowed for this message.</param>
    /// <param name="exception">The exception that caused the failure.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task HandleAsync(
        IJsMessageContext<T> context, 
        string streamName, 
        string consumerName, 
        long maxDeliveries,
        Exception exception, 
        CancellationToken cancellationToken);
}

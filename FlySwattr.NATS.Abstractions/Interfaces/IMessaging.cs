// ReSharper disable CheckNamespace
// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Abstractions;

/// <summary>
/// Defines a contract for a message bus handling core messaging operations.
/// </summary>
public interface IMessageBus
{
    /// <summary>
    /// Publishes a message to a specific subject.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="subject">The subject to publish to.</param>
    /// <param name="message">The message content.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task PublishAsync<T>(string subject, T message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes a message to a specific subject with custom headers.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="subject">The subject to publish to.</param>
    /// <param name="message">The message content.</param>
    /// <param name="headers">Optional headers to include with the message.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task PublishAsync<T>(string subject, T message, MessageHeaders? headers, CancellationToken cancellationToken = default);

    /// <summary>
    /// Subscribes to a subject and processes received messages using the provided handler.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="subject">The subject to subscribe to.</param>
    /// <param name="handler">The handler to process incoming messages.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task SubscribeAsync<T>(string subject, Func<IMessageContext<T>, Task> handler, string? queueGroup = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends a request and waits for a response.
    /// </summary>
    /// <typeparam name="TRequest">The type of the request message.</typeparam>
    /// <typeparam name="TResponse">The type of the response message.</typeparam>
    /// <param name="subject">The subject to send the request to.</param>
    /// <param name="request">The request message content.</param>
    /// <param name="timeout">The duration to wait for a response.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The response message, or default if no response is received.</returns>
    Task<TResponse?> RequestAsync<TRequest, TResponse>(string subject, TRequest request, TimeSpan timeout, CancellationToken cancellationToken = default);
}

/// <summary>
/// Defines a contract for publishing to JetStream.
/// </summary>
public interface IJetStreamPublisher
{
    /// <summary>
    /// Publishes a message to a JetStream subject WITHOUT specifying a message ID.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>WARNING:</b> This overload does NOT provide application-level idempotency and will throw
    /// <see cref="ArgumentException"/> in the default implementation. Use the overload with 
    /// <paramref name="messageId"/> parameter instead.
    /// </para>
    /// <para>
    /// For proper deduplication, always use <see cref="PublishAsync{T}(string, T, string?, CancellationToken)"/>
    /// with a business-key-derived message ID (e.g., "Order123-Created").
    /// </para>
    /// </remarks>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="subject">The subject to publish to.</param>
    /// <param name="message">The message content.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentException">Thrown because a messageId is required for proper idempotency.</exception>
    [Obsolete("Use the overload with messageId parameter for proper application-level idempotency. This overload throws ArgumentException.")]
    Task PublishAsync<T>(string subject, T message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes a message to a JetStream subject with a caller-supplied message ID for server-side 
    /// de-duplication across retries and restarts.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>IMPORTANT:</b> The <paramref name="messageId"/> is REQUIRED for proper application-level idempotency.
    /// Use a business-key-derived ID (e.g., "Order123-Created", "Payment-{TransactionId}-Processed") that 
    /// remains stable across retries.
    /// </para>
    /// <para>
    /// NATS JetStream uses this ID for de-duplication within its configurable deduplication window 
    /// (typically 2 minutes). If the same message ID is published multiple times within this window,
    /// only the first publish is persisted.
    /// </para>
    /// <example>
    /// Correct usage:
    /// <code>
    /// await publisher.PublishAsync("orders.created", order, $"Order-{order.Id}-Created", ct);
    /// </code>
    /// </example>
    /// </remarks>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="subject">The subject to publish to.</param>
    /// <param name="message">The message content.</param>
    /// <param name="messageId">
    /// <b>Required.</b> A stable, business-key-derived message ID for JetStream de-duplication.
    /// Must be non-null and non-empty. Examples: "Order-123-Created", "Payment-{txnId}-Processed".
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="messageId"/> is null or whitespace.</exception>
    Task PublishAsync<T>(string subject, T message, string? messageId, CancellationToken cancellationToken = default);
}

/// <summary>
/// Defines a contract for consuming from JetStream.
/// </summary>
public interface IJetStreamConsumer
{
    /// <summary>
    /// Consumes messages from a JetStream stream.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="stream">The name of the stream.</param>
    /// <param name="subject">The subject filter.</param>
    /// <param name="handler">The handler to process incoming messages.</param>
    /// <param name="options">Optional configuration for consumption behavior.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task ConsumeAsync<T>(StreamName stream, SubjectName subject, Func<IJsMessageContext<T>, Task> handler, JetStreamConsumeOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Consume messages using pull-based delivery for better per-worker back-pressure control.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="stream">The name of the stream.</param>
    /// <param name="consumer">The name of the durable consumer.</param>
    /// <param name="handler">The handler to process incoming messages.</param>
    /// <param name="options">Optional configuration for consumption behavior.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task ConsumePullAsync<T>(StreamName stream, ConsumerName consumer, Func<IJsMessageContext<T>, Task> handler, JetStreamConsumeOptions? options = null, CancellationToken cancellationToken = default);
}


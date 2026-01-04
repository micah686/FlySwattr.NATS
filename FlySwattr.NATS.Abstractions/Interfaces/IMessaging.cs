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
    /// Publishes a message to a JetStream subject.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="subject">The subject to publish to.</param>
    /// <param name="message">The message content.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task PublishAsync<T>(string subject, T message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes a message to a JetStream subject with an optional caller-supplied message id
    /// for server-side de-duplication across retries and restarts.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="subject">The subject to publish to.</param>
    /// <param name="message">The message content.</param>
    /// <param name="messageId">
    /// Optional message id used for JetStream de-duplication. When null, an id is generated per publish.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task PublishAsync<T>(string subject, T message, string? messageId, CancellationToken cancellationToken = default);
}

/// <summary>
/// Defines a contract for consuming from JetStream.
/// </summary>
public interface IJetStreamConsumer
{
    //TODO: breakout bulkheadPool, as it leaks the Resilience package
    /// <summary>
    /// Consumes messages from a JetStream stream.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="stream">The name of the stream.</param>
    /// <param name="subject">The subject filter.</param>
    /// <param name="handler">The handler to process incoming messages.</param>
    /// <param name="queueGroup">Optional queue group (durable consumer name).</param>
    /// <param name="maxDegreeOfParallelism">Optional maximum number of concurrent message processors.</param>
    /// <param name="bulkheadPool">Optional bulkhead pool name for resource isolation (e.g., "critical", "default").</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task ConsumeAsync<T>(StreamName stream, SubjectName subject, Func<IJsMessageContext<T>, Task> handler, QueueGroup? queueGroup = null, int? maxDegreeOfParallelism = null, string? bulkheadPool = null, CancellationToken cancellationToken = default);

    //TODO: breakout bulkheadPool, as it leaks the Resilience package
    /// <summary>
    /// Consume messages using pull-based delivery for better per-worker back-pressure control.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="stream">The name of the stream.</param>
    /// <param name="consumer">The name of the durable consumer.</param>
    /// <param name="handler">The handler to process incoming messages.</param>
    /// <param name="batchSize">The number of messages to pull in each batch.</param>
    /// <param name="maxDegreeOfParallelism">Optional maximum number of concurrent message processors.</param>
    /// <param name="bulkheadPool">Optional bulkhead pool name for resource isolation (e.g., "critical", "default").</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task ConsumePullAsync<T>(StreamName stream, ConsumerName consumer, Func<IJsMessageContext<T>, Task> handler, int batchSize = 10, int? maxDegreeOfParallelism = null, string? bulkheadPool = null, CancellationToken cancellationToken = default);
}


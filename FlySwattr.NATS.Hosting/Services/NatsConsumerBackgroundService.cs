using System.Threading.Channels;
using CommunityToolkit.HighPerformance;
using CommunityToolkit.HighPerformance.Buffers;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Abstractions.Exceptions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using Polly;

namespace FlySwattr.NATS.Hosting.Services;

/// <summary>
/// Production-grade background service for consuming messages from a NATS JetStream consumer
/// with channel-based backpressure, large payload offloading, DLQ handling, and resilience integration.
/// </summary>
public partial class NatsConsumerBackgroundService<T> : BackgroundService
{
    private const int MaxDlqPayloadSize = 64 * 1024; // 64KB inline limit
    private const int InitialBufferSize = 4 * 1024; // Start small to avoid over-allocation

    private readonly INatsJSConsumer _consumer;
    private readonly Func<IJsMessageContext<T>, Task> _handler;
    private readonly ILogger _logger;
    private readonly NatsJSConsumeOpts _consumeOpts;
    private readonly string _streamName;
    private readonly string _consumerName;
    private readonly int _workerCount;

    // Optional dependencies
    private readonly IJetStreamPublisher? _dlqPublisher;
    private readonly DeadLetterPolicy? _dlqPolicy;
    private readonly IMessageSerializer? _serializer;
    private readonly IObjectStore? _objectStore;
    private readonly IDlqNotificationService? _notificationService;

    // Resilience pipelines (provided by Resilience package, optional)
    private readonly ResiliencePipeline? _resiliencePipeline;

    public NatsConsumerBackgroundService(
        INatsJSConsumer consumer,
        string streamName,
        string consumerName,
        Func<IJsMessageContext<T>, Task> handler,
        NatsJSConsumeOpts consumeOpts,
        ILogger logger,
        int? maxDegreeOfParallelism = null,
        ResiliencePipeline? resiliencePipeline = null,
        IJetStreamPublisher? dlqPublisher = null,
        DeadLetterPolicy? dlqPolicy = null,
        IMessageSerializer? serializer = null,
        IObjectStore? objectStore = null,
        IDlqNotificationService? notificationService = null)
    {
        _consumer = consumer;
        _streamName = streamName;
        _consumerName = consumerName;
        _handler = handler;
        _consumeOpts = consumeOpts;
        _logger = logger;
        _workerCount = maxDegreeOfParallelism ?? consumeOpts.MaxMsgs ?? 1;

        _dlqPublisher = dlqPublisher;
        _dlqPolicy = dlqPolicy;
        _serializer = serializer;
        _objectStore = objectStore;
        _notificationService = notificationService;

        _resiliencePipeline = resiliencePipeline;
    }



    /// <summary>
    /// Factory method for creating message context. Virtual for testability.
    /// </summary>
    protected virtual IJsMessageContext<T> CreateContext(INatsJSMsg<T> msg)
    {
        return new JsMessageContextWrapper<T>(msg);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            // Channel capacity matches worker count for strict backpressure
            var channel = Channel.CreateBounded<IJsMessageContext<T>>(new BoundedChannelOptions(_workerCount)
            {
                SingleWriter = true,
                SingleReader = false,
                FullMode = BoundedChannelFullMode.Wait,
                Capacity = _workerCount
            });

            var workers = new Task[_workerCount];
            for (int i = 0; i < _workerCount; i++)
            {
                workers[i] = RunWorkerAsync(channel.Reader, stoppingToken);
            }

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await foreach (var msg in _consumer.ConsumeAsync<T>(opts: _consumeOpts, cancellationToken: stoppingToken))
                    {
                        var context = CreateContext(msg);

                        // Non-blocking write to apply backpressure
                        if (!channel.Writer.TryWrite(context))
                        {
                            // Fail fast: NAK immediately with short backoff for redistribution
                            LogBackpressure(_streamName, _consumerName);
                            await context.NackAsync(TimeSpan.FromSeconds(1), stoppingToken);
                            continue;
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    LogPullConsumerLoopFailed(_streamName, _consumerName, ex);
                    try
                    {
                        await Task.Delay(1000, stoppingToken);
                    }
                    catch (OperationCanceledException) { break; }
                }
            }

            channel.Writer.Complete();

            // Graceful shutdown with bounded timeout
            using var shutdownCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            try
            {
                await Task.WhenAll(workers).WaitAsync(shutdownCts.Token);
            }
            catch (OperationCanceledException)
            {
                LogShutdownTimeout(_streamName, _consumerName);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on graceful shutdown
        }
    }

    private async Task RunWorkerAsync(ChannelReader<IJsMessageContext<T>> reader, CancellationToken token)
    {
        try
        {
            await foreach (var context in reader.ReadAllAsync(token))
            {
                try
                {
                    await ExecuteHandlerWithResilienceAsync(context, token);
                }
                catch (NullMessagePayloadException ex)
                {
                    LogNullPayloadTermination(_streamName, _consumerName, ex);
                    try { await context.TermAsync(token); } catch { /* best effort */ }
                    continue;
                }
                catch (InvalidOperationException ex) when (ex.Message.Contains("null"))
                {
                    LogNullPayloadTermination(_streamName, _consumerName, ex);
                    try { await context.TermAsync(token); } catch { /* best effort */ }
                    continue;
                }
                catch (OperationCanceledException)
                {
                    throw; // Graceful shutdown
                }
                catch (Exception ex)
                {
                    try
                    {
                        await HandlePoisonMessageAsync(context, ex, token);
                    }
                    catch (Exception handleEx)
                    {
                        LogPoisonMessageHandlingFailed(handleEx);
                        try { await context.NackAsync(TimeSpan.FromSeconds(30), token); }
                        catch { /* Last resort */ }
                    }
                    continue;
                }

                // Ack with bounded timeout
                try
                {
                    using var ackCts = CancellationTokenSource.CreateLinkedTokenSource(token);
                    ackCts.CancelAfter(TimeSpan.FromSeconds(5));
                    await context.AckAsync(ackCts.Token);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    LogAckAbandonedDuringShutdown(_streamName, _consumerName);
                }
                catch (Exception ex)
                {
                    LogAckFailed(_streamName, _consumerName, ex.Message);
                }
            }
        }
        catch (OperationCanceledException) { /* stop */ }
        catch (Exception ex)
        {
            LogWorkerCrashed(_streamName, _consumerName, ex);
        }
    }

    private async Task ExecuteHandlerWithResilienceAsync(IJsMessageContext<T> context, CancellationToken token)
    {
        if (_resiliencePipeline != null)
        {
            await _resiliencePipeline.ExecuteAsync(async _ => await _handler(context), token);
        }
        else
        {
            await _handler(context);
        }
    }

    private async Task HandlePoisonMessageAsync(IJsMessageContext<T> context, Exception ex, CancellationToken token)
    {
        var configuredLimit = _consumer.Info.Config.MaxDeliver;
        var limitLong = configuredLimit > 0 ? configuredLimit : long.MaxValue;

        if (context.NumDelivered >= limitLong && _dlqPolicy != null)
        {
            LogDlqInitiated(_streamName, _consumerName, context.NumDelivered, limitLong, ex);

            if (_dlqPublisher != null && _serializer != null)
            {
                try
                {
                    var dlqMessage = await CreateDlqMessageAsync(context, ex, token);
                    await _dlqPublisher.PublishAsync(_dlqPolicy.TargetSubject, dlqMessage, token);

                    if (_notificationService != null)
                    {
                        await SendDlqNotificationAsync(context, ex, token);
                    }

                    await context.TermAsync(token);
                }
                catch (Exception dlqEx)
                {
                    LogDlqFailed(dlqEx);
                    await context.NackAsync(TimeSpan.FromSeconds(30), token);
                }
            }
            else
            {
                LogNoDlqAvailable();
                try { await context.TermAsync(token); } catch { /* best effort */ }
            }
        }
        else
        {
            LogHandlerFailedWithNak(_streamName, _consumerName, context.NumDelivered, ex);
            var backoffSeconds = Math.Min(30, Math.Pow(2, context.NumDelivered - 1));
            try { await context.NackAsync(TimeSpan.FromSeconds(backoffSeconds), token); } catch { /* best effort */ }
        }
    }

    private async Task<DlqMessage> CreateDlqMessageAsync(IJsMessageContext<T> context, Exception ex, CancellationToken token)
    {
        byte[] payload;
        string contentType;
        string serializerType;

        try
        {
            using var bufferWriter = new ArrayPoolBufferWriter<byte>(InitialBufferSize);
            _serializer!.Serialize(bufferWriter, context.Message);

            if (bufferWriter.WrittenCount > MaxDlqPayloadSize && _objectStore != null)
            {
                // Offload to object store
                var objectKey = $"dlq-payload/{_streamName}/{_consumerName}/{context.Sequence}-{DateTimeOffset.UtcNow.Ticks}";
                using var payloadStream = bufferWriter.WrittenMemory.AsStream();
                await _objectStore.PutAsync(objectKey, payloadStream, token);

                payload = Array.Empty<byte>();
                contentType = $"objstore://{objectKey}";
            }
            else if (bufferWriter.WrittenCount <= MaxDlqPayloadSize)
            {
                payload = bufferWriter.WrittenMemory.ToArray();
                contentType = _serializer.GetContentType<T>();
            }
            else
            {
                LogLargePayloadWithoutObjectStore(bufferWriter.WrittenCount, MaxDlqPayloadSize);
                payload = Array.Empty<byte>();
                contentType = $"truncated:size={bufferWriter.WrittenCount}";
            }

            serializerType = _serializer.GetType().FullName ?? "Unknown";
        }
        catch (Exception serializationEx)
        {
            LogDlqSerializationFailed(serializationEx);
            payload = Array.Empty<byte>();
            contentType = "application/octet-stream";
            serializerType = "Failed";
        }

        return new DlqMessage
        {
            OriginalStream = _streamName,
            OriginalConsumer = _consumerName,
            OriginalSubject = context.Subject,
            OriginalSequence = context.Sequence,
            DeliveryCount = (int)context.NumDelivered,
            FailedAt = DateTimeOffset.UtcNow,
            Payload = payload,
            PayloadEncoding = contentType,
            ErrorReason = ex.Message,
            OriginalHeaders = context.Headers.Headers,
            OriginalMessageType = typeof(T).AssemblyQualifiedName ?? typeof(T).FullName,
            SerializerType = serializerType
        };
    }

    private async Task SendDlqNotificationAsync(IJsMessageContext<T> context, Exception ex, CancellationToken token)
    {
        try
        {
            var notification = new DlqNotification(
                MessageId: $"{_streamName}-{context.Sequence}",
                OriginalStream: _streamName,
                OriginalConsumer: _consumerName,
                OriginalSubject: context.Subject,
                OriginalSequence: context.Sequence,
                DeliveryCount: (int)context.NumDelivered,
                ErrorReason: ex.Message,
                OccurredAt: DateTimeOffset.UtcNow
            );
            await _notificationService!.NotifyAsync(notification, token);
        }
        catch (Exception notifyEx)
        {
            LogDlqNotificationFailed(_streamName, _consumerName, notifyEx);
        }
    }

    // Zero-allocation logging via source generators
    [LoggerMessage(Level = LogLevel.Debug, Message = "Channel full, applying backpressure (Stream: {StreamName}, Consumer: {ConsumerName})")]
    private partial void LogBackpressure(string streamName, string consumerName);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Terminating null payload message from {StreamName}/{ConsumerName}")]
    private partial void LogNullPayloadTermination(string streamName, string consumerName, Exception exception);

    [LoggerMessage(Level = LogLevel.Error, Message = "Failed to handle poison message")]
    private partial void LogPoisonMessageHandlingFailed(Exception exception);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Ack abandoned during shutdown for {StreamName}/{ConsumerName}")]
    private partial void LogAckAbandonedDuringShutdown(string streamName, string consumerName);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Ack failed for processed message {StreamName}/{ConsumerName}: {ErrorMessage}")]
    private partial void LogAckFailed(string streamName, string consumerName, string errorMessage);

    [LoggerMessage(Level = LogLevel.Error, Message = "Worker crashed for {StreamName}/{ConsumerName}")]
    private partial void LogWorkerCrashed(string streamName, string consumerName, Exception exception);

    [LoggerMessage(Level = LogLevel.Error, Message = "Handler failed for {StreamName}/{ConsumerName} (Delivery {DeliveryCount}/{MaxDeliver}), initiating DLQ procedure")]
    private partial void LogDlqInitiated(string streamName, string consumerName, uint deliveryCount, long maxDeliver, Exception exception);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to serialize message for DLQ")]
    private partial void LogDlqSerializationFailed(Exception exception);

    [LoggerMessage(Level = LogLevel.Error, Message = "Failed to DLQ message")]
    private partial void LogDlqFailed(Exception exception);

    [LoggerMessage(Level = LogLevel.Warning, Message = "No DLQ policy, publisher, or serializer available. Terminating poison message.")]
    private partial void LogNoDlqAvailable();

    [LoggerMessage(Level = LogLevel.Warning, Message = "Large payload ({ActualSize} bytes) exceeds inline limit ({MaxSize} bytes) but no object store available. Payload will be truncated.")]
    private partial void LogLargePayloadWithoutObjectStore(int actualSize, int maxSize);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Handler failed for {StreamName}/{ConsumerName} (Delivery {DeliveryCount}), sending NAK with backoff")]
    private partial void LogHandlerFailedWithNak(string streamName, string consumerName, uint deliveryCount, Exception exception);

    [LoggerMessage(Level = LogLevel.Error, Message = "Pull consumer loop failed for {StreamName}/{ConsumerName}")]
    private partial void LogPullConsumerLoopFailed(string streamName, string consumerName, Exception exception);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Workers did not complete within shutdown timeout for {StreamName}/{ConsumerName}")]
    private partial void LogShutdownTimeout(string streamName, string consumerName);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to send DLQ notification for {StreamName}/{ConsumerName}")]
    private partial void LogDlqNotificationFailed(string streamName, string consumerName, Exception exception);
}

/// <summary>
/// Internal wrapper for INatsJSMsg to implement IJsMessageContext.
/// </summary>
internal class JsMessageContextWrapper<T> : IJsMessageContext<T>
{
    private readonly INatsJSMsg<T> _msg;

    public JsMessageContextWrapper(INatsJSMsg<T> msg)
    {
        _msg = msg;
    }

    public T Message => _msg.Data ?? throw new NullMessagePayloadException(_msg.Subject);
    public string Subject => _msg.Subject;
    public MessageHeaders Headers => _msg.Headers != null
        ? new MessageHeaders(_msg.Headers.ToDictionary(k => k.Key, k => k.Value.ToString()))
        : MessageHeaders.Empty;
    public string? ReplyTo => _msg.ReplyTo;

    public ulong Sequence => _msg.Metadata?.Sequence.Stream ?? 0;
    public DateTimeOffset Timestamp => _msg.Metadata?.Timestamp ?? DateTimeOffset.UtcNow;
    public bool Redelivered => (_msg.Metadata?.NumDelivered ?? 1) > 1;
    public uint NumDelivered => (uint)(_msg.Metadata?.NumDelivered ?? 1);

    public async Task AckAsync(CancellationToken cancellationToken = default) =>
        await _msg.AckAsync(cancellationToken: cancellationToken);

    public async Task NackAsync(TimeSpan? delay = null, CancellationToken cancellationToken = default) =>
        await _msg.NakAsync(delay: delay ?? TimeSpan.FromSeconds(5), cancellationToken: cancellationToken);

    public async Task TermAsync(CancellationToken cancellationToken = default) =>
        await _msg.AckTerminateAsync(cancellationToken: cancellationToken);

    public async Task InProgressAsync(CancellationToken cancellationToken = default) =>
        await _msg.AckProgressAsync(cancellationToken: cancellationToken);

    public async Task RespondAsync<TResponse>(TResponse response, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(ReplyTo))
            throw new InvalidOperationException("Message does not have a ReplyTo subject.");

        if (_msg is INatsMsg<T> natsMsg)
            await natsMsg.ReplyAsync(response, cancellationToken: cancellationToken);
        else
            throw new NotSupportedException("This message type does not support replies.");
    }
}
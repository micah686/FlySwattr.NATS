using System.Buffers;
using CommunityToolkit.HighPerformance;
using CommunityToolkit.HighPerformance.Buffers;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Abstractions.Exceptions;
using FlySwattr.NATS.Core;
using Microsoft.Extensions.Logging;

namespace FlySwattr.NATS.Hosting.Services;

/// <summary>
/// Default implementation of IPoisonMessageHandler that implements the Store+Notify DLQ pattern.
/// Handles transient failures with NAK/Backoff and permanent failures (or exhausted retries) 
/// by offloading to a Dead Letter Queue and optionally notifying via IDlqNotificationService.
/// </summary>
/// <typeparam name="T">The message payload type.</typeparam>
internal partial class DefaultDlqPoisonHandler<T> : IPoisonMessageHandler<T>
{
    private const int MaxDlqPayloadSize = 1024 * 1024; // 1MB - NATS default max payload
    private const int InitialBufferSize = 4 * 1024;

    private readonly IJetStreamPublisher? _dlqPublisher;
    private readonly IMessageSerializer? _serializer;
    private readonly IObjectStore? _objectStore;
    private readonly IDlqNotificationService? _notificationService;
    private readonly IDlqPolicyRegistry _dlqPolicyRegistry;
    private readonly ILogger _logger;

    public DefaultDlqPoisonHandler(
        IJetStreamPublisher? dlqPublisher,
        IMessageSerializer? serializer,
        IObjectStore? objectStore,
        IDlqNotificationService? notificationService,
        IDlqPolicyRegistry dlqPolicyRegistry,
        ILogger<DefaultDlqPoisonHandler<T>> logger)
    {
        _dlqPublisher = dlqPublisher;
        _serializer = serializer;
        _objectStore = objectStore;
        _notificationService = notificationService;
        _dlqPolicyRegistry = dlqPolicyRegistry;
        _logger = logger;
    }

    public async Task HandleAsync(
        IJsMessageContext<T> context,
        string streamName,
        string consumerName,
        long maxDeliveries,
        Exception exception,
        CancellationToken cancellationToken)
    {
        // Check for permanent failures (Validation) or exhausted retries
        bool isValidationFailure = exception is MessageValidationException;
        bool isRetryExhausted = context.NumDelivered >= maxDeliveries;

        if (isValidationFailure || isRetryExhausted)
        {
            var policy = _dlqPolicyRegistry.Get(streamName, consumerName);

            if (isValidationFailure)
            {
                LogValidationFailed(streamName, consumerName, context.Subject, exception);
            }
            else
            {
                LogDlqInitiated(streamName, consumerName, context.NumDelivered, maxDeliveries, exception);
            }

            if (policy != null && _dlqPublisher != null && _serializer != null)
            {
                try
                {
                    var dlqMessage = await CreateDlqMessageAsync(context, streamName, consumerName, exception, cancellationToken);
                    
                    // Use deterministic ID based on original stream/consumer/sequence for DLQ idempotency
                    var prefix = isValidationFailure ? "dlq-validation" : "dlq";
                    var dlqMessageId = $"{prefix}-{streamName}-{consumerName}-{context.Sequence}";
                    
                    await _dlqPublisher.PublishAsync(policy.TargetSubject, dlqMessage, dlqMessageId, cancellationToken);

                    if (_notificationService != null)
                    {
                        await SendDlqNotificationAsync(context, streamName, consumerName, exception, cancellationToken);
                    }

                    await context.TermAsync(cancellationToken);
                }
                catch (Exception dlqEx)
                {
                    LogDlqFailed(dlqEx);
                    // If DLQ fails, we NAK with a long delay to try again later, unless it was a validation failure which might loop forever if we NAK.
                    // But if DLQ fails (e.g. NATS down), we probably want to retry DLQing.
                    await context.NackAsync(TimeSpan.FromSeconds(30), cancellationToken);
                }
            }
            else
            {
                LogNoDlqAvailable();
                try { await context.TermAsync(cancellationToken); } catch { /* best effort */ }
            }
        }
        else
        {
            // Transient failure, NAK with exponential backoff
            LogHandlerFailedWithNak(streamName, consumerName, context.NumDelivered, exception);
            var backoffSeconds = Math.Min(30, Math.Pow(2, context.NumDelivered - 1));
            try { await context.NackAsync(TimeSpan.FromSeconds(backoffSeconds), cancellationToken); } catch { /* best effort */ }
        }
    }

    private async Task<DlqMessage> CreateDlqMessageAsync(
        IJsMessageContext<T> context, 
        string streamName, 
        string consumerName, 
        Exception ex, 
        CancellationToken token)
    {
        if (_serializer == null) throw new InvalidOperationException("Serializer is required for DLQ");

        byte[] payload;
        string contentType;
        string serializerType;

        try
        {
            using var bufferWriter = new ArrayPoolBufferWriter<byte>(InitialBufferSize);
            _serializer.Serialize(bufferWriter, context.Message);

            if (bufferWriter.WrittenCount > MaxDlqPayloadSize && _objectStore != null)
            {
                // Offload to object store
                var objectKey = $"dlq-payload/{streamName}/{consumerName}/{context.Sequence}-{DateTimeOffset.UtcNow.Ticks}";
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
            OriginalStream = streamName,
            OriginalConsumer = consumerName,
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

    private async Task SendDlqNotificationAsync(
        IJsMessageContext<T> context, 
        string streamName, 
        string consumerName, 
        Exception ex, 
        CancellationToken token)
    {
        try
        {
            var notification = new DlqNotification(
                MessageId: $"{streamName}-{context.Sequence}",
                OriginalStream: streamName,
                OriginalConsumer: consumerName,
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
            LogDlqNotificationFailed(streamName, consumerName, notifyEx);
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Validation failed for message on {Subject} (Stream: {StreamName}, Consumer: {ConsumerName}). Routing to DLQ.")]
    private partial void LogValidationFailed(string streamName, string consumerName, string subject, Exception exception);

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

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to send DLQ notification for {StreamName}/{ConsumerName}")]
    private partial void LogDlqNotificationFailed(string streamName, string consumerName, Exception exception);
}

using System.Buffers;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Configuration;
using FlySwattr.NATS.Core.Services;
using FlySwattr.NATS.Core.Serializers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace FlySwattr.NATS.Core.Decorators;

/// <summary>
/// Decorator that implements the Claim Check pattern for large payload offloading.
/// When a message payload exceeds the configured threshold, it is automatically 
/// offloaded to an IObjectStore and replaced with a reference header.
/// </summary>
internal class OffloadingJetStreamPublisher : IJetStreamPublisher
{
    private readonly IJetStreamPublisher _inner;
    private readonly IRawJetStreamPublisher _rawPublisher;
    private readonly IObjectStore _objectStore;
    private readonly IMessageSerializer _serializer;
    private readonly IMessageTypeAliasRegistry _typeAliasRegistry;
    private readonly PayloadOffloadingOptions _options;
    private readonly ILogger<OffloadingJetStreamPublisher> _logger;

    public OffloadingJetStreamPublisher(
        IJetStreamPublisher inner,
        IObjectStore objectStore,
        IMessageSerializer serializer,
        IMessageTypeAliasRegistry typeAliasRegistry,
        IOptions<PayloadOffloadingOptions> options,
        ILogger<OffloadingJetStreamPublisher> logger)
        : this(inner, (IRawJetStreamPublisher)inner, objectStore, serializer, typeAliasRegistry, options, logger)
    {
    }

    public OffloadingJetStreamPublisher(
        IJetStreamPublisher inner,
        IRawJetStreamPublisher rawPublisher,
        IObjectStore objectStore,
        IMessageSerializer serializer,
        IMessageTypeAliasRegistry typeAliasRegistry,
        IOptions<PayloadOffloadingOptions> options,
        ILogger<OffloadingJetStreamPublisher> logger)
    {
        _inner = inner;
        _rawPublisher = rawPublisher;
        _objectStore = objectStore;
        _serializer = serializer;
        _typeAliasRegistry = typeAliasRegistry;
        _options = options.Value;
        _logger = logger;
    }

    public async Task PublishAsync<T>(string subject, T message, string? messageId, MessageHeaders? headers = null, CancellationToken cancellationToken = default)
    {
        MessageSecurity.RejectReservedHeaders(
            headers,
            [
                _options.ClaimCheckHeaderName,
                _options.ClaimCheckTypeHeaderName,
                "Content-Type",
                "Nats-Msg-Id",
                "traceparent",
                "tracestate"
            ]);

        // Enforce messageId requirement at decorator level for consistency
        if (string.IsNullOrWhiteSpace(messageId))
        {
            throw new ArgumentException(
                "A messageId must be provided for JetStream publishing to ensure application-level idempotency. " +
                "Use a business-key-derived ID (e.g., 'Order123-Created') to enable proper de-duplication across retries.",
                nameof(messageId));
        }

        // Serialize the message to determine its size
        var bufferWriter = new ArrayBufferWriter<byte>();
        _serializer.Serialize(bufferWriter, message);
        var payload = bufferWriter.WrittenMemory;
        var payloadSizeForThreshold = MemoryPackSchemaEnvelopeSerializer.GetLogicalPayloadSize(payload.Span);

        if (payloadSizeForThreshold > _options.ThresholdBytes)
        {
            await PublishWithOffloadingAsync(subject, message, payload, messageId, headers, cancellationToken);
        }
        else
        {
            // Under threshold - pass through to inner publisher
            await _inner.PublishAsync(subject, message, messageId, headers, cancellationToken);
        }
    }

    public async Task PublishBatchAsync<T>(
        IReadOnlyList<BatchMessage<T>> messages,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messages);
        if (messages.Count == 0) return;

        // Process each message: offload if needed, then collect for batch publish
        var processedMessages = new List<BatchMessage<T>>(messages.Count);
        var uploadedKeys = new List<string>();

        try
        {
            foreach (var msg in messages)
            {
                MessageSecurity.RejectReservedHeaders(
                    msg.Headers,
                    [
                        _options.ClaimCheckHeaderName,
                        _options.ClaimCheckTypeHeaderName,
                        "Content-Type",
                        "Nats-Msg-Id",
                        "traceparent",
                        "tracestate"
                    ]);

                if (string.IsNullOrWhiteSpace(msg.MessageId))
                {
                    throw new ArgumentException(
                        "A messageId must be provided for JetStream publishing to ensure application-level idempotency.",
                        nameof(messages));
                }

                // Serialize to check size
                var bufferWriter = new ArrayBufferWriter<byte>();
                _serializer.Serialize(bufferWriter, msg.Message);
                var payload = bufferWriter.WrittenMemory;
                var payloadSize = MemoryPackSchemaEnvelopeSerializer.GetLogicalPayloadSize(payload.Span);

                if (payloadSize > _options.ThresholdBytes)
                {
                    // Upload to object store
                    var objectKey = MessageSecurity.ValidateObjectStoreKey(
                        $"{_options.ObjectKeyPrefix}/{msg.Subject}/{Guid.NewGuid():N}");
                    var claimCheckRef = $"objstore://{objectKey}";

                    using var payloadStream = new MemoryStream(payload.ToArray());
                    await _objectStore.PutAsync(objectKey, payloadStream, cancellationToken);
                    uploadedKeys.Add(objectKey);

                    var claimCheckHeaders = new Dictionary<string, string>
                    {
                        [_options.ClaimCheckHeaderName] = claimCheckRef,
                        [_options.ClaimCheckTypeHeaderName] = _typeAliasRegistry.GetAlias(typeof(T)),
                        ["Content-Type"] = _serializer.GetContentType<T>()
                    };

                    if (msg.Headers != null)
                    {
                        foreach (var header in msg.Headers.Headers)
                        {
                            claimCheckHeaders[header.Key] = header.Value;
                        }
                    }

                    // Will be published as raw empty payload with claim-check headers
                    // Since inner.PublishBatchAsync expects typed messages, delegate to individual raw publishes
                    await _rawPublisher.PublishRawAsync(
                        msg.Subject,
                        ReadOnlyMemory<byte>.Empty,
                        msg.MessageId,
                        new MessageHeaders(claimCheckHeaders),
                        cancellationToken);
                }
                else
                {
                    processedMessages.Add(msg);
                }
            }

            // Batch-publish the non-offloaded messages through the inner publisher
            if (processedMessages.Count > 0)
            {
                await _inner.PublishBatchAsync(processedMessages, cancellationToken);
            }
        }
        catch
        {
            // Compensating action: clean up any uploaded objects on failure
            foreach (var key in uploadedKeys)
            {
                try
                {
                    await _objectStore.DeleteAsync(key, cancellationToken);
                }
                catch (Exception cleanupEx)
                {
                    _logger.LogError(cleanupEx, "Failed to clean up orphaned object {ObjectKey}", key);
                }
            }
            throw;
        }
    }

    private async Task PublishWithOffloadingAsync<T>(
        string subject,
        T message,
        ReadOnlyMemory<byte> payload,
        string? messageId,
        MessageHeaders? headers,
        CancellationToken cancellationToken)
    {
        // Generate a unique object key
        var objectKey = MessageSecurity.ValidateObjectStoreKey($"{_options.ObjectKeyPrefix}/{subject}/{Guid.NewGuid():N}");
        var claimCheckRef = $"objstore://{objectKey}";

        _logger.LogDebug(
            "Offloading large payload ({Size} bytes) to object store with key {ObjectKey}",
            payload.Length,
            objectKey);

        // Upload to object store
        using var payloadStream = new MemoryStream(payload.ToArray());
        await _objectStore.PutAsync(objectKey, payloadStream, cancellationToken);

        // Build headers with the claim check reference and preserve the original content type.
        var claimCheckHeaders = new Dictionary<string, string>
        {
            [_options.ClaimCheckHeaderName] = claimCheckRef,
            [_options.ClaimCheckTypeHeaderName] = _typeAliasRegistry.GetAlias(typeof(T)),
            ["Content-Type"] = _serializer.GetContentType<T>()
        };

        // Merge any existing headers
        if (headers != null)
        {
            foreach (var header in headers.Headers)
            {
                claimCheckHeaders[header.Key] = header.Value;
            }
        }

        try
        {
            // Publish an empty payload with headers so consumers can hydrate before business deserialization.
            await _rawPublisher.PublishRawAsync(
                subject,
                ReadOnlyMemory<byte>.Empty,
                messageId,
                new MessageHeaders(claimCheckHeaders),
                cancellationToken);

            _logger.LogInformation(
                "Published large message ({Size} bytes) to {Subject} via claim check {ObjectKey}",
                payload.Length,
                subject,
                objectKey);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch
        {
            // Compensating action: clean up the orphaned payload from object store
            _logger.LogWarning(
                "Publish failed after uploading payload to object store. Attempting to clean up orphaned object {ObjectKey}",
                objectKey);

            try
            {
                await _objectStore.DeleteAsync(objectKey, cancellationToken);
                _logger.LogDebug("Successfully cleaned up orphaned object {ObjectKey}", objectKey);
            }
            catch (Exception cleanupEx)
            {
                // Log but don't mask the original exception
                _logger.LogError(
                    cleanupEx,
                    "Failed to clean up orphaned object {ObjectKey}. Manual cleanup may be required.",
                    objectKey);
            }

            throw;
        }
    }
}

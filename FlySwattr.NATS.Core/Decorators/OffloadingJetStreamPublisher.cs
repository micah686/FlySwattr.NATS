using System.Buffers;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Configuration;
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
    private readonly PayloadOffloadingOptions _options;
    private readonly ILogger<OffloadingJetStreamPublisher> _logger;

    public OffloadingJetStreamPublisher(
        IJetStreamPublisher inner,
        IObjectStore objectStore,
        IMessageSerializer serializer,
        IOptions<PayloadOffloadingOptions> options,
        ILogger<OffloadingJetStreamPublisher> logger)
        : this(inner, (IRawJetStreamPublisher)inner, objectStore, serializer, options, logger)
    {
    }

    public OffloadingJetStreamPublisher(
        IJetStreamPublisher inner,
        IRawJetStreamPublisher rawPublisher,
        IObjectStore objectStore,
        IMessageSerializer serializer,
        IOptions<PayloadOffloadingOptions> options,
        ILogger<OffloadingJetStreamPublisher> logger)
    {
        _inner = inner;
        _rawPublisher = rawPublisher;
        _objectStore = objectStore;
        _serializer = serializer;
        _options = options.Value;
        _logger = logger;
    }

    public async Task PublishAsync<T>(string subject, T message, string? messageId, MessageHeaders? headers = null, CancellationToken cancellationToken = default)
    {
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

    private async Task PublishWithOffloadingAsync<T>(
        string subject,
        T message,
        ReadOnlyMemory<byte> payload,
        string? messageId,
        MessageHeaders? headers,
        CancellationToken cancellationToken)
    {
        // Generate a unique object key
        var objectKey = $"{_options.ObjectKeyPrefix}/{subject}/{Guid.NewGuid():N}";
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

using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace FlySwattr.NATS.Core.Decorators;

/// <summary>
/// Decorator that implements the consumer side of the Claim Check pattern.
/// Automatically detects and retrieves large payloads that were offloaded to IObjectStore.
/// </summary>
public class OffloadingJetStreamConsumer : IJetStreamConsumer
{
    private readonly IJetStreamConsumer _inner;
    private readonly IObjectStore _objectStore;
    private readonly IMessageSerializer _serializer;
    private readonly PayloadOffloadingOptions _options;
    private readonly ILogger<OffloadingJetStreamConsumer> _logger;

    public OffloadingJetStreamConsumer(
        IJetStreamConsumer inner,
        IObjectStore objectStore,
        IMessageSerializer serializer,
        IOptions<PayloadOffloadingOptions> options,
        ILogger<OffloadingJetStreamConsumer> logger)
    {
        _inner = inner;
        _objectStore = objectStore;
        _serializer = serializer;
        _options = options.Value;
        _logger = logger;
    }

    public async Task ConsumeAsync<T>(
        StreamName stream,
        SubjectName subject,
        Func<IJsMessageContext<T>, Task> handler,
        QueueGroup? queueGroup = null,
        int? maxDegreeOfParallelism = null,
        int? maxConcurrency = null,
        string? bulkheadPool = null,
        CancellationToken cancellationToken = default)
    {
        // Wrap the handler to intercept ClaimCheckMessage and retrieve the actual payload
        Func<IJsMessageContext<T>, Task> wrappedHandler = async context =>
        {
            var resolvedContext = await ResolveClaimCheckAsync(context, cancellationToken);
            await handler(resolvedContext);
        };

        await _inner.ConsumeAsync(stream, subject, wrappedHandler, queueGroup, maxDegreeOfParallelism, maxConcurrency, bulkheadPool, cancellationToken);
    }

    public async Task ConsumePullAsync<T>(
        StreamName stream,
        ConsumerName consumer,
        Func<IJsMessageContext<T>, Task> handler,
        int batchSize = 10,
        int? maxDegreeOfParallelism = null,
        int? maxConcurrency = null,
        string? bulkheadPool = null,
        CancellationToken cancellationToken = default)
    {
        // Wrap the handler to intercept ClaimCheckMessage and retrieve the actual payload
        Func<IJsMessageContext<T>, Task> wrappedHandler = async context =>
        {
            var resolvedContext = await ResolveClaimCheckAsync(context, cancellationToken);
            await handler(resolvedContext);
        };

        await _inner.ConsumePullAsync(stream, consumer, wrappedHandler, batchSize, maxDegreeOfParallelism, maxConcurrency, bulkheadPool, cancellationToken);
    }

    private async Task<IJsMessageContext<T>> ResolveClaimCheckAsync<T>(
        IJsMessageContext<T> context,
        CancellationToken cancellationToken)
    {
        // Check if this is a ClaimCheckMessage by examining the actual message content
        // The message will be deserialized as type T, but if T is object or the publisher
        // sent a ClaimCheckMessage, we need to detect and handle it
        
        // First, check if headers indicate a claim check (for header-based approach)
        if (context.Headers.Headers.TryGetValue(_options.ClaimCheckHeaderName, out var claimCheckRef))
        {
            return await ResolveFromHeaderAsync<T>(context, claimCheckRef, cancellationToken);
        }

        // Check if the message itself is a ClaimCheckMessage
        // This handles the case where the message was serialized as ClaimCheckMessage
        try
        {
            if (context.Message is ClaimCheckMessage claimCheck)
            {
                return await ResolveFromClaimCheckMessageAsync<T>(context, claimCheck, cancellationToken);
            }
        }
        catch
        {
            // Not a ClaimCheckMessage - pass through
        }

        // Not a claim check message - return original context
        return context;
    }

    private async Task<IJsMessageContext<T>> ResolveFromHeaderAsync<T>(
        IJsMessageContext<T> context,
        string claimCheckRef,
        CancellationToken cancellationToken)
    {
        var objectKey = ExtractObjectKey(claimCheckRef);
        
        _logger.LogDebug("Resolving claim check from header: {ObjectKey}", objectKey);

        var payload = await DownloadPayloadAsync(objectKey, cancellationToken);
        var message = _serializer.Deserialize<T>(payload);

        return new OffloadingMessageContext<T>(context, message);
    }

    private async Task<IJsMessageContext<T>> ResolveFromClaimCheckMessageAsync<T>(
        IJsMessageContext<T> context,
        ClaimCheckMessage claimCheck,
        CancellationToken cancellationToken)
    {
        var objectKey = ExtractObjectKey(claimCheck.ObjectStoreRef);
        
        _logger.LogDebug(
            "Resolving claim check message: {ObjectKey} (Original type: {OriginalType}, Size: {Size})",
            objectKey,
            claimCheck.OriginalType,
            claimCheck.OriginalSize);

        var payload = await DownloadPayloadAsync(objectKey, cancellationToken);
        var message = _serializer.Deserialize<T>(payload);

        _logger.LogInformation(
            "Retrieved large message ({Size} bytes) from claim check {ObjectKey}",
            payload.Length,
            objectKey);

        return new OffloadingMessageContext<T>(context, message);
    }

    private async Task<ReadOnlyMemory<byte>> DownloadPayloadAsync(string objectKey, CancellationToken cancellationToken)
    {
        using var memoryStream = new MemoryStream();
        await _objectStore.GetAsync(objectKey, memoryStream, cancellationToken);
        return memoryStream.ToArray();
    }

    private static string ExtractObjectKey(string reference)
    {
        // Reference format: "objstore://claimcheck/subject/guid"
        const string prefix = "objstore://";
        if (reference.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
        {
            return reference.Substring(prefix.Length);
        }
        return reference;
    }
}

/// <summary>
/// Message context wrapper that holds a resolved claim check payload.
/// </summary>
internal class OffloadingMessageContext<T> : IJsMessageContext<T>
{
    private readonly IJsMessageContext<T> _inner;
    private readonly T _resolvedMessage;

    public OffloadingMessageContext(IJsMessageContext<T> inner, T resolvedMessage)
    {
        _inner = inner;
        _resolvedMessage = resolvedMessage;
    }

    // Return the resolved message instead of the claim check wrapper
    public T Message => _resolvedMessage;

    // Delegate all other properties and methods to the inner context
    public string Subject => _inner.Subject;
    public MessageHeaders Headers => _inner.Headers;
    public string? ReplyTo => _inner.ReplyTo;
    public ulong Sequence => _inner.Sequence;
    public DateTimeOffset Timestamp => _inner.Timestamp;
    public bool Redelivered => _inner.Redelivered;
    public uint NumDelivered => _inner.NumDelivered;

    public Task AckAsync(CancellationToken cancellationToken = default) => _inner.AckAsync(cancellationToken);
    public Task NackAsync(TimeSpan? delay = null, CancellationToken cancellationToken = default) => _inner.NackAsync(delay, cancellationToken);
    public Task TermAsync(CancellationToken cancellationToken = default) => _inner.TermAsync(cancellationToken);
    public Task InProgressAsync(CancellationToken cancellationToken = default) => _inner.InProgressAsync(cancellationToken);
    public Task RespondAsync<TResponse>(TResponse response, CancellationToken cancellationToken = default) => _inner.RespondAsync(response, cancellationToken);
}

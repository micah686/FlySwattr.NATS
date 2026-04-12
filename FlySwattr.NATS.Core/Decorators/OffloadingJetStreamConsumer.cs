using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace FlySwattr.NATS.Core.Decorators;

internal class OffloadingJetStreamConsumer : IJetStreamConsumer
{
    private readonly IRawJetStreamConsumer _rawConsumer;
    private readonly IObjectStore _objectStore;
    private readonly IMessageSerializer _serializer;
    private readonly PayloadOffloadingOptions _options;
    private readonly ILogger<OffloadingJetStreamConsumer> _logger;

    public OffloadingJetStreamConsumer(
        IRawJetStreamConsumer rawConsumer,
        IObjectStore objectStore,
        IMessageSerializer serializer,
        IOptions<PayloadOffloadingOptions> options,
        ILogger<OffloadingJetStreamConsumer> logger)
    {
        _rawConsumer = rawConsumer;
        _objectStore = objectStore;
        _serializer = serializer;
        _options = options.Value;
        _logger = logger;
    }

    public Task ConsumeAsync<T>(
        StreamName stream,
        SubjectName subject,
        Func<IJsMessageContext<T>, Task> handler,
        JetStreamConsumeOptions? options = null,
        CancellationToken cancellationToken = default)
        => _rawConsumer.ConsumeRawAsync(
            stream,
            subject,
            context => ResolveAndHandleAsync(context, handler, cancellationToken),
            options,
            cancellationToken);

    public Task ConsumePullAsync<T>(
        StreamName stream,
        ConsumerName consumer,
        Func<IJsMessageContext<T>, Task> handler,
        JetStreamConsumeOptions? options = null,
        CancellationToken cancellationToken = default)
        => _rawConsumer.ConsumePullRawAsync(
            stream,
            consumer,
            context => ResolveAndHandleAsync(context, handler, cancellationToken),
            options,
            cancellationToken);

    private async Task ResolveAndHandleAsync<T>(
        IJsMessageContext<byte[]> context,
        Func<IJsMessageContext<T>, Task> handler,
        CancellationToken cancellationToken)
    {
        var resolvedContext = await ResolveClaimCheckAsync<T>(context, cancellationToken);
        await handler(resolvedContext);
    }

    internal async Task<IJsMessageContext<T>> ResolveClaimCheckAsync<T>(
        IJsMessageContext<byte[]> context,
        CancellationToken cancellationToken)
    {
        if (context.Headers.Headers.TryGetValue(_options.ClaimCheckHeaderName, out var claimCheckRef))
        {
            return await ResolveFromHeaderAsync<T>(context, claimCheckRef, cancellationToken);
        }

        var message = _serializer.Deserialize<T>(context.Message);
        return new OffloadingMessageContext<T>(context, message);
    }

    private async Task<IJsMessageContext<T>> ResolveFromHeaderAsync<T>(
        IJsMessageContext<byte[]> context,
        string claimCheckRef,
        CancellationToken cancellationToken)
    {
        var objectKey = ExtractObjectKey(claimCheckRef);
        _logger.LogDebug("Resolving claim check from header: {ObjectKey}", objectKey);
        var payload = await DownloadPayloadAsync(objectKey, cancellationToken);
        var message = _serializer.Deserialize<T>(payload);
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
        const string prefix = "objstore://";
        return reference.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)
            ? reference[prefix.Length..]
            : reference;
    }
}

internal class OffloadingMessageContext<T> : IJsMessageContext<T>
{
    private readonly IMessageContext<byte[]> _inner;
    private readonly T _resolvedMessage;

    public OffloadingMessageContext(IMessageContext<byte[]> inner, T resolvedMessage)
    {
        _inner = inner;
        _resolvedMessage = resolvedMessage;
    }

    public T Message => _resolvedMessage;
    public string Subject => _inner.Subject;
    public MessageHeaders Headers => _inner.Headers;
    public string? ReplyTo => _inner.ReplyTo;

    public Task RespondAsync<TResponse>(TResponse response, CancellationToken cancellationToken = default)
        => _inner.RespondAsync(response, cancellationToken);

    public Task AckAsync(CancellationToken cancellationToken = default)
        => ((IJsMessageContext<byte[]>)_inner).AckAsync(cancellationToken);

    public Task NackAsync(TimeSpan? delay = null, CancellationToken cancellationToken = default)
        => ((IJsMessageContext<byte[]>)_inner).NackAsync(delay, cancellationToken);

    public Task TermAsync(CancellationToken cancellationToken = default)
        => ((IJsMessageContext<byte[]>)_inner).TermAsync(cancellationToken);

    public Task InProgressAsync(CancellationToken cancellationToken = default)
        => ((IJsMessageContext<byte[]>)_inner).InProgressAsync(cancellationToken);

    public ulong Sequence => ((IJsMessageContext<byte[]>)_inner).Sequence;
    public DateTimeOffset Timestamp => ((IJsMessageContext<byte[]>)_inner).Timestamp;
    public bool Redelivered => ((IJsMessageContext<byte[]>)_inner).Redelivered;
    public uint NumDelivered => ((IJsMessageContext<byte[]>)_inner).NumDelivered;
}

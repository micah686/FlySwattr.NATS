using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Abstractions.Exceptions;
using FlySwattr.NATS.Core.Configuration;
using FlySwattr.NATS.Core.Services;
using FlySwattr.NATS.Core.Serializers;
using FlySwattr.NATS.Core.Telemetry;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Core;

internal interface IRawJetStreamPublisher
{
    Task PublishRawAsync(string subject, ReadOnlyMemory<byte> payload, string? messageId, MessageHeaders? headers = null, CancellationToken cancellationToken = default);
}

/// <summary>
/// Allows a decorator to publish bytes that were already serialized during a size-check pass,
/// while still having all typed-publish headers (Content-Type, version, schema metadata, telemetry)
/// attached by <see cref="NatsJetStreamBus"/>. This avoids a second serialization in the
/// below-threshold path of <see cref="FlySwattr.NATS.Core.Decorators.OffloadingJetStreamPublisher"/>.
/// </summary>
internal interface IPreserializedJetStreamPublisher
{
    Task PublishBytesAsync<T>(
        string subject,
        ReadOnlyMemory<byte> serializedPayload,
        string? messageId,
        MessageHeaders? headers = null,
        CancellationToken cancellationToken = default);
}

internal interface IRawJetStreamConsumer
{
    Task ConsumeRawAsync(
        StreamName stream,
        SubjectName subject,
        Func<IJsMessageContext<byte[]>, Task> handler,
        JetStreamConsumeOptions? options = null,
        CancellationToken cancellationToken = default);

    Task ConsumePullRawAsync(
        StreamName stream,
        ConsumerName consumer,
        Func<IJsMessageContext<byte[]>, Task> handler,
        JetStreamConsumeOptions? options = null,
        CancellationToken cancellationToken = default);
}

public class NatsJetStreamBus : IJetStreamPublisher, IJetStreamConsumer, IRawJetStreamPublisher, IRawJetStreamConsumer, IPreserializedJetStreamPublisher, IAsyncDisposable
{
    private readonly string[] _reservedPublishHeaders;
    private readonly string[] _reservedRawPublishHeaders;

    private readonly INatsJSContext _jsContext;
    private readonly ILogger<NatsJetStreamBus> _logger;
    private readonly IMessageSerializer _serializer;
    private readonly BackgroundTaskManager _backgroundTaskManager;
    private readonly WireCompatibilityOptions _wireOptions;
    private int _disposed;

    public NatsJetStreamBus(
        INatsJSContext jsContext,
        ILogger<NatsJetStreamBus> logger,
        IMessageSerializer serializer,
        IOptions<WireCompatibilityOptions>? wireOptions = null)
        : this(
            jsContext,
            logger,
            serializer,
            new BackgroundTaskManager(NullLogger<BackgroundTaskManager>.Instance),
            wireOptions)
    {
    }

    internal NatsJetStreamBus(
        INatsJSContext jsContext,
        ILogger<NatsJetStreamBus> logger,
        IMessageSerializer serializer,
        BackgroundTaskManager backgroundTaskManager,
        IOptions<WireCompatibilityOptions>? wireOptions = null)
    {
        _jsContext = jsContext;
        _logger = logger;
        _serializer = serializer;
        _backgroundTaskManager = backgroundTaskManager;
        _wireOptions = wireOptions?.Value ?? new WireCompatibilityOptions();

        // Build reserved header lists including the version header
        _reservedPublishHeaders =
        [
            "Content-Type",
            "Nats-Msg-Id",
            "traceparent",
            "tracestate",
            _wireOptions.VersionHeaderName
        ];
        _reservedRawPublishHeaders =
        [
            "Nats-Msg-Id",
            "traceparent",
            "tracestate",
            _wireOptions.VersionHeaderName
        ];
    }

    public async Task PublishAsync<T>(string subject, T message, string? messageId, MessageHeaders? headers = null, CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();
        MessageSecurity.ValidatePublishSubject(subject);
        
        // Require a caller-provided message ID for true application-level idempotency.
        // Auto-generating GUIDs would defeat JetStream's deduplication on retries.
        if (string.IsNullOrWhiteSpace(messageId))
        {
            throw new ArgumentException(
                "A messageId must be provided for JetStream publishing to ensure application-level idempotency. " +
                "Use a business-key-derived ID (e.g., 'Order123-Created') to enable proper de-duplication across retries.",
                nameof(messageId));
        }

        using var activity = NatsTelemetry.ActivitySource.StartActivity($"{subject} publish", ActivityKind.Producer);
        NatsTelemetry.ApplyMessagingTags(activity, subject);
        activity?.SetTag(NatsTelemetry.MessagingMessageId, messageId);

        var natsHeaders = new NatsHeaders
        {
            ["Content-Type"] = _serializer.GetContentType<T>(),
            ["Nats-Msg-Id"] = messageId,
            [_wireOptions.VersionHeaderName] = _wireOptions.ProtocolVersion.ToString()
        };

        // Merge custom headers if provided
        var validatedHeaders = MessageSecurity.BuildValidatedHeaders(headers, _reservedPublishHeaders, paramName: nameof(headers));
        foreach (var header in validatedHeaders)
        {
            natsHeaders[header.Key] = header.Value;
        }
        
        NatsTelemetry.InjectTraceContext(activity, natsHeaders);
        MemoryPackSchemaMetadata.AddHeadersIfNeeded<T>(natsHeaders);

        // Pass the message directly to NATS.Net - let the configured MemoryPackSerializerRegistry
        // handle serialization. Do NOT pre-serialize here as that causes double-serialization.
        var ack = await _jsContext.PublishAsync(
            subject,
            message,
            headers: natsHeaders,
            opts: new NatsJSPubOpts { MsgId = messageId },
            cancellationToken: cancellationToken);

        ack.EnsureSuccess();
        
        // Record metrics
        stopwatch.Stop();
        var tags = NatsTelemetry.CreateMessagingTags(subject);
        NatsTelemetry.MessagesPublished.Add(1, tags);
        NatsTelemetry.PublishDuration.Record(stopwatch.Elapsed.TotalMilliseconds, tags);

        _logger.LogDebug("Published JetStream message to {Subject} with MsgId {MsgId}", subject, messageId);
    }

    public async Task PublishBatchAsync<T>(
        IReadOnlyList<BatchMessage<T>> messages,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messages);
        if (messages.Count == 0) return;

        // Validate all messages upfront
        for (var i = 0; i < messages.Count; i++)
        {
            var msg = messages[i];
            MessageSecurity.ValidatePublishSubject(msg.Subject);
            MessageSecurity.BuildValidatedHeaders(msg.Headers, _reservedPublishHeaders, paramName: nameof(messages));

            if (string.IsNullOrWhiteSpace(msg.MessageId))
            {
                throw new ArgumentException(
                    $"Batch message at index {i} has a null or empty messageId. " +
                    "A messageId must be provided for JetStream publishing to ensure application-level idempotency.",
                    nameof(messages));
            }
        }

        // Publish all concurrently
        var tasks = new Task[messages.Count];
        for (var i = 0; i < messages.Count; i++)
        {
            var msg = messages[i];
            tasks[i] = PublishAsync(msg.Subject, msg.Message, msg.MessageId, msg.Headers, cancellationToken);
        }

        // Collect failures
        var exceptions = new List<Exception>();
        for (var i = 0; i < tasks.Length; i++)
        {
            try
            {
                await tasks[i];
            }
            catch (Exception ex)
            {
                exceptions.Add(new InvalidOperationException(
                    $"Batch message at index {i} (MessageId: '{messages[i].MessageId}') failed: {ex.Message}", ex));
            }
        }

        if (exceptions.Count > 0)
        {
            throw new AggregateException(
                $"{exceptions.Count} of {messages.Count} batch message(s) failed to publish.", exceptions);
        }
    }

    public async Task PublishRawAsync(string subject, ReadOnlyMemory<byte> payload, string? messageId, MessageHeaders? headers = null, CancellationToken cancellationToken = default)
    {
        MessageSecurity.ValidatePublishSubject(subject);

        if (string.IsNullOrWhiteSpace(messageId))
        {
            throw new ArgumentException(
                "A messageId must be provided for JetStream publishing to ensure application-level idempotency. " +
                "Use a business-key-derived ID (e.g., 'Order123-Created') to enable proper de-duplication across retries.",
                nameof(messageId));
        }

        var natsHeaders = new NatsHeaders
        {
            ["Nats-Msg-Id"] = messageId,
            [_wireOptions.VersionHeaderName] = _wireOptions.ProtocolVersion.ToString()
        };

        var validatedHeaders = MessageSecurity.BuildValidatedHeaders(headers, _reservedRawPublishHeaders, paramName: nameof(headers));
        foreach (var header in validatedHeaders)
        {
            natsHeaders[header.Key] = header.Value;
        }

        var ack = await _jsContext.PublishAsync(
            subject,
            payload.ToArray(),
            serializer: RawByteArraySerializer.Instance,
            headers: natsHeaders,
            opts: new NatsJSPubOpts { MsgId = messageId },
            cancellationToken: cancellationToken);

        ack.EnsureSuccess();
    }

    /// <summary>
    /// Publishes bytes that were already serialized by the caller (e.g. during a size-check pass)
    /// and adds the same headers as <see cref="PublishAsync{T}"/>: Content-Type, message-ID, wire
    /// version, MemoryPack schema metadata, and distributed trace context.  This avoids a second
    /// serialization when the payload is already known to be below the offloading threshold.
    /// </summary>
    public async Task PublishBytesAsync<T>(
        string subject,
        ReadOnlyMemory<byte> serializedPayload,
        string? messageId,
        MessageHeaders? headers = null,
        CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();
        MessageSecurity.ValidatePublishSubject(subject);

        if (string.IsNullOrWhiteSpace(messageId))
        {
            throw new ArgumentException(
                "A messageId must be provided for JetStream publishing to ensure application-level idempotency.",
                nameof(messageId));
        }

        using var activity = NatsTelemetry.ActivitySource.StartActivity($"{subject} publish", ActivityKind.Producer);
        NatsTelemetry.ApplyMessagingTags(activity, subject);
        activity?.SetTag(NatsTelemetry.MessagingMessageId, messageId);

        var natsHeaders = new NatsHeaders
        {
            ["Content-Type"] = _serializer.GetContentType<T>(),
            ["Nats-Msg-Id"] = messageId,
            [_wireOptions.VersionHeaderName] = _wireOptions.ProtocolVersion.ToString()
        };

        var validatedHeaders = MessageSecurity.BuildValidatedHeaders(headers, _reservedPublishHeaders, paramName: nameof(headers));
        foreach (var header in validatedHeaders)
        {
            natsHeaders[header.Key] = header.Value;
        }

        NatsTelemetry.InjectTraceContext(activity, natsHeaders);
        MemoryPackSchemaMetadata.AddHeadersIfNeeded<T>(natsHeaders);

        var ack = await _jsContext.PublishAsync(
            subject,
            serializedPayload.ToArray(),
            serializer: RawByteArraySerializer.Instance,
            headers: natsHeaders,
            opts: new NatsJSPubOpts { MsgId = messageId },
            cancellationToken: cancellationToken);

        ack.EnsureSuccess();

        stopwatch.Stop();
        var tags = NatsTelemetry.CreateMessagingTags(subject);
        NatsTelemetry.MessagesPublished.Add(1, tags);
        NatsTelemetry.PublishDuration.Record(stopwatch.Elapsed.TotalMilliseconds, tags);

        _logger.LogDebug("Published pre-serialized JetStream message to {Subject} with MsgId {MsgId}", subject, messageId);
    }

    public Task ConsumeAsync<T>(
        StreamName stream,
        SubjectName subject,
        Func<IJsMessageContext<T>, Task> handler,
        JetStreamConsumeOptions? options = null,
        CancellationToken cancellationToken = default)
        => ConsumeAsyncInternal(stream, subject, handler, options, cancellationToken);

    public Task ConsumePullAsync<T>(
        StreamName stream,
        ConsumerName consumer,
        Func<IJsMessageContext<T>, Task> handler,
        JetStreamConsumeOptions? options = null,
        CancellationToken cancellationToken = default)
        => ConsumePullAsyncInternal(stream, consumer, handler, options, cancellationToken);

    public Task ConsumeRawAsync(
        StreamName stream,
        SubjectName subject,
        Func<IJsMessageContext<byte[]>, Task> handler,
        JetStreamConsumeOptions? options = null,
        CancellationToken cancellationToken = default)
        => ConsumeInternalAsync(stream, subject, null, handler, options, cancellationToken, RawByteArraySerializer.Instance);

    public Task ConsumePullRawAsync(
        StreamName stream,
        ConsumerName consumer,
        Func<IJsMessageContext<byte[]>, Task> handler,
        JetStreamConsumeOptions? options = null,
        CancellationToken cancellationToken = default)
        => ConsumeInternalAsync(stream, null, consumer, handler, options, cancellationToken, RawByteArraySerializer.Instance);

    private Task ConsumeInternalAsync<T>(
        StreamName stream,
        SubjectName? subject,
        ConsumerName? consumer,
        Func<IJsMessageContext<T>, Task> handler,
        JetStreamConsumeOptions? options,
        CancellationToken cancellationToken,
        INatsDeserialize<T>? deserializer = null)
    {
        if (consumer is null)
        {
            return ConsumeAsyncInternal(stream, subject ?? throw new ArgumentNullException(nameof(subject)), handler, options, cancellationToken, deserializer);
        }

        return ConsumePullAsyncInternal(stream, consumer ?? throw new ArgumentNullException(nameof(consumer)), handler, options, cancellationToken, deserializer);
    }

    private async Task ConsumeAsyncInternal<T>(
        StreamName stream,
        SubjectName subject,
        Func<IJsMessageContext<T>, Task> handler,
        JetStreamConsumeOptions? options,
        CancellationToken cancellationToken,
        INatsDeserialize<T>? deserializer = null)
    {
        var opts = options ?? JetStreamConsumeOptions.Default;
        ValidateConsumeOptions(opts);
        string? consumerName = opts.DurableName?.Value ?? opts.LegacyDurableName;

        try
        {
            INatsJSConsumer consumer;
            if (!string.IsNullOrEmpty(consumerName))
            {
                consumer = await _jsContext.GetConsumerAsync(stream.Value, consumerName, cancellationToken);
            }
            else
            {
                var config = new ConsumerConfig
                {
                    Name = "ephemeral_" + Guid.NewGuid().ToString("N"),
                    FilterSubject = subject.Value,
                    DeliverPolicy = ConsumerConfigDeliverPolicy.New,
                    AckPolicy = ConsumerConfigAckPolicy.Explicit,
                    DeliverGroup = opts.DeliverGroup?.Value
                };
                if (opts.MaxAckPending.HasValue)
                {
                    config.MaxAckPending = opts.MaxAckPending.Value;
                }
                consumer = await _jsContext.CreateOrUpdateConsumerAsync(stream.Value, config, cancellationToken);
            }

            var effectiveParallelism = opts.MaxDegreeOfParallelism ?? 10;
            var service = new BasicNatsConsumerService<T>(
                consumer,
                stream.Value,
                consumerName ?? consumer.Info?.Name ?? "unknown_consumer",
                handler,
                _logger,
                effectiveParallelism,
                deserializer);

            await StartBackgroundConsumerAsync(service, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to start consumer for stream {Stream} subject {Subject} queue {QueueGroup}",
                stream.Value,
                subject.Value,
                consumerName ?? "(ephemeral)");
            throw;
        }
    }

    private async Task ConsumePullAsyncInternal<T>(
        StreamName stream,
        ConsumerName consumer,
        Func<IJsMessageContext<T>, Task> handler,
        JetStreamConsumeOptions? options,
        CancellationToken cancellationToken,
        INatsDeserialize<T>? deserializer = null)
    {
        var opts = options ?? JetStreamConsumeOptions.Default;
        ValidateConsumeOptions(opts);
        try
        {
            var jsConsumer = await _jsContext.GetConsumerAsync(stream.Value, consumer.Value, cancellationToken);
            var effectiveParallelism = opts.MaxDegreeOfParallelism ?? 10;
            var service = new BasicNatsConsumerService<T>(
                jsConsumer,
                stream.Value,
                consumer.Value,
                handler,
                _logger,
                effectiveParallelism,
                deserializer);

            await StartBackgroundConsumerAsync(service, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to start pull consumer for stream {Stream} consumer {Consumer}",
                stream.Value,
                consumer.Value);
            throw;
        }
    }

    private Task StartBackgroundConsumerAsync<T>(BasicNatsConsumerService<T> service, CancellationToken cancellationToken)
        => _backgroundTaskManager.StartAsync(service, cancellationToken);

    private static void ValidateConsumeOptions(JetStreamConsumeOptions options)
    {
        if (options.MaxDegreeOfParallelism is <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "MaxDegreeOfParallelism must be greater than zero when specified.");
        }

        if (options.MaxConcurrency is <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "MaxConcurrency must be greater than zero when specified.");
        }

        if (options.BatchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "BatchSize must be greater than zero.");
        }

        if (options.MaxAckPending is <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "MaxAckPending must be greater than zero when specified.");
        }

        if (options.DurableName != null && options.LegacyDurableName != null && options.DurableName.Value != options.LegacyDurableName)
        {
            throw new ArgumentException("QueueGroup can only be used as a backward-compatible alias for DurableName when both values match.", nameof(options));
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1) return;

        await _backgroundTaskManager.DisposeAsync();
    }
}

internal class BasicNatsConsumerService<T> : BackgroundService
{
    private readonly INatsJSConsumer _consumer;
    private readonly string _stream;
    private readonly string _consumerName;
    private readonly Func<IJsMessageContext<T>, Task> _handler;
    private readonly ILogger _logger;
    private readonly int _maxMsgs;
    private readonly INatsDeserialize<T>? _deserializer;

    public BasicNatsConsumerService(
        INatsJSConsumer consumer,
        string stream,
        string consumerName,
        Func<IJsMessageContext<T>, Task> handler,
        ILogger logger,
        int maxMsgs,
        INatsDeserialize<T>? deserializer = null)
    {
        _consumer = consumer;
        _stream = stream;
        _consumerName = consumerName;
        _handler = handler;
        _logger = logger;
        _maxMsgs = maxMsgs;
        _deserializer = deserializer;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var opts = new NatsJSConsumeOpts
                {
                    MaxMsgs = _maxMsgs,
                    Expires = TimeSpan.FromSeconds(30)
                };

                var messages = _deserializer == null
                    ? _consumer.ConsumeAsync<T>(opts: opts, cancellationToken: stoppingToken)
                    : _consumer.ConsumeAsync(_deserializer, opts: opts, cancellationToken: stoppingToken);

                await foreach (var msg in messages)
                {
                    var handlerStopwatch = Stopwatch.StartNew();
                    var tags = new TagList 
                    { 
                        { NatsTelemetry.MessagingDestinationName, _stream },
                        { NatsTelemetry.NatsStream, _stream },
                        { NatsTelemetry.NatsConsumer, _consumerName }
                    };
                    
                    try
                    {
                        var parentContext = NatsTelemetry.ExtractTraceContext(msg.Headers);
                        using var activity = NatsTelemetry.ActivitySource.StartActivity($"{_stream} receive", ActivityKind.Consumer, parentContext);

                        if (activity != null)
                        {
                            activity.SetTag(NatsTelemetry.MessagingSystem, NatsTelemetry.MessagingSystemName);
                            activity.SetTag(NatsTelemetry.MessagingDestinationName, _stream);
                            activity.SetTag(NatsTelemetry.MessagingOperation, "receive");
                            activity.SetTag(NatsTelemetry.NatsStream, _stream);
                            activity.SetTag(NatsTelemetry.NatsConsumer, _consumerName);
                            activity.SetTag(NatsTelemetry.NatsSubject, msg.Subject);
                        }

                        var context = new JsMessageContext<T>(msg);
                        await _handler(context);
                        
                        // Record success metrics
                        NatsTelemetry.MessagesReceived.Add(1, tags);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error processing message from {Stream}/{Consumer}", _stream, _consumerName);
                        
                        // Record failure metrics
                        var errorTags = new TagList 
                        { 
                            { NatsTelemetry.MessagingDestinationName, _stream },
                            { NatsTelemetry.NatsStream, _stream },
                            { NatsTelemetry.NatsConsumer, _consumerName },
                            { "error.type", ex.GetType().Name }
                        };
                        NatsTelemetry.MessagesFailed.Add(1, errorTags);
                    }
                    finally
                    {
                        handlerStopwatch.Stop();
                        NatsTelemetry.MessageProcessingDuration.Record(handlerStopwatch.Elapsed.TotalMilliseconds, tags);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Consumer loop error for {Stream}/{Consumer}. Retrying in 1s...", _stream, _consumerName);
                try
                {
                    await Task.Delay(1000, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
    }
}

internal sealed class RawByteArraySerializer : INatsSerialize<byte[]>, INatsDeserialize<byte[]>
{
    public static RawByteArraySerializer Instance { get; } = new();

    public void Serialize(IBufferWriter<byte> bufferWriter, byte[] value)
    {
        bufferWriter.Write(value);
    }

    public byte[]? Deserialize(in ReadOnlySequence<byte> buffer) => buffer.ToArray();
}

internal class JsMessageContext<T> : IJsMessageContext<T>
{
    private readonly INatsJSMsg<T> _msg;

    public JsMessageContext(INatsJSMsg<T> msg)
    {
        _msg = msg;
    }

    public T Message => _msg.Data ?? throw new NullMessagePayloadException(_msg.Subject);
    public string Subject => _msg.Subject;
    public MessageHeaders Headers => _msg.Headers != null
        ? new MessageHeaders(_msg.Headers.ToDictionary(k => k.Key, k => k.Value.ToString()))
        : MessageHeaders.Empty;

    public string? ReplyTo => _msg.ReplyTo;

    public async Task AckAsync(CancellationToken cancellationToken = default) => await _msg.AckAsync(cancellationToken: cancellationToken);

    public async Task NackAsync(TimeSpan? delay = null, CancellationToken cancellationToken = default) => await _msg.NakAsync(delay: delay ?? TimeSpan.FromSeconds(5), cancellationToken: cancellationToken);

    public async Task TermAsync(CancellationToken cancellationToken = default) => await _msg.AckTerminateAsync(cancellationToken: cancellationToken);

    public async Task InProgressAsync(CancellationToken cancellationToken = default) => await _msg.AckProgressAsync(cancellationToken: cancellationToken);

    public ulong Sequence => _msg.Metadata?.Sequence.Stream ?? 0;
    public DateTimeOffset Timestamp => _msg.Metadata?.Timestamp ?? DateTimeOffset.UtcNow;
    public bool Redelivered => (_msg.Metadata?.NumDelivered ?? 1) > 1;
    public uint NumDelivered => (uint)(_msg.Metadata?.NumDelivered ?? 1);

    public async Task RespondAsync<TResponse>(TResponse response, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(ReplyTo))
        {
            throw new InvalidOperationException("Message does not have a ReplyTo subject.");
        }
        // Assuming _msg is castable or we have access to ReplyAsync via extension or interface
        // Standard NATS.Net v2 INatsJSMsg<T> inherits from INatsMsg<T> which has ReplyAsync
        if (_msg is INatsMsg<T> natsMsg)
        {
             await natsMsg.ReplyAsync(response, cancellationToken: cancellationToken);
        }
        else
        {
             throw new NotSupportedException("This message type does not support replies.");
        }
    }
}

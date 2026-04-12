using System.Collections.Concurrent;
using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Abstractions.Exceptions;
using FlySwattr.NATS.Core.Serializers;
using FlySwattr.NATS.Core.Telemetry;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Core;

internal interface IRawJetStreamPublisher
{
    Task PublishRawAsync(string subject, ReadOnlyMemory<byte> payload, string? messageId, MessageHeaders? headers = null, CancellationToken cancellationToken = default);
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

public class NatsJetStreamBus : IJetStreamPublisher, IJetStreamConsumer, IRawJetStreamPublisher, IRawJetStreamConsumer, IAsyncDisposable
{
    private readonly INatsJSContext _jsContext;
    private readonly ILogger<NatsJetStreamBus> _logger;
    private readonly IMessageSerializer _serializer;

    private readonly ConcurrentDictionary<Guid, Task> _backgroundTasks = new();
    private readonly ConcurrentDictionary<Guid, IDisposable> _backgroundServices = new();
    private readonly ConcurrentDictionary<Guid, CancellationTokenSource> _linkedTokenSources = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly Timer _cleanupTimer;

    public NatsJetStreamBus(
        INatsJSContext jsContext,
        ILogger<NatsJetStreamBus> logger,
        IMessageSerializer serializer)
    {
        _jsContext = jsContext;
        _logger = logger;
        _serializer = serializer;
        _cleanupTimer = new Timer(CleanupCompletedTasks, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
    }

    private void CleanupCompletedTasks(object? state)
    {
        foreach (var kvp in _backgroundTasks)
        {
            if (kvp.Value.IsCompleted)
            {
                _backgroundTasks.TryRemove(kvp.Key, out _);
            }
        }
    }

    public Task PublishAsync<T>(string subject, T message, CancellationToken cancellationToken = default)
    {
        throw new ArgumentException(
            "A messageId must be provided for JetStream publishing to ensure application-level idempotency. " +
            "Use a business-key-derived ID (e.g., 'Order123-Created') to enable proper de-duplication across retries.",
            nameof(message));
    }

    public async Task PublishAsync<T>(string subject, T message, string? messageId, MessageHeaders? headers = null, CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();
        
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
        if (activity != null)
        {
            activity.SetTag(NatsTelemetry.MessagingSystem, NatsTelemetry.MessagingSystemName);
            activity.SetTag(NatsTelemetry.MessagingDestinationName, subject);
            activity.SetTag(NatsTelemetry.MessagingOperation, "publish");
            activity.SetTag(NatsTelemetry.MessagingMessageId, messageId);
        }

        var natsHeaders = new NatsHeaders
        {
            ["Content-Type"] = _serializer.GetContentType<T>(),
            ["Nats-Msg-Id"] = messageId
        };

        // Merge custom headers if provided
        if (headers != null)
        {
            foreach (var header in headers.Headers)
            {
                natsHeaders[header.Key] = header.Value;
            }
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
        var tags = new TagList { { NatsTelemetry.MessagingDestinationName, subject } };
        NatsTelemetry.MessagesPublished.Add(1, tags);
        NatsTelemetry.PublishDuration.Record(stopwatch.Elapsed.TotalMilliseconds, tags);

        _logger.LogDebug("Published JetStream message to {Subject} with MsgId {MsgId}", subject, messageId);
    }

    public async Task PublishRawAsync(string subject, ReadOnlyMemory<byte> payload, string? messageId, MessageHeaders? headers = null, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(messageId))
        {
            throw new ArgumentException(
                "A messageId must be provided for JetStream publishing to ensure application-level idempotency. " +
                "Use a business-key-derived ID (e.g., 'Order123-Created') to enable proper de-duplication across retries.",
                nameof(messageId));
        }

        var natsHeaders = new NatsHeaders
        {
            ["Nats-Msg-Id"] = messageId
        };

        if (headers != null)
        {
            foreach (var header in headers.Headers)
            {
                natsHeaders[header.Key] = header.Value;
            }
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
        string? consumerName = opts.QueueGroup?.Value;

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
                    AckPolicy = ConsumerConfigAckPolicy.Explicit
                };
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

    private async Task StartBackgroundConsumerAsync<T>(BasicNatsConsumerService<T> service, CancellationToken cancellationToken)
    {
        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cts.Token);
        var taskId = Guid.NewGuid();

        _linkedTokenSources.TryAdd(taskId, linkedCts);

        await service.StartAsync(linkedCts.Token);
        _backgroundServices.TryAdd(taskId, service);

        if (service.ExecuteTask is Task task && !task.IsCompleted)
        {
            var wrappedTask = WrapTaskWithCleanupAsync(task, taskId, linkedCts);
            _backgroundTasks.TryAdd(taskId, wrappedTask);
        }
        else
        {
            _linkedTokenSources.TryRemove(taskId, out _);
            _backgroundServices.TryRemove(taskId, out _);
            await StopAndDisposeServiceAsync(service);
            linkedCts.Dispose();
        }
    }

    private async Task WrapTaskWithCleanupAsync(Task task, Guid taskId, CancellationTokenSource linkedCts)
    {
        try
        {
            await task;
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown - don't log as error
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Background consumer faulted");
        }
        finally
        {
            // Clean up resources
            _backgroundTasks.TryRemove(taskId, out _);
            _linkedTokenSources.TryRemove(taskId, out _);

            if (_backgroundServices.TryRemove(taskId, out var svc) && svc is IDisposable disposable)
            {
                await StopAndDisposeServiceAsync(disposable);
            }

            try
            {
                linkedCts.Dispose();
            }
            catch (ObjectDisposedException)
            {
                // Already disposed - ignore
            }
        }
    }

    private async Task StopAndDisposeServiceAsync(IDisposable svc)
    {
        if (svc is BackgroundService bgSvc)
        {
            try
            {
                await bgSvc.StopAsync(CancellationToken.None);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error stopping background service");
            }
        }

        try
        {
            svc.Dispose();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error disposing background service");
        }
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        try
        {
            await Task.WhenAll(_backgroundTasks.Values);
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown
        }
        catch (AggregateException ae) when (ae.InnerExceptions.All(e => e is OperationCanceledException))
        {
            // All inner exceptions are cancellation - expected
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error during JetStream bus shutdown");
        }

        var stopTasks = new List<Task>();
        foreach (var svc in _backgroundServices.Values)
        {
            stopTasks.Add(StopAndDisposeServiceAsync(svc));
        }

        await Task.WhenAll(stopTasks);
        _backgroundServices.Clear();

        // Dispose any remaining linked token sources
        foreach (var cts in _linkedTokenSources.Values)
        {
            try
            {
                cts.Dispose();
            }
            catch (ObjectDisposedException)
            {
                // Already disposed - ignore
            }
        }
        _linkedTokenSources.Clear();

        await _cleanupTimer.DisposeAsync();
        _cts.Dispose();
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

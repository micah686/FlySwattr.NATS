using System.Buffers;
using System.Collections.Concurrent;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Abstractions.Exceptions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Core;

public class NatsJetStreamBus : IJetStreamPublisher, IJetStreamConsumer, IAsyncDisposable
{
    private readonly INatsJSContext _jsContext;
    private readonly ILogger<NatsJetStreamBus> _logger;
    private readonly IMessageSerializer _serializer;

    private readonly ConcurrentDictionary<Guid, Task> _backgroundTasks = new();
    private readonly ConcurrentDictionary<Guid, IDisposable> _backgroundServices = new();
    private readonly CancellationTokenSource _cts = new();

    public NatsJetStreamBus(
        INatsJSContext jsContext,
        ILogger<NatsJetStreamBus> logger,
        IMessageSerializer serializer)
    {
        _jsContext = jsContext;
        _logger = logger;
        _serializer = serializer;
    }

    public Task PublishAsync<T>(string subject, T message, CancellationToken cancellationToken = default)
    {
        throw new ArgumentException(
            "A messageId must be provided for JetStream publishing to ensure application-level idempotency. " +
            "Use a business-key-derived ID (e.g., 'Order123-Created') to enable proper de-duplication across retries.",
            nameof(message));
    }

    public async Task PublishAsync<T>(string subject, T message, string? messageId, CancellationToken cancellationToken = default)
    {
        var headers = new NatsHeaders();
        headers.Add("Content-Type", _serializer.GetContentType<T>());

        var bufferWriter = new ArrayBufferWriter<byte>();
        _serializer.Serialize(bufferWriter, message);
        var payload = bufferWriter.WrittenMemory;

        // Require a caller-provided message ID for true application-level idempotency.
        // Auto-generating GUIDs would defeat JetStream's deduplication on retries.
        if (string.IsNullOrWhiteSpace(messageId))
        {
            throw new ArgumentException(
                "A messageId must be provided for JetStream publishing to ensure application-level idempotency. " +
                "Use a business-key-derived ID (e.g., 'Order123-Created') to enable proper de-duplication across retries.",
                nameof(messageId));
        }
        
        headers["Nats-Msg-Id"] = messageId;

        var ack = await _jsContext.PublishAsync(
            subject,
            payload,
            headers: headers,
            opts: new NatsJSPubOpts { MsgId = messageId }, 
            cancellationToken: cancellationToken);

        ack.EnsureSuccess();

        _logger.LogDebug("Published JetStream message to {Subject} with MsgId {MsgId}", subject, messageId);
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
        // Note: maxConcurrency is enforced by the Resilience decorator, not here in Core
        string? consumerName = queueGroup?.Value;

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
            
            var effectiveParallelism = maxDegreeOfParallelism ?? 10;
            
            var service = new BasicNatsConsumerService<T>(
                consumer,
                stream.Value,
                consumerName ?? consumer.Info.Name,
                handler,
                _logger,
                effectiveParallelism
            );

            var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cts.Token);
            await service.StartAsync(linkedCts.Token);

            var taskId = Guid.NewGuid();
            _backgroundServices.TryAdd(taskId, service);

            if (service.ExecuteTask is Task task && !task.IsCompleted)
            {
                _backgroundTasks.TryAdd(taskId, task);

                _ = task.ContinueWith(async t =>
                {
                    try
                    {
                        _backgroundTasks.TryRemove(taskId, out _);
                        _backgroundServices.TryRemove(taskId, out var svc);
                        if (svc is IDisposable disposable)
                        {
                            await StopAndDisposeServiceAsync(disposable);
                        }

                        if (t.IsFaulted)
                            _logger.LogError(t.Exception, "Background consumer faulted");
                    }
                    finally
                    {
                        linkedCts.Dispose();
                    }
                }, CancellationToken.None, TaskContinuationOptions.None, TaskScheduler.Default).Unwrap();
            }
            else
            {
                _backgroundServices.TryRemove(taskId, out _);
                await StopAndDisposeServiceAsync(service);
                linkedCts.Dispose();
            }
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
        // Note: maxConcurrency is enforced by the Resilience decorator, not here in Core
        try
        {
            var jsConsumer = await _jsContext.GetConsumerAsync(stream.Value, consumer.Value, cancellationToken);

            var effectiveParallelism = maxDegreeOfParallelism ?? 10;
            
            var service = new BasicNatsConsumerService<T>(
                jsConsumer,
                stream.Value,
                consumer.Value,
                handler,
                _logger,
                effectiveParallelism
            );

            var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cts.Token);
            await service.StartAsync(linkedCts.Token);

            var taskId = Guid.NewGuid();
            _backgroundServices.TryAdd(taskId, service);

            if (service.ExecuteTask is Task task && !task.IsCompleted)
            {
                _backgroundTasks.TryAdd(taskId, task);

                _ = task.ContinueWith(async t =>
                {
                    try
                    {
                        _backgroundTasks.TryRemove(taskId, out _);
                        _backgroundServices.TryRemove(taskId, out var svc);
                        if (svc is IDisposable disposable)
                        {
                            await StopAndDisposeServiceAsync(disposable);
                        }

                        if (t.IsFaulted)
                            _logger.LogError(t.Exception, "Background pull consumer faulted");
                    }
                    finally
                    {
                        linkedCts.Dispose();
                    }
                }, CancellationToken.None, TaskContinuationOptions.None, TaskScheduler.Default).Unwrap();
            }
            else
            {
                _backgroundServices.TryRemove(taskId, out _);
                await StopAndDisposeServiceAsync(service);
                linkedCts.Dispose();
            }
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

    public BasicNatsConsumerService(
        INatsJSConsumer consumer,
        string stream,
        string consumerName,
        Func<IJsMessageContext<T>, Task> handler,
        ILogger logger,
        int maxMsgs)
    {
        _consumer = consumer;
        _stream = stream;
        _consumerName = consumerName;
        _handler = handler;
        _logger = logger;
        _maxMsgs = maxMsgs;
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

                await foreach (var msg in _consumer.ConsumeAsync<T>(opts: opts, cancellationToken: stoppingToken))
                {
                    try
                    {
                        var context = new JsMessageContext<T>(msg);
                        await _handler(context);
                        // Auto-ack if not acked by handler? 
                        // The pattern usually relies on the handler to ack. 
                        // But for safety, we could check. 
                        // However, standard NATS pattern is explicit ack.
                        // We will leave it to the handler as per the original design intent.
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error processing message from {Stream}/{Consumer}", _stream, _consumerName);
                        // Optional: Nack on failure if not already handled
                        // await msg.NakAsync(cancellationToken: stoppingToken);
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
using System.Threading.Channels;
using System.Diagnostics;
using CommunityToolkit.HighPerformance;
using CommunityToolkit.HighPerformance.Buffers;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Abstractions.Exceptions;
using FlySwattr.NATS.Core.Telemetry;
using FlySwattr.NATS.Hosting.Health;
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
    private const int MaxDlqPayloadSize = 1024 * 1024; // 1MB - NATS default max payload
    private const int InitialBufferSize = 4 * 1024; // Start small to avoid over-allocation

    private readonly INatsJSConsumer _consumer;
    private readonly Func<IJsMessageContext<T>, Task> _handler;
    private readonly ILogger _logger;
    private readonly NatsJSConsumeOpts _consumeOpts;
    private readonly string _streamName;
    private readonly string _consumerName;
    private readonly int _maxParallelism;

    private readonly IPoisonMessageHandler<T> _poisonHandler;

    // Resilience pipelines (provided by Resilience package, optional)
    private readonly ResiliencePipeline? _resiliencePipeline;

    // Health metrics for zombie consumer detection
    private readonly IConsumerHealthMetrics? _healthMetrics;

    // Topology coordination signal for Safety Mode startup
    private readonly ITopologyReadySignal? _topologyReadySignal;

    // Middleware pipeline for cross-cutting concerns
    private readonly IReadOnlyList<IConsumerMiddleware<T>> _middlewares;

    public NatsConsumerBackgroundService(
        INatsJSConsumer consumer,
        string streamName,
        string consumerName,
        Func<IJsMessageContext<T>, Task> handler,
        NatsJSConsumeOpts consumeOpts,
        ILogger logger,
        IPoisonMessageHandler<T> poisonHandler,
        int? maxDegreeOfParallelism = null,
        ResiliencePipeline? resiliencePipeline = null,
        IConsumerHealthMetrics? healthMetrics = null,
        ITopologyReadySignal? topologyReadySignal = null,
        IEnumerable<IConsumerMiddleware<T>>? middlewares = null)
    {
        _consumer = consumer;
        _streamName = streamName;
        _consumerName = consumerName;
        _handler = handler;
        _consumeOpts = consumeOpts;
        _logger = logger;
        _maxParallelism = maxDegreeOfParallelism ?? consumeOpts.MaxMsgs ?? 1;
        _poisonHandler = poisonHandler;

        _resiliencePipeline = resiliencePipeline;
        _healthMetrics = healthMetrics;
        _topologyReadySignal = topologyReadySignal;
        _middlewares = middlewares?.ToList().AsReadOnly() ?? (IReadOnlyList<IConsumerMiddleware<T>>)Array.Empty<IConsumerMiddleware<T>>();
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
        // Register with health metrics for zombie detection
        _healthMetrics?.RegisterConsumer(_streamName, _consumerName);

        // Wait for topology to be ready before starting consume loop (Safety Mode)
        if (_topologyReadySignal != null)
        {
            try
            {
                LogWaitingForTopology(_streamName, _consumerName);
                await _topologyReadySignal.WaitAsync(stoppingToken);
                LogTopologyReady(_streamName, _consumerName);
            }
            catch (OperationCanceledException)
            {
                LogTopologyWaitCanceled(_streamName, _consumerName);
                return;
            }
            catch (Exception ex)
            {
                LogTopologyFailed(_streamName, _consumerName, ex);
                return;
            }
        }

        try
        {
            // Channel capacity matches worker count for strict backpressure
            var channel = Channel.CreateBounded<IJsMessageContext<T>>(new BoundedChannelOptions(_maxParallelism)
            {
                SingleWriter = true,
                SingleReader = false,
                FullMode = BoundedChannelFullMode.Wait,
                Capacity = _maxParallelism
            });

            var parallelTasks = new Task[_maxParallelism];
            for (int i = 0; i < _maxParallelism; i++)
            {
                parallelTasks[i] = RunWorkerAsync(channel.Reader, stoppingToken);
            }

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await foreach (var msg in _consumer.ConsumeAsync<T>(opts: _consumeOpts, cancellationToken: stoppingToken))
                    {
                        // Record loop iteration for health check (consumer is actively receiving)
                        _healthMetrics?.RecordLoopIteration(_streamName, _consumerName);

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
                await Task.WhenAll(parallelTasks).WaitAsync(shutdownCts.Token);
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
        finally
        {
            // Unregister from health metrics
            _healthMetrics?.UnregisterConsumer(_streamName, _consumerName);
        }
    }

    private async Task RunWorkerAsync(ChannelReader<IJsMessageContext<T>> reader, CancellationToken token)
    {
        try
        {
            await foreach (var context in reader.ReadAllAsync(token))
            {
                var parentContext = NatsTelemetry.ExtractTraceContext(context.Headers);
                using var activity = NatsTelemetry.ActivitySource.StartActivity($"{_streamName} process", ActivityKind.Consumer, parentContext);

                if (activity != null)
                {
                    activity.SetTag(NatsTelemetry.MessagingSystem, NatsTelemetry.MessagingSystemName);
                    activity.SetTag(NatsTelemetry.MessagingDestinationName, _streamName);
                    activity.SetTag(NatsTelemetry.MessagingOperation, "process");
                    activity.SetTag(NatsTelemetry.NatsStream, _streamName);
                    activity.SetTag(NatsTelemetry.NatsConsumer, _consumerName);
                    activity.SetTag(NatsTelemetry.NatsSubject, context.Subject);
                    if (context.ReplyTo != null)
                    {
                        activity.SetTag(NatsTelemetry.MessagingConversationId, context.ReplyTo);
                    }
                }

                try
                {
                    await ExecuteHandlerWithResilienceAsync(context, token);
                }
                catch (NullMessagePayloadException ex)
                {
                    activity?.SetStatus(ActivityStatusCode.Error, "Null payload");
                    LogNullPayloadTermination(_streamName, _consumerName, ex);
                    try { await context.TermAsync(token); } catch { /* best effort */ }
                    continue;
                }
                catch (InvalidOperationException ex) when (ex.Message.Contains("null"))
                {
                    activity?.SetStatus(ActivityStatusCode.Error, "Null payload");
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
                    activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                    try
                    {
                        var configuredLimit = _consumer.Info.Config.MaxDeliver;
                        var limitLong = configuredLimit > 0 ? configuredLimit : long.MaxValue;
                        await _poisonHandler.HandleAsync(context, _streamName, _consumerName, limitLong, ex, token);
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

                    // Record heartbeat after successful message processing
                    _healthMetrics?.RecordHeartbeat(_streamName, _consumerName);
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
        // Build the "Russian Doll" middleware pipeline
        Func<Task> pipeline = () => _handler(context);

        // Wrap in middleware (reversed so first registered executes first)
        foreach (var middleware in _middlewares.Reverse())
        {
            var next = pipeline;
            var mw = middleware; // Capture for closure
            pipeline = () => mw.InvokeAsync(context, next, token);
        }

        // Execute through resilience pipeline if configured
        if (_resiliencePipeline != null)
        {
            await _resiliencePipeline.ExecuteAsync(async _ => await pipeline(), token);
        }
        else
        {
            await pipeline();
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

    [LoggerMessage(Level = LogLevel.Error, Message = "Pull consumer loop failed for {StreamName}/{ConsumerName}")]
    private partial void LogPullConsumerLoopFailed(string streamName, string consumerName, Exception exception);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Workers did not complete within shutdown timeout for {StreamName}/{ConsumerName}")]
    private partial void LogShutdownTimeout(string streamName, string consumerName);

    [LoggerMessage(Level = LogLevel.Information, Message = "Consumer {StreamName}/{ConsumerName} waiting for topology ready signal...")]
    private partial void LogWaitingForTopology(string streamName, string consumerName);

    [LoggerMessage(Level = LogLevel.Information, Message = "Consumer {StreamName}/{ConsumerName} received topology ready signal, starting consume loop.")]
    private partial void LogTopologyReady(string streamName, string consumerName);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Consumer {StreamName}/{ConsumerName} topology wait was canceled during shutdown.")]
    private partial void LogTopologyWaitCanceled(string streamName, string consumerName);

    [LoggerMessage(Level = LogLevel.Error, Message = "Consumer {StreamName}/{ConsumerName} cannot start: topology provisioning failed.")]
    private partial void LogTopologyFailed(string streamName, string consumerName, Exception exception);
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
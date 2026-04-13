using System.Threading.Channels;
using System.Diagnostics;
using System.Buffers;
using CommunityToolkit.HighPerformance;
using CommunityToolkit.HighPerformance.Buffers;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Configuration;
using FlySwattr.NATS.Abstractions.Exceptions;
using FlySwattr.NATS.Core.Services;
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
    private static readonly TimeSpan DefaultAckTimeout = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan ShutdownNackDelay = TimeSpan.FromSeconds(1);

    private readonly INatsJSConsumer _consumer;
    private readonly Func<IJsMessageContext<T>, Task> _handler;
    private readonly ILogger _logger;
    private readonly NatsJSConsumeOpts _consumeOpts;
    private readonly string _streamName;
    private readonly string _consumerName;
    private readonly int _maxParallelism;

    private readonly IPoisonMessageHandler<T> _poisonHandler;
    private readonly IMessageSerializer? _serializer;
    private readonly IObjectStore? _objectStore;
    private readonly PayloadOffloadingOptions? _offloadingOptions;

    // Resilience pipelines (provided by Resilience package, optional)
    private readonly ResiliencePipeline? _resiliencePipeline;

    // Health metrics for zombie consumer detection
    private readonly IConsumerHealthMetrics? _healthMetrics;

    // Topology coordination signal for Safety Mode startup
    private readonly ITopologyReadySignal? _topologyReadySignal;

    // Middleware pipeline for cross-cutting concerns
    private readonly IReadOnlyList<IConsumerMiddleware<T>> _middlewares;
    private readonly TimeSpan _ackTimeout;

    internal NatsConsumerBackgroundService(
        INatsJSConsumer consumer,
        string streamName,
        string consumerName,
        Func<IJsMessageContext<T>, Task> handler,
        NatsJSConsumeOpts consumeOpts,
        ILogger logger,
        IPoisonMessageHandler<T> poisonHandler,
        int? maxDegreeOfParallelism = null,
        TimeSpan? ackTimeout = null,
        ResiliencePipeline? resiliencePipeline = null,
        IConsumerHealthMetrics? healthMetrics = null,
        ITopologyReadySignal? topologyReadySignal = null,
        IEnumerable<IConsumerMiddleware<T>>? middlewares = null)
        : this(
            consumer,
            streamName,
            consumerName,
            handler,
            consumeOpts,
            logger,
            poisonHandler,
            serializer: null,
            objectStore: null,
            offloadingOptions: null,
            maxDegreeOfParallelism: maxDegreeOfParallelism,
            ackTimeout: ackTimeout,
            resiliencePipeline: resiliencePipeline,
            healthMetrics: healthMetrics,
            topologyReadySignal: topologyReadySignal,
            middlewares: middlewares)
    {
    }

    internal NatsConsumerBackgroundService(
        INatsJSConsumer consumer,
        string streamName,
        string consumerName,
        Func<IJsMessageContext<T>, Task> handler,
        NatsJSConsumeOpts consumeOpts,
        ILogger logger,
        IPoisonMessageHandler<T> poisonHandler,
        IMessageSerializer? serializer,
        IObjectStore? objectStore,
        PayloadOffloadingOptions? offloadingOptions,
        int? maxDegreeOfParallelism = null,
        TimeSpan? ackTimeout = null,
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
        _serializer = serializer;
        _objectStore = objectStore;
        _offloadingOptions = offloadingOptions;

        _resiliencePipeline = resiliencePipeline;
        _healthMetrics = healthMetrics;
        _topologyReadySignal = topologyReadySignal;
        _middlewares = middlewares?.ToList().AsReadOnly() ?? (IReadOnlyList<IConsumerMiddleware<T>>)Array.Empty<IConsumerMiddleware<T>>();
        _ackTimeout = ackTimeout.HasValue && ackTimeout.Value > TimeSpan.Zero
            ? ackTimeout.Value
            : DefaultAckTimeout;
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
                    if (UseRawConsumption())
                    {
                        await foreach (var context in ReadContextsAsync(
                                           _consumer.ConsumeAsync(PassthroughByteArrayDeserializer.Instance, opts: _consumeOpts, cancellationToken: stoppingToken),
                                           stoppingToken))
                        {
                            _healthMetrics?.RecordLoopIteration(_streamName, _consumerName);
                            if (!channel.Writer.TryWrite(context))
                            {
                                LogBackpressure(_streamName, _consumerName);
                                await context.NackAsync(TimeSpan.FromSeconds(1), stoppingToken);
                            }
                        }
                    }
                    else
                    {
                        await foreach (var context in ReadContextsAsync(
                                           _consumer.ConsumeAsync<T>(opts: _consumeOpts, cancellationToken: stoppingToken),
                                           stoppingToken))
                        {
                            _healthMetrics?.RecordLoopIteration(_streamName, _consumerName);
                            if (!channel.Writer.TryWrite(context))
                            {
                                LogBackpressure(_streamName, _consumerName);
                                await context.NackAsync(TimeSpan.FromSeconds(1), stoppingToken);
                            }
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

            channel.Writer.TryComplete();

            if (stoppingToken.IsCancellationRequested)
            {
                await DrainPendingMessagesAsync(channel.Reader);
            }

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

    private bool UseRawConsumption()
        => _serializer != null && _objectStore != null && _offloadingOptions != null;

    private async IAsyncEnumerable<IJsMessageContext<T>> ReadContextsAsync(
        IAsyncEnumerable<INatsJSMsg<T>> messages,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var msg in messages.WithCancellation(cancellationToken))
        {
            yield return CreateContext(msg);
        }
    }

    private async IAsyncEnumerable<IJsMessageContext<T>> ReadContextsAsync(
        IAsyncEnumerable<INatsJSMsg<byte[]>> messages,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var msg in messages.WithCancellation(cancellationToken))
        {
            var rawContext = new JsMessageContextWrapper<byte[]>(msg);
            yield return await ResolveRawContextAsync(rawContext, cancellationToken);
        }
    }

    private async Task<IJsMessageContext<T>> ResolveRawContextAsync(
        IJsMessageContext<byte[]> rawContext,
        CancellationToken cancellationToken)
    {
        if (rawContext.Headers.Headers.TryGetValue(_offloadingOptions!.ClaimCheckHeaderName, out var claimCheckRef))
        {
            var payload = await DownloadPayloadAsync(claimCheckRef, cancellationToken);
            return new HydratedMessageContext<T>(rawContext, _serializer!.Deserialize<T>(payload));
        }

        return new HydratedMessageContext<T>(rawContext, _serializer!.Deserialize<T>(rawContext.Message));
    }

    private async Task<ReadOnlyMemory<byte>> DownloadPayloadAsync(string claimCheckRef, CancellationToken cancellationToken)
    {
        var objectKey = claimCheckRef.StartsWith("objstore://", StringComparison.OrdinalIgnoreCase)
            ? claimCheckRef["objstore://".Length..]
            : claimCheckRef;
        objectKey = MessageSecurity.ValidateObjectStoreKey(objectKey, nameof(claimCheckRef));

        using var memoryStream = new MemoryStream();
        await _objectStore!.GetAsync(objectKey, memoryStream, cancellationToken);
        return memoryStream.ToArray();
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
                    ackCts.CancelAfter(_ackTimeout);
                    await context.AckAsync(ackCts.Token);

                    // Record heartbeat after successful message processing
                    _healthMetrics?.RecordHeartbeat(_streamName, _consumerName);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    // Consumer is shutting down - ack abandoned intentionally
                    LogAckAbandonedDuringShutdown(_streamName, _consumerName);
                }
                catch (OperationCanceledException)
                {
                    // Ack timeout exceeded - not a shutdown
                    LogAckTimeout(_streamName, _consumerName, _ackTimeout.TotalMilliseconds);
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

    private async Task DrainPendingMessagesAsync(ChannelReader<IJsMessageContext<T>> reader)
    {
        var drainedCount = 0;

        while (reader.TryRead(out var pendingContext))
        {
            try
            {
                await pendingContext.NackAsync(ShutdownNackDelay, CancellationToken.None);
                drainedCount++;
            }
            catch (Exception ex)
            {
                LogShutdownNackFailed(_streamName, _consumerName, pendingContext.Subject, ex);
            }
        }

        if (drainedCount > 0)
        {
            LogShutdownDrain(_streamName, _consumerName, drainedCount);
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

    [LoggerMessage(Level = LogLevel.Warning, Message = "Ack timed out after {AckTimeoutMs}ms for {StreamName}/{ConsumerName}")]
    private partial void LogAckTimeout(string streamName, string consumerName, double ackTimeoutMs);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Ack failed for processed message {StreamName}/{ConsumerName}: {ErrorMessage}")]
    private partial void LogAckFailed(string streamName, string consumerName, string errorMessage);

    [LoggerMessage(Level = LogLevel.Error, Message = "Worker crashed for {StreamName}/{ConsumerName}")]
    private partial void LogWorkerCrashed(string streamName, string consumerName, Exception exception);

    [LoggerMessage(Level = LogLevel.Error, Message = "Pull consumer loop failed for {StreamName}/{ConsumerName}")]
    private partial void LogPullConsumerLoopFailed(string streamName, string consumerName, Exception exception);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Workers did not complete within shutdown timeout for {StreamName}/{ConsumerName}")]
    private partial void LogShutdownTimeout(string streamName, string consumerName);

    [LoggerMessage(Level = LogLevel.Information, Message = "Drained and NAKed {Count} queued message(s) during shutdown for {StreamName}/{ConsumerName}")]
    private partial void LogShutdownDrain(string streamName, string consumerName, int count);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to NAK queued shutdown message {Subject} for {StreamName}/{ConsumerName}")]
    private partial void LogShutdownNackFailed(string streamName, string consumerName, string subject, Exception exception);

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

internal class HydratedMessageContext<T> : IJsMessageContext<T>
{
    private readonly IJsMessageContext<byte[]> _inner;
    private readonly T _message;

    public HydratedMessageContext(IJsMessageContext<byte[]> inner, T message)
    {
        _inner = inner;
        _message = message;
    }

    public T Message => _message;
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

internal sealed class PassthroughByteArrayDeserializer : INatsDeserialize<byte[]>
{
    public static PassthroughByteArrayDeserializer Instance { get; } = new();
    public byte[]? Deserialize(in ReadOnlySequence<byte> buffer) => buffer.ToArray();
}

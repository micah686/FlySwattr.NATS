using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Services;
using FlySwattr.NATS.Core.Serializers;
using FlySwattr.NATS.Core.Telemetry;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;

namespace FlySwattr.NATS.Core;

public class NatsMessageBus : IMessageBus, IAsyncDisposable
{
    private static readonly string[] ReservedHeaders =
    [
        "traceparent",
        "tracestate"
    ];

    private readonly INatsConnection _connection;
    private readonly ILogger<NatsMessageBus> _logger;
    private readonly Func<double> _randomNextDouble;
    private readonly ConcurrentDictionary<Guid, Task> _backgroundTasks = new();
    private readonly ConcurrentDictionary<Guid, IAsyncDisposable> _subscriptions = new();
    private readonly ConcurrentDictionary<Guid, CancellationTokenSource> _subscriptionCts = new();
    private readonly CancellationTokenSource _cts = new();
    private int _disposed;

    public event EventHandler<NatsConnectionState>? ConnectionStateChanged;

    private static readonly TimeSpan DefaultRequestTimeout = TimeSpan.FromSeconds(30);

    public NatsMessageBus(INatsConnection connection, ILogger<NatsMessageBus> logger)
        : this(connection, logger, () => Random.Shared.NextDouble())
    {
    }

    internal NatsMessageBus(INatsConnection connection, ILogger<NatsMessageBus> logger, Func<double> randomNextDouble)
    {
        _connection = connection;
        _logger = logger;
        _randomNextDouble = randomNextDouble;

        // Use events for connection state monitoring to avoid polling overhead.
        _connection.ConnectionOpened += OnConnectionEvent;
        _connection.ConnectionDisconnected += OnConnectionEvent;
        _connection.ReconnectFailed += OnConnectionEvent;

    }

    private async ValueTask OnConnectionEvent(object? sender, NatsEventArgs e)
    {
        try
        {
            // Capture state synchronously to avoid race
            var state = _connection.ConnectionState;
            
            // Invoke event handlers asynchronously
            if (ConnectionStateChanged != null)
            {
                foreach (var handler in ConnectionStateChanged.GetInvocationList())
                {
                    try
                    {
                        await Task.Run(() => handler.DynamicInvoke(this, state))
                            .ConfigureAwait(false);
                    }
                    catch (Exception handlerEx)
                    {
                        _logger.LogError(handlerEx, "Connection state change handler failed");
                    }
                }
            }
            
            _logger.LogInformation("NATS Connection Event: {Event}, State: {State}", e.GetType().Name, state);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error handling connection event");
        }
    }

    public async Task PublishAsync<T>(string subject, T message, CancellationToken cancellationToken = default)
    {
        MessageSecurity.ValidatePublishSubject(subject);
        var stopwatch = Stopwatch.StartNew();
        using var activity = NatsTelemetry.ActivitySource.StartActivity($"{subject} publish", ActivityKind.Producer);
        if (activity != null)
        {
            activity.SetTag(NatsTelemetry.MessagingSystem, NatsTelemetry.MessagingSystemName);
            activity.SetTag(NatsTelemetry.MessagingDestinationName, subject);
            activity.SetTag(NatsTelemetry.MessagingOperation, "publish");
        }

        var natsHeaders = new NatsHeaders();
        NatsTelemetry.InjectTraceContext(activity, natsHeaders);
        MemoryPackSchemaMetadata.AddHeadersIfNeeded<T>(natsHeaders);
        
        await _connection.PublishAsync(subject, message, headers: natsHeaders, cancellationToken: cancellationToken);
        
        // Record metrics
        stopwatch.Stop();
        var tags = new TagList { { NatsTelemetry.MessagingDestinationName, subject } };
        NatsTelemetry.MessagesPublished.Add(1, tags);
        NatsTelemetry.PublishDuration.Record(stopwatch.Elapsed.TotalMilliseconds, tags);
    }

    public async Task PublishAsync<T>(string subject, T message, MessageHeaders? headers, CancellationToken cancellationToken = default)
    {
        MessageSecurity.ValidatePublishSubject(subject);
        MessageSecurity.RejectReservedHeaders(headers, ReservedHeaders);
        var stopwatch = Stopwatch.StartNew();
        using var activity = NatsTelemetry.ActivitySource.StartActivity($"{subject} publish", ActivityKind.Producer);
        if (activity != null)
        {
            activity.SetTag(NatsTelemetry.MessagingSystem, NatsTelemetry.MessagingSystemName);
            activity.SetTag(NatsTelemetry.MessagingDestinationName, subject);
            activity.SetTag(NatsTelemetry.MessagingOperation, "publish");
        }
        
        var natsHeaders = new NatsHeaders();
        if (headers?.Headers.Count > 0)
        {
            foreach (var kvp in headers.Headers)
            {
                if (string.IsNullOrWhiteSpace(kvp.Key))
                {
                    throw new ArgumentException("Header key cannot be null or whitespace", nameof(headers));
                }
                
                if (kvp.Key.Contains(':') || kvp.Key.Any(char.IsControl))
                {
                    throw new ArgumentException($"Invalid header key '{kvp.Key}'. Keys cannot contain colons or control characters.", nameof(headers));
                }
                
                if (kvp.Value == null)
                {
                    throw new ArgumentException($"Header value for '{kvp.Key}' cannot be null", nameof(headers));
                }

                natsHeaders.Add(kvp.Key, kvp.Value);
            }
        }
        
        NatsTelemetry.InjectTraceContext(activity, natsHeaders);
        MemoryPackSchemaMetadata.AddHeadersIfNeeded<T>(natsHeaders);
        
        await _connection.PublishAsync(subject, message, headers: natsHeaders, cancellationToken: cancellationToken);
        
        // Record metrics
        stopwatch.Stop();
        var tags = new TagList { { NatsTelemetry.MessagingDestinationName, subject } };
        NatsTelemetry.MessagesPublished.Add(1, tags);
        NatsTelemetry.PublishDuration.Record(stopwatch.Elapsed.TotalMilliseconds, tags);
    }

    public async Task<ISubscription> SubscribeAsync<T>(string subject, Func<IMessageContext<T>, Task> handler, string? queueGroup = null, CancellationToken cancellationToken = default)
    {
        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cts.Token);
        var token = linkedCts.Token;
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var taskId = Guid.NewGuid();
        var subscriptionHandle = new NatsSubscriptionHandle(taskId, StopSubscriptionAsync);

        _logger.LogInformation("Starting subscription for {Subject}", subject);

        // Store so StopSubscriptionAsync can cancel this specific subscription's loop
        _subscriptionCts[taskId] = linkedCts;

        var task = Task.Run(async () =>
        {
            var backoff = TimeSpan.FromSeconds(1);
            while (!token.IsCancellationRequested)
            {
                INatsSub<T>? sub = null;
                try
                {
                    _logger.LogDebug("Attempting to subscribe to {Subject}", subject);
                    sub = await _connection.SubscribeCoreAsync<T>(subject, queueGroup: queueGroup, cancellationToken: token);
                    _subscriptions[taskId] = sub;

                    _logger.LogDebug("Successfully subscribed to {Subject}", subject);
                    tcs.TrySetResult(true);

                    backoff = TimeSpan.FromSeconds(1);

                    await foreach (var msg in sub.Msgs.ReadAllAsync(token))
                    {
                        var parentContext = NatsTelemetry.ExtractTraceContext(msg.Headers);
                        using var activity = NatsTelemetry.ActivitySource.StartActivity($"{subject} receive", ActivityKind.Consumer, parentContext);
                        
                        if (activity != null)
                        {
                            activity.SetTag(NatsTelemetry.MessagingSystem, NatsTelemetry.MessagingSystemName);
                            activity.SetTag(NatsTelemetry.MessagingDestinationName, subject);
                            activity.SetTag(NatsTelemetry.MessagingOperation, "receive");
                            activity.SetTag(NatsTelemetry.NatsSubject, msg.Subject);
                            if (msg.ReplyTo != null)
                            {
                                activity.SetTag(NatsTelemetry.MessagingConversationId, msg.ReplyTo);
                            }
                        }

                        var handlerStopwatch = Stopwatch.StartNew();
                        var handlerTags = new TagList { { NatsTelemetry.MessagingDestinationName, subject } };
                        try
                        {
                            _logger.LogDebug("Received message on {Subject}", msg.Subject);
                            var context = new MessageContext<T>(msg);
                            await handler(context);
                            
                            // Record success metrics
                            NatsTelemetry.MessagesReceived.Add(1, handlerTags);
                        }
                        catch (Exception ex)
                        {
                            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                            _logger.LogError(ex, "Error handling message {Subject}", subject);
                            
                            // Record failure metrics
                            var errorTags = new TagList { { NatsTelemetry.MessagingDestinationName, subject }, { "error.type", ex.GetType().Name } };
                            NatsTelemetry.MessagesFailed.Add(1, errorTags);
                        }
                        finally
                        {
                            handlerStopwatch.Stop();
                            NatsTelemetry.MessageProcessingDuration.Record(handlerStopwatch.Elapsed.TotalMilliseconds, handlerTags);
                        }
                    }
                    _logger.LogWarning("Subscription Msgs loop completed UNEXPECTEDLY for {Subject}. Token cancelled: {IsCancelled}", subject, token.IsCancellationRequested);
                    
                    if (!token.IsCancellationRequested)
                    {
                        await Task.Delay(1000, token);
                    }
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation("Subscription cancelled for {Subject}", subject);
                    break;
                }
                catch (Exception ex)
                {
                    var jitteredBackoff = ApplyJitter(backoff);
                    _logger.LogError(ex, "Subscription loop failed for {Subject}. Reconnecting in {BackoffMs}ms...", subject, jitteredBackoff.TotalMilliseconds);

                    try
                    {
                        await Task.Delay(jitteredBackoff, token);
                    }
                    catch (OperationCanceledException) { break; }

                    backoff = backoff * 2;
                    if (backoff > TimeSpan.FromSeconds(60)) backoff = TimeSpan.FromSeconds(60);
                }
                finally
                {
                    if (sub != null)
                    {
                        try
                        {
                            await sub.DisposeAsync();
                        }
                        catch (Exception ex)
                        {
                             _logger.LogWarning(ex, "Failed to dispose subscription for {Subject}", subject);
                        }
                    }
                }
            }

            _logger.LogInformation("Background task finishing for {Subject}", subject);
            _subscriptions.TryRemove(taskId, out _);
            _backgroundTasks.TryRemove(taskId, out _);
            _subscriptionCts.TryRemove(taskId, out _);
            linkedCts.Dispose();

        }, token);

        _backgroundTasks.TryAdd(taskId, task);

        try
        {
             await tcs.Task.WaitAsync(TimeSpan.FromSeconds(30), token);
             _logger.LogInformation("SubscribeAsync returning for {Subject}", subject);
        }
        catch (TimeoutException)
        {
            _logger.LogWarning("Subscription to {Subject} timed out waiting for initial connection.", subject);
            await linkedCts.CancelAsync();
            throw new TimeoutException($"Subscription to {subject} timed out waiting for initial connection.");
        }
        catch when (token.IsCancellationRequested)
        {
            _logger.LogWarning("SubscribeAsync cancelled for {Subject} during initial wait", subject);
        }

        return subscriptionHandle;
    }

    private async Task StopSubscriptionAsync(Guid subscriptionId, CancellationToken cancellationToken)
    {
        // Cancel the per-subscription CTS to stop the reconnect loop from resubscribing.
        // The background task owns disposal of the CTS; we only cancel here.
        if (_subscriptionCts.TryRemove(subscriptionId, out var subCts))
        {
            await subCts.CancelAsync();
        }

        // Dispose the underlying NATS subscription to unblock ReadAllAsync immediately.
        if (_subscriptions.TryRemove(subscriptionId, out var subscription))
        {
            await subscription.DisposeAsync();
        }

        if (_backgroundTasks.TryGetValue(subscriptionId, out var task))
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(TimeSpan.FromSeconds(30));
            try
            {
                await task.WaitAsync(timeoutCts.Token);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException)
            {
                _logger.LogWarning("Timed out waiting for subscription {SubscriptionId} to stop", subscriptionId);
            }
        }
    }

    private TimeSpan ApplyJitter(TimeSpan baseDelay)
    {
        var jitterFactor = 1.0 + (_randomNextDouble() - 0.5) * 0.5;
        var jitteredMs = Math.Max(1, baseDelay.TotalMilliseconds * jitterFactor);
        return TimeSpan.FromMilliseconds(jitteredMs);
    }

    public async Task<TResponse?> RequestAsync<TRequest, TResponse>(string subject, TRequest request, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        using var activity = NatsTelemetry.ActivitySource.StartActivity($"{subject} request", ActivityKind.Client);
        if (activity != null)
        {
            activity.SetTag(NatsTelemetry.MessagingSystem, NatsTelemetry.MessagingSystemName);
            activity.SetTag(NatsTelemetry.MessagingDestinationName, subject);
            activity.SetTag(NatsTelemetry.MessagingOperation, "request");
        }

        var natsHeaders = new NatsHeaders();
        NatsTelemetry.InjectTraceContext(activity, natsHeaders);

        var reply = await _connection.RequestAsync<TRequest, TResponse>(
            subject,
            request,
            headers: natsHeaders,
            replyOpts: new NatsSubOpts { Timeout = timeout },
            cancellationToken: cancellationToken);
        return reply.Data;
    }


    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1) return;

        // Unsubscribe events
        _connection.ConnectionOpened -= OnConnectionEvent;
        _connection.ConnectionDisconnected -= OnConnectionEvent;
        _connection.ReconnectFailed -= OnConnectionEvent;

        await _cts.CancelAsync();

        // Background tasks own their subscriptions: each task disposes its own INatsSub in its
        // finally block and then removes itself from _subscriptions.  Wait for them to finish
        // before touching the dictionary so we don't double-dispose the same sub concurrently.
        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        try
        {
            await Task.WhenAll(_backgroundTasks.Values).WaitAsync(timeoutCts.Token);
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Graceful shutdown timed out or cancelled");
        }
        catch (AggregateException ae) when (ae.InnerExceptions.All(e => e is OperationCanceledException))
        {
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error during message bus shutdown");
        }

        // Safety net: dispose any subscriptions whose background tasks did not complete in time.
        // TryRemove ensures a subscription is only disposed once even if a slow task is still
        // winding down and tries to remove the same entry after we do.
        foreach (var kvp in _subscriptions)
        {
            if (_subscriptions.TryRemove(kvp.Key, out var sub))
            {
                try { await sub.DisposeAsync(); }
                catch (OperationCanceledException) { }
                catch (Exception ex) { _logger.LogWarning(ex, "Error disposing subscription {Id}", kvp.Key); }
            }
        }

        _cts.Dispose();
    }
}

internal sealed class NatsSubscriptionHandle : ISubscription
{
    private readonly Func<Guid, CancellationToken, Task> _stopAsync;
    private int _stopped;

    public NatsSubscriptionHandle(Guid id, Func<Guid, CancellationToken, Task> stopAsync)
    {
        Id = id;
        _stopAsync = stopAsync;
    }

    public Guid Id { get; }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
        {
            return;
        }

        await _stopAsync(Id, cancellationToken);
    }

    public ValueTask DisposeAsync() => new(StopAsync());
}

internal class MessageContext<T> : IMessageContext<T>
{
    private readonly INatsMsg<T> _msg;

    public MessageContext(INatsMsg<T> msg)
    {
        _msg = msg;
    }

    public T Message => _msg.Data ?? throw new InvalidOperationException("Message data is null");
    public string Subject => _msg.Subject;
    public MessageHeaders Headers => _msg.Headers != null
        ? new MessageHeaders(_msg.Headers.ToDictionary(k => k.Key, k => k.Value.ToString()))
        : MessageHeaders.Empty;

    public string? ReplyTo => _msg.ReplyTo;

    public async Task RespondAsync<TResponse>(TResponse response, CancellationToken cancellationToken = default)
    {
         if (ReplyTo == null) return;
         await _msg.ReplyAsync(response, cancellationToken: cancellationToken);
    }
}

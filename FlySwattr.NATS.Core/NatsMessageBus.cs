using System.Collections.Concurrent;
using FlySwattr.NATS.Abstractions;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;

namespace FlySwattr.NATS.Core;

public class NatsMessageBus : IMessageBus, IAsyncDisposable
{
    private readonly INatsConnection _connection;
    private readonly ILogger<NatsMessageBus> _logger;
    private readonly ConcurrentDictionary<Guid, Task> _backgroundTasks = new();
    private readonly ConcurrentDictionary<Guid, IAsyncDisposable> _subscriptions = new();
    private readonly CancellationTokenSource _cts = new();
    private bool _disposed;

    public event EventHandler<NatsConnectionState>? ConnectionStateChanged;

    private static readonly TimeSpan DefaultRequestTimeout = TimeSpan.FromSeconds(30);

    public NatsMessageBus(INatsConnection connection, ILogger<NatsMessageBus> logger)
    {
        _connection = connection;
        _logger = logger;

        // Use events instead of polling (MED-6)
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
        await _connection.PublishAsync(subject, message, cancellationToken: cancellationToken);
    }

    public async Task SubscribeAsync<T>(string subject, Func<IMessageContext<T>, Task> handler, string? queueGroup = null, CancellationToken cancellationToken = default)
    {
        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cts.Token);
        var token = linkedCts.Token;
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var taskId = Guid.NewGuid();

        var task = Task.Run(async () =>
        {
            var backoff = TimeSpan.FromSeconds(1);
            while (!token.IsCancellationRequested)
            {
                INatsSub<T>? sub = null;
                try
                {
                    sub = await _connection.SubscribeCoreAsync<T>(subject, queueGroup: queueGroup, cancellationToken: token);
                    _subscriptions[taskId] = sub;

                    tcs.TrySetResult(true);

                    backoff = TimeSpan.FromSeconds(1);

                    await foreach (var msg in sub.Msgs.ReadAllAsync(token))
                    {
                        try
                        {
                            var context = new MessageContext<T>(msg);
                            await handler(context);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error handling message {Subject}", subject);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Subscription loop failed for {Subject}. Reconnecting in {Backoff}s...", subject, backoff.TotalSeconds);

                    try
                    {
                        await Task.Delay(backoff, token);
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

            _subscriptions.TryRemove(taskId, out _);
            _backgroundTasks.TryRemove(taskId, out _);
            linkedCts.Dispose();

        }, token);

        _backgroundTasks.TryAdd(taskId, task);

        try
        {
             await tcs.Task.WaitAsync(TimeSpan.FromSeconds(30), token);
        }
        catch (TimeoutException)
        {
            _logger.LogWarning("Subscription to {Subject} timed out waiting for initial connection.", subject);
            throw new TimeoutException($"Subscription to {subject} timed out waiting for initial connection.");
        }
        catch when (token.IsCancellationRequested)
        {
            // ignore
        }
    }

    public async Task<TResponse?> RequestAsync<TRequest, TResponse>(string subject, TRequest request, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        var reply = await _connection.RequestAsync<TRequest, TResponse>(
            subject,
            request,
            replyOpts: new NatsSubOpts { Timeout = timeout },
            cancellationToken: cancellationToken);
        return reply.Data;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        // Unsubscribe events
        _connection.ConnectionOpened -= OnConnectionEvent;
        _connection.ConnectionDisconnected -= OnConnectionEvent;
        _connection.ReconnectFailed -= OnConnectionEvent;

        _cts.Cancel();
        
        var subscriptionsSnapshot = _subscriptions.ToArray();

        foreach (var kvp in subscriptionsSnapshot)
        {
            try
            {
                _subscriptions.TryRemove(kvp.Key, out _);
                await kvp.Value.DisposeAsync();
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error disposing subscription {Id}", kvp.Key);
            }
        }
        _subscriptions.Clear();
        
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
        _cts.Dispose();
    }
}

public class MessageContext<T> : IMessageContext<T>
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
         await _msg.ReplyAsync(response, cancellationToken: cancellationToken);
    }
}

using System.Collections.Concurrent;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace FlySwattr.NATS.Core.Services;

/// <summary>
/// Owns background service lifecycle so messaging components can focus on transport concerns.
/// </summary>
internal sealed class BackgroundTaskManager : IAsyncDisposable
{
    private readonly ILogger<BackgroundTaskManager> _logger;
    private readonly ConcurrentDictionary<Guid, Task> _backgroundTasks = new();
    private readonly ConcurrentDictionary<Guid, IDisposable> _backgroundServices = new();
    private readonly ConcurrentDictionary<Guid, CancellationTokenSource> _linkedTokenSources = new();
    private readonly CancellationTokenSource _shutdownTokenSource = new();
    private readonly Timer _cleanupTimer;
    private int _disposed;

    public BackgroundTaskManager(ILogger<BackgroundTaskManager> logger)
    {
        _logger = logger;
        _cleanupTimer = new Timer(CleanupCompletedTasks, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
    }

    public async Task StartAsync(BackgroundService service, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) == 1, this);
        ArgumentNullException.ThrowIfNull(service);

        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdownTokenSource.Token);
        var taskId = Guid.NewGuid();

        _linkedTokenSources.TryAdd(taskId, linkedCts);

        await service.StartAsync(linkedCts.Token);
        _backgroundServices.TryAdd(taskId, service);

        if (service.ExecuteTask is Task task && !task.IsCompleted)
        {
            var wrappedTask = WrapTaskWithCleanupAsync(task, taskId, linkedCts);
            _backgroundTasks.TryAdd(taskId, wrappedTask);
            return;
        }

        _linkedTokenSources.TryRemove(taskId, out _);
        _backgroundServices.TryRemove(taskId, out _);
        await StopAndDisposeServiceAsync(service);
        linkedCts.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        _shutdownTokenSource.Cancel();

        try
        {
            await Task.WhenAll(_backgroundTasks.Values);
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown.
        }
        catch (AggregateException ae) when (ae.InnerExceptions.All(e => e is OperationCanceledException))
        {
            // Expected during shutdown.
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error while waiting for background tasks to stop");
        }

        var stopTasks = new List<Task>();
        foreach (var kvp in _backgroundServices)
        {
            if (_backgroundServices.TryRemove(kvp.Key, out var service))
            {
                stopTasks.Add(StopAndDisposeServiceAsync(service));
            }
        }

        await Task.WhenAll(stopTasks);

        foreach (var kvp in _linkedTokenSources)
        {
            if (_linkedTokenSources.TryRemove(kvp.Key, out var cts))
            {
                try
                {
                    cts.Dispose();
                }
                catch (ObjectDisposedException)
                {
                    // Already disposed.
                }
            }
        }

        await _cleanupTimer.DisposeAsync();
        _shutdownTokenSource.Dispose();
    }

    private void CleanupCompletedTasks(object? _)
    {
        foreach (var kvp in _backgroundTasks)
        {
            if (kvp.Value.IsCompleted)
            {
                _backgroundTasks.TryRemove(kvp.Key, out Task? _);
            }
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
            // Expected during shutdown.
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Background service faulted");
        }
        finally
        {
            _backgroundTasks.TryRemove(taskId, out _);
            _linkedTokenSources.TryRemove(taskId, out _);

            if (_backgroundServices.TryRemove(taskId, out var service))
            {
                await StopAndDisposeServiceAsync(service);
            }

            try
            {
                linkedCts.Dispose();
            }
            catch (ObjectDisposedException)
            {
                // Already disposed.
            }
        }
    }

    private async Task StopAndDisposeServiceAsync(IDisposable service)
    {
        if (service is BackgroundService backgroundService)
        {
            try
            {
                await backgroundService.StopAsync(CancellationToken.None);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error stopping background service");
            }
        }

        try
        {
            service.Dispose();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error disposing background service");
        }
    }
}

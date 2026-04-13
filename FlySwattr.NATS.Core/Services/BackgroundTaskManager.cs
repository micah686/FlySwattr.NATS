using System.Collections.Concurrent;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace FlySwattr.NATS.Core.Services;

/// <summary>
/// Owns background service lifecycle so messaging components can focus on transport concerns.
/// </summary>
internal sealed class BackgroundTaskManager : IAsyncDisposable
{
    private sealed record ConsumerRegistration(
        IDisposable Service,
        CancellationTokenSource LinkedTokenSource,
        Task? BackgroundTask);

    private readonly ILogger<BackgroundTaskManager> _logger;
    private readonly ConcurrentDictionary<Guid, ConsumerRegistration> _registrations = new();
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

        await service.StartAsync(linkedCts.Token);

        if (service.ExecuteTask is Task task && !task.IsCompleted)
        {
            var wrappedTask = WrapTaskWithCleanupAsync(task, taskId);
            _registrations[taskId] = new ConsumerRegistration(service, linkedCts, wrappedTask);
            return;
        }

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
            var backgroundTasks = _registrations.Values
                .Select(static registration => registration.BackgroundTask)
                .OfType<Task>()
                .ToArray();

            if (backgroundTasks.Length > 0)
            {
                await Task.WhenAll(backgroundTasks);
            }
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
        foreach (var kvp in _registrations)
        {
            if (_registrations.TryRemove(kvp.Key, out var registration))
            {
                stopTasks.Add(StopAndDisposeRegistrationAsync(registration));
            }
        }

        await Task.WhenAll(stopTasks);

        await _cleanupTimer.DisposeAsync();
        _shutdownTokenSource.Dispose();
    }

    private void CleanupCompletedTasks(object? _)
    {
        foreach (var kvp in _registrations)
        {
            if (kvp.Value.BackgroundTask is { IsCompleted: true } &&
                _registrations.TryRemove(kvp.Key, out var registration))
            {
                _ = StopAndDisposeRegistrationAsync(registration);
            }
        }
    }

    private async Task WrapTaskWithCleanupAsync(Task task, Guid taskId)
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
            if (_registrations.TryRemove(taskId, out var registration))
            {
                await StopAndDisposeRegistrationAsync(registration);
            }
        }
    }

    private async Task StopAndDisposeRegistrationAsync(ConsumerRegistration registration)
    {
        await StopAndDisposeServiceAsync(registration.Service);

        try
        {
            registration.LinkedTokenSource.Dispose();
        }
        catch (ObjectDisposedException)
        {
            // Already disposed.
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

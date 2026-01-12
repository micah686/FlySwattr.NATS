using FlySwattr.NATS.Abstractions;
using Microsoft.Extensions.Logging;

namespace FlySwattr.NATS.Topology.Services;

/// <summary>
/// Implementation of <see cref="ITopologyReadySignal"/> using TaskCompletionSource.
/// Provides thread-safe coordination for startup sequencing.
/// </summary>
public class TopologyReadySignal : ITopologyReadySignal
{
    private readonly TaskCompletionSource<bool> _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly ILogger<TopologyReadySignal> _logger;
    private int _signaled;

    public TopologyReadySignal(ILogger<TopologyReadySignal> logger)
    {
        _logger = logger;
    }

    /// <inheritdoc />
    public bool IsSignaled => _signaled != 0;

    /// <inheritdoc />
    public async Task WaitAsync(CancellationToken cancellationToken = default)
    {
        if (_tcs.Task.IsCompleted)
        {
            // Already signaled - return immediately (will throw if faulted)
            await _tcs.Task;
            return;
        }

        _logger.LogDebug("Waiting for topology ready signal...");

        // Create a cancellation-aware wait
        using var registration = cancellationToken.Register(
            () => _tcs.TrySetCanceled(cancellationToken),
            useSynchronizationContext: false);

        try
        {
            await _tcs.Task;
            _logger.LogDebug("Topology ready signal received.");
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug("Wait for topology signal was cancelled.");
            throw;
        }
    }

    /// <inheritdoc />
    public void SignalReady()
    {
        if (Interlocked.CompareExchange(ref _signaled, 1, 0) == 0)
        {
            _logger.LogInformation("Topology ready signal dispatched.");
            _tcs.TrySetResult(true);
        }
    }

    /// <inheritdoc />
    public void SignalFailed(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        if (Interlocked.CompareExchange(ref _signaled, 1, 0) == 0)
        {
            _logger.LogError(exception, "Topology provisioning failed. Signaling failure to dependent services.");
            _tcs.TrySetException(exception);
        }
    }
}

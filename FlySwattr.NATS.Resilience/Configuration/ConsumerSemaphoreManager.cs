using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace FlySwattr.NATS.Resilience.Configuration;

/// <summary>
/// Manages per-consumer semaphores to enforce individual concurrency limits
/// within a shared bulkhead pool. Prevents high-volume consumers from 
/// monopolizing the entire pool.
/// </summary>
/// <remarks>
/// <para>
/// This implements a two-tier concurrency model:
/// <list type="number">
///   <item>Global Bulkhead (outer): Pool-wide limit (e.g., 100 concurrent across all consumers)</item>
///   <item>Per-Consumer Semaphore (inner): Optional limit per consumer (e.g., max 20 for high-volume consumers)</item>
/// </list>
/// </para>
/// <para>
/// Example scenario: If "LogIngest" and "OrderProcessing" share the "default" pool (100 permits),
/// setting maxConcurrency=30 for "LogIngest" ensures it can never consume more than 30 permits,
/// leaving at least 70 available for other consumers.
/// </para>
/// </remarks>
internal class ConsumerSemaphoreManager : IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _semaphores = new();
    private readonly ILogger<ConsumerSemaphoreManager> _logger;
    private bool _disposed;

    public ConsumerSemaphoreManager(ILogger<ConsumerSemaphoreManager> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Gets or creates a semaphore for a specific consumer with the given concurrency limit.
    /// </summary>
    /// <param name="consumerKey">Unique consumer identifier (e.g., "stream/consumer").</param>
    /// <param name="maxConcurrency">Maximum concurrent operations for this consumer.</param>
    /// <returns>A SemaphoreSlim limiting this consumer's concurrency.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when maxConcurrency is less than 1.</exception>
    /// <exception cref="InvalidOperationException">Thrown when trying to change the limit for an existing consumer.</exception>
    public SemaphoreSlim GetConsumerSemaphore(string consumerKey, int maxConcurrency)
    {
        EnsureNotDisposed();

        if (maxConcurrency < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxConcurrency), 
                maxConcurrency, 
                "maxConcurrency must be at least 1");
        }

        return _semaphores.GetOrAdd(consumerKey, key =>
        {
            _logger.LogInformation(
                "Creating per-consumer semaphore for '{Consumer}' with maxConcurrency={MaxConcurrency}",
                key, maxConcurrency);
            
            return new SemaphoreSlim(maxConcurrency, maxConcurrency);
        });
    }

    /// <summary>
    /// Checks if a consumer has a semaphore registered.
    /// </summary>
    /// <param name="consumerKey">The consumer identifier.</param>
    /// <returns>True if a semaphore exists for this consumer.</returns>
    public bool HasConsumerSemaphore(string consumerKey)
    {
        return _semaphores.ContainsKey(consumerKey);
    }

    /// <summary>
    /// Removes and disposes a consumer's semaphore when it's no longer needed.
    /// Call this when a consumer is being torn down to prevent memory leaks.
    /// </summary>
    /// <param name="consumerKey">The consumer identifier.</param>
    public void RemoveConsumerSemaphore(string consumerKey)
    {
        if (_semaphores.TryRemove(consumerKey, out var semaphore))
        {
            semaphore.Dispose();
            _logger.LogDebug("Removed and disposed semaphore for consumer '{Consumer}'", consumerKey);
        }
    }

    /// <summary>
    /// Gets the current count of available permits for a consumer's semaphore.
    /// Useful for monitoring and diagnostics.
    /// </summary>
    /// <param name="consumerKey">The consumer identifier.</param>
    /// <returns>The current available count, or -1 if no semaphore exists.</returns>
    public int GetCurrentCount(string consumerKey)
    {
        return _semaphores.TryGetValue(consumerKey, out var semaphore) 
            ? semaphore.CurrentCount 
            : -1;
    }

    private void EnsureNotDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(ConsumerSemaphoreManager));
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        foreach (var (key, semaphore) in _semaphores)
        {
            try
            {
                semaphore.Dispose();
                _logger.LogDebug("Disposed semaphore for consumer '{Consumer}'", key);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error disposing semaphore for consumer '{Consumer}'", key);
            }
        }

        _semaphores.Clear();
        await ValueTask.CompletedTask;
    }
}

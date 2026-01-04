using System.Collections.Concurrent;
using System.Threading.RateLimiting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;

namespace FlySwattr.NATS.Resilience.Configuration;

/// <summary>
/// Configuration for bulkhead isolation pools.
/// Named pools get dedicated concurrency limits to prevent resource starvation.
/// </summary>
public class BulkheadConfiguration
{
    /// <summary>
    /// Named pools with dedicated concurrency limits.
    /// Keys are pool names, values are the maximum concurrent operations allowed.
    /// </summary>
    public Dictionary<string, int> NamedPools { get; set; } = new()
    {
        ["default"] = 100,   // Regular consumers share this pool
        ["critical"] = 10,   // DLQ, system events - guaranteed capacity
        ["highPriority"] = 20 // User-configurable priority tier
    };

    /// <summary>
    /// Queue limit multiplier relative to permit limit.
    /// For a pool with 10 permits and multiplier of 2, queue limit is 20.
    /// </summary>
    public int QueueLimitMultiplier { get; set; } = 2;
}

/// <summary>
/// Manages bulkhead isolation pools that provide dedicated concurrency limits
/// for different consumer categories. Critical consumers (like DLQ processor)
/// get their own pools that aren't affected by traffic in other pools.
/// </summary>
public class BulkheadManager : IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, (ConcurrencyLimiter Limiter, ResiliencePipeline Pipeline)> _pools = new();
    private readonly BulkheadConfiguration _config;
    private readonly ILogger<BulkheadManager> _logger;
    private bool _disposed;

    public BulkheadManager(
        IOptions<BulkheadConfiguration> config,
        ILogger<BulkheadManager> logger)
    {
        _config = config.Value;
        _logger = logger;
    }

    /// <summary>
    /// Gets the concurrency limiter for a named pool.
    /// Creates the pool on first access with configured limits.
    /// </summary>
    /// <param name="poolName">Name of the pool (e.g., "default", "critical").</param>
    /// <returns>A ConcurrencyLimiter with the pool's configured limits.</returns>
    public ConcurrencyLimiter GetPoolLimiter(string poolName)
    {
        EnsureNotDisposed();

        var (limiter, _) = GetOrCreatePool(poolName);
        return limiter;
    }

    /// <summary>
    /// Gets a resilience pipeline backed by the named pool's limiter.
    /// Use this for executing operations with pool isolation.
    /// </summary>
    /// <param name="poolName">Name of the pool.</param>
    /// <returns>A resilience pipeline with rate limiting for this pool.</returns>
    public ResiliencePipeline GetPoolPipeline(string poolName)
    {
        EnsureNotDisposed();

        var (_, pipeline) = GetOrCreatePool(poolName);
        return pipeline;
    }

    /// <summary>
    /// Gets the permit limit for a named pool.
    /// </summary>
    /// <param name="poolName">Name of the pool.</param>
    /// <returns>The maximum concurrent operations for this pool.</returns>
    public int GetPoolLimit(string poolName)
    {
        return _config.NamedPools.TryGetValue(poolName, out var limit) 
            ? limit 
            : _config.NamedPools["default"];
    }

    /// <summary>
    /// Checks if a named pool exists in configuration.
    /// </summary>
    /// <param name="poolName">Name of the pool to check.</param>
    /// <returns>True if the pool is configured.</returns>
    public bool HasPool(string poolName) => _config.NamedPools.ContainsKey(poolName);

    private (ConcurrencyLimiter Limiter, ResiliencePipeline Pipeline) GetOrCreatePool(string poolName)
    {
        return _pools.GetOrAdd(poolName, name =>
        {
            var limit = _config.NamedPools.TryGetValue(name, out var configuredLimit)
                ? configuredLimit
                : _config.NamedPools.GetValueOrDefault("default", 100);

            var queueLimit = limit * _config.QueueLimitMultiplier;

            _logger.LogInformation(
                "Creating bulkhead pool '{PoolName}' with PermitLimit={PermitLimit}, QueueLimit={QueueLimit}",
                name, limit, queueLimit);

            var limiter = new ConcurrencyLimiter(new ConcurrencyLimiterOptions
            {
                PermitLimit = limit,
                QueueLimit = queueLimit,
                QueueProcessingOrder = QueueProcessingOrder.OldestFirst
            });

            var pipeline = new ResiliencePipelineBuilder()
                .AddRateLimiter(limiter)
                .Build();

            return (limiter, pipeline);
        });
    }

    private void EnsureNotDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(BulkheadManager));
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        foreach (var (name, (limiter, _)) in _pools)
        {
            try
            {
                limiter.Dispose();
                _logger.LogDebug("Disposed bulkhead pool '{PoolName}'", name);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error disposing bulkhead pool '{PoolName}'", name);
            }
        }

        _pools.Clear();
        await ValueTask.CompletedTask;
    }
}

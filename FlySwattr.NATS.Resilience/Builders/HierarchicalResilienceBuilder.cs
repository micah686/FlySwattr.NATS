using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.CircuitBreaker;

namespace FlySwattr.NATS.Resilience.Builders;

/// <summary>
/// Configuration for consumer-level circuit breakers.
/// </summary>
public class ConsumerCircuitBreakerOptions
{
    /// <summary>
    /// The failure ratio threshold that will open the circuit (default: 30%).
    /// More sensitive than the global publisher circuit breaker (50%).
    /// </summary>
    public double FailureRatio { get; set; } = 0.3;

    /// <summary>
    /// The minimum number of calls within the sampling window required before the circuit breaker can trip.
    /// </summary>
    public int MinimumThroughput { get; set; } = 5;

    /// <summary>
    /// The sampling duration for calculating the failure ratio (default: 2 minutes).
    /// </summary>
    public TimeSpan SamplingDuration { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>
    /// How long the circuit stays open before transitioning to half-open (default: 30 seconds).
    /// </summary>
    public TimeSpan BreakDuration { get; set; } = TimeSpan.FromSeconds(30);
}

/// <summary>
/// Builds hierarchical resilience pipelines that combine global protection with consumer-level circuit breakers.
/// Each consumer gets its own circuit breaker that trips independently from others, preventing cascade failures.
/// Uses a ConcurrentDictionary with time-based eviction to prevent memory leaks from ephemeral consumers.
/// </summary>
internal class HierarchicalResilienceBuilder : IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, (ResiliencePipeline Pipeline, DateTime LastAccess)> _pipelineCache = new();
    private readonly ILogger<HierarchicalResilienceBuilder> _logger;
    private readonly ConsumerCircuitBreakerOptions _defaultOptions;
    private readonly Timer _cleanupTimer;
    private readonly TimeSpan _evictionThreshold = TimeSpan.FromMinutes(30);
    private bool _disposed;

    public HierarchicalResilienceBuilder(
        ILogger<HierarchicalResilienceBuilder> logger,
        ConsumerCircuitBreakerOptions? defaultOptions = null)
    {
        _logger = logger;
        _defaultOptions = defaultOptions ?? new ConsumerCircuitBreakerOptions();
        _cleanupTimer = new Timer(CleanupStalePipelines, null, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));
    }

    private void CleanupStalePipelines(object? state)
    {
        if (_disposed) return;

        var threshold = DateTime.UtcNow - _evictionThreshold;
        foreach (var key in _pipelineCache.Keys)
        {
            if (_pipelineCache.TryGetValue(key, out var entry) && entry.LastAccess < threshold)
            {
                if (_pipelineCache.TryRemove(key, out _))
                {
                    _logger.LogDebug("Evicted stale pipeline for {Consumer} (last access: {LastAccess})", key, entry.LastAccess);
                }
            }
        }
    }

    /// <summary>
    /// Gets or creates a composite resilience pipeline for a consumer.
    /// The pipeline layers: Global Pipeline -> Consumer Circuit Breaker
    /// </summary>
    /// <param name="consumerKey">Unique identifier for the consumer (e.g., "stream/consumer").</param>
    /// <param name="globalPipeline">The global resilience pipeline to wrap.</param>
    /// <param name="options">Optional per-consumer circuit breaker configuration.</param>
    /// <returns>A composite resilience pipeline with consumer-level circuit breaking.</returns>
    public ResiliencePipeline GetPipeline(
        string consumerKey,
        ResiliencePipeline globalPipeline,
        ConsumerCircuitBreakerOptions? options = null)
    {
        // Try to get existing pipeline and update last access time
        if (_pipelineCache.TryGetValue(consumerKey, out var existing))
        {
            // Update last access time (atomic update)
            _pipelineCache.TryUpdate(consumerKey, (existing.Pipeline, DateTime.UtcNow), existing);
            return existing.Pipeline;
        }

        // Create new pipeline
        var effectiveOptions = options ?? _defaultOptions;

        _logger.LogDebug(
            "Creating consumer circuit breaker for {Consumer} with FailureRatio={FailureRatio}, SamplingDuration={SamplingDuration}",
            consumerKey, effectiveOptions.FailureRatio, effectiveOptions.SamplingDuration);

        var pipelineBuilder = new ResiliencePipelineBuilder();

        // 1. Execute Global Pipeline first
        pipelineBuilder.AddPipeline(globalPipeline);

        // 2. Add Consumer-specific Circuit Breaker
        pipelineBuilder.AddCircuitBreaker(new CircuitBreakerStrategyOptions
        {
            Name = $"Consumer_{consumerKey}",
            FailureRatio = effectiveOptions.FailureRatio,
            MinimumThroughput = effectiveOptions.MinimumThroughput,
            SamplingDuration = effectiveOptions.SamplingDuration,
            BreakDuration = effectiveOptions.BreakDuration,
            ShouldHandle = new PredicateBuilder().Handle<Exception>(ex => !IsOperationCanceled(ex)),
            OnOpened = args =>
            {
                _logger.LogWarning("Consumer circuit breaker OPENED for {Consumer}", consumerKey);
                return default;
            },
            OnClosed = args =>
            {
                _logger.LogInformation("Consumer circuit breaker CLOSED for {Consumer}", consumerKey);
                return default;
            },
            OnHalfOpened = args =>
            {
                _logger.LogInformation("Consumer circuit breaker HALF-OPENED for {Consumer}, testing...", consumerKey);
                return default;
            }
        });

        var pipeline = pipelineBuilder.Build();

        // Use GetOrAdd to handle race conditions - another thread may have added it
        var entry = _pipelineCache.GetOrAdd(consumerKey, _ => (pipeline, DateTime.UtcNow));

        // If we lost the race, return the pipeline that was added by the other thread
        return entry.Pipeline;
    }

    /// <summary>
    /// Invalidates and removes a consumer's circuit breaker pipeline.
    /// Call this when a consumer is being torn down.
    /// </summary>
    /// <param name="consumerKey">The consumer identifier.</param>
    public void InvalidatePipeline(string consumerKey)
    {
        if (_pipelineCache.TryRemove(consumerKey, out _))
        {
            _logger.LogDebug("Invalidated and removed pipeline for {Consumer}", consumerKey);
        }
    }

    private static bool IsOperationCanceled(Exception ex) => ex is OperationCanceledException;

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        // Dispose the cleanup timer
        await _cleanupTimer.DisposeAsync();

        // Clear the cache
        _pipelineCache.Clear();
        _logger.LogDebug("HierarchicalResilienceBuilder disposed, {Count} pipelines cleared", _pipelineCache.Count);
    }
}

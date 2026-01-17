using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Resilience.Builders;
using FlySwattr.NATS.Resilience.Configuration;
using Microsoft.Extensions.Logging;
using Polly;

namespace FlySwattr.NATS.Resilience.Decorators;

/// <summary>
/// Decorator that wraps IJetStreamConsumer with bulkhead isolation, per-consumer fairness,
/// and consumer-level circuit breakers. Ensures ConsumeAsync calls respect global and 
/// per-consumer limits to prevent high-volume consumers from monopolizing shared resources.
/// </summary>
/// <remarks>
/// <para>
/// This implements a two-tier concurrency model:
/// <list type="number">
///   <item>Global Bulkhead (outer): Pool-wide limit shared across all consumers in the pool</item>
///   <item>Per-Consumer Semaphore (inner): Optional limit per consumer to ensure fairness</item>
/// </list>
/// </para>
/// </remarks>
internal class ResilientJetStreamConsumer : IJetStreamConsumer
{
    private readonly IJetStreamConsumer _inner;
    private readonly BulkheadManager _bulkheadManager;
    private readonly ConsumerSemaphoreManager _semaphoreManager;
    private readonly HierarchicalResilienceBuilder _resilienceBuilder;
    private readonly ILogger<ResilientJetStreamConsumer> _logger;

    // Global pipeline shared across all consumers for base resilience
    private readonly ResiliencePipeline _globalPipeline;

    /// <summary>
    /// Creates a resilient consumer decorator with two-tier concurrency control.
    /// </summary>
    /// <param name="inner">The underlying Core consumer (NatsJetStreamBus).</param>
    /// <param name="bulkheadManager">Manager for pool-level concurrency limits.</param>
    /// <param name="semaphoreManager">Manager for per-consumer concurrency limits.</param>
    /// <param name="resilienceBuilder">Hierarchical resilience builder for consumer-level circuit breakers.</param>
    /// <param name="logger">Logger instance.</param>
    public ResilientJetStreamConsumer(
        IJetStreamConsumer inner,
        BulkheadManager bulkheadManager,
        ConsumerSemaphoreManager semaphoreManager,
        HierarchicalResilienceBuilder resilienceBuilder,
        ILogger<ResilientJetStreamConsumer> logger)
    {
        _inner = inner;
        _bulkheadManager = bulkheadManager;
        _semaphoreManager = semaphoreManager;
        _resilienceBuilder = resilienceBuilder;
        _logger = logger;

        // Build a minimal global pipeline for consumers (bulkhead is per-pool, circuit breaker per-consumer)
        _globalPipeline = new ResiliencePipelineBuilder().Build();
    }

    public async Task ConsumeAsync<T>(
        StreamName stream,
        SubjectName subject,
        Func<IJsMessageContext<T>, Task> handler,
        QueueGroup? queueGroup = null,
        int? maxDegreeOfParallelism = null,
        int? maxConcurrency = null,
        string? bulkheadPool = null,
        CancellationToken cancellationToken = default)
    {
        var poolName = bulkheadPool ?? "default";
        var consumerKey = $"{stream.Value}/{queueGroup?.Value ?? subject.Value}";

        _logger.LogDebug(
            "Starting resilient consumer for {ConsumerKey} in pool '{Pool}' with maxConcurrency={MaxConcurrency}",
            consumerKey, poolName, maxConcurrency?.ToString() ?? "unlimited");

        // Get the bulkhead pipeline for this pool (enforces global concurrency limits)
        var bulkheadPipeline = _bulkheadManager.GetPoolPipeline(poolName);

        // Get the consumer-level circuit breaker layered on global pipeline
        var consumerPipeline = _resilienceBuilder.GetPipeline(consumerKey, _globalPipeline);

        // If maxConcurrency is specified, wrap the handler with per-consumer semaphore
        var effectiveHandler = maxConcurrency.HasValue
            ? WrapHandlerWithSemaphore(consumerKey, maxConcurrency.Value, handler)
            : handler;

        // Wrap the consume call with bulkhead + circuit breaker
        await bulkheadPipeline.ExecuteAsync(async ct =>
        {
            await consumerPipeline.ExecuteAsync(async innerCt =>
            {
                await _inner.ConsumeAsync(
                    stream, subject, effectiveHandler, queueGroup,
                    maxDegreeOfParallelism, maxConcurrency, bulkheadPool, innerCt);
            }, ct);
        }, cancellationToken);
    }

    public async Task ConsumePullAsync<T>(
        StreamName stream,
        ConsumerName consumer,
        Func<IJsMessageContext<T>, Task> handler,
        int batchSize = 10,
        int? maxDegreeOfParallelism = null,
        int? maxConcurrency = null,
        string? bulkheadPool = null,
        CancellationToken cancellationToken = default)
    {
        var poolName = bulkheadPool ?? "default";
        var consumerKey = $"{stream.Value}/{consumer.Value}";

        _logger.LogDebug(
            "Starting resilient pull consumer for {ConsumerKey} in pool '{Pool}' with maxConcurrency={MaxConcurrency}",
            consumerKey, poolName, maxConcurrency?.ToString() ?? "unlimited");

        // Get the bulkhead pipeline for this pool
        var bulkheadPipeline = _bulkheadManager.GetPoolPipeline(poolName);

        // Get the consumer-level circuit breaker
        var consumerPipeline = _resilienceBuilder.GetPipeline(consumerKey, _globalPipeline);

        // If maxConcurrency is specified, wrap the handler with per-consumer semaphore
        var effectiveHandler = maxConcurrency.HasValue
            ? WrapHandlerWithSemaphore(consumerKey, maxConcurrency.Value, handler)
            : handler;

        // Wrap the consume call
        await bulkheadPipeline.ExecuteAsync(async ct =>
        {
            await consumerPipeline.ExecuteAsync(async innerCt =>
            {
                await _inner.ConsumePullAsync(
                    stream, consumer, effectiveHandler, batchSize,
                    maxDegreeOfParallelism, maxConcurrency, bulkheadPool, innerCt);
            }, ct);
        }, cancellationToken);
    }

    /// <summary>
    /// Wraps a handler with per-consumer semaphore to enforce fairness within the global bulkhead.
    /// </summary>
    private Func<IJsMessageContext<T>, Task> WrapHandlerWithSemaphore<T>(
        string consumerKey,
        int maxConcurrency,
        Func<IJsMessageContext<T>, Task> handler)
    {
        var semaphore = _semaphoreManager.GetConsumerSemaphore(consumerKey, maxConcurrency);

        return async context =>
        {
            await semaphore.WaitAsync();
            try
            {
                await handler(context);
            }
            finally
            {
                semaphore.Release();
            }
        };
    }
}


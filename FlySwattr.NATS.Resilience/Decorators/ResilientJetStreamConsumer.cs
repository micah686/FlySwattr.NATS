using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Resilience.Builders;
using FlySwattr.NATS.Resilience.Configuration;
using Microsoft.Extensions.Logging;
using Polly;

namespace FlySwattr.NATS.Resilience.Decorators;

/// <summary>
/// Decorator that wraps IJetStreamConsumer with bulkhead isolation and consumer-level circuit breakers.
/// Ensures ConsumeAsync calls respect global and critical pool limits before spinning up background services.
/// </summary>
public class ResilientJetStreamConsumer : IJetStreamConsumer
{
    private readonly IJetStreamConsumer _inner;
    private readonly BulkheadManager _bulkheadManager;
    private readonly HierarchicalResilienceBuilder _resilienceBuilder;
    private readonly ILogger<ResilientJetStreamConsumer> _logger;

    // Global pipeline shared across all consumers for base resilience
    private readonly ResiliencePipeline _globalPipeline;

    /// <summary>
    /// Creates a resilient consumer decorator.
    /// </summary>
    /// <param name="inner">The underlying Core consumer (NatsJetStreamBus).</param>
    /// <param name="bulkheadManager">Manager for concurrency pool limits.</param>
    /// <param name="resilienceBuilder">Hierarchical resilience builder for consumer-level circuit breakers.</param>
    /// <param name="logger">Logger instance.</param>
    public ResilientJetStreamConsumer(
        IJetStreamConsumer inner,
        BulkheadManager bulkheadManager,
        HierarchicalResilienceBuilder resilienceBuilder,
        ILogger<ResilientJetStreamConsumer> logger)
    {
        _inner = inner;
        _bulkheadManager = bulkheadManager;
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
        string? bulkheadPool = null,
        CancellationToken cancellationToken = default)
    {
        var poolName = bulkheadPool ?? "default";
        var consumerKey = $"{stream.Value}/{queueGroup?.Value ?? subject.Value}";

        _logger.LogDebug(
            "Starting resilient consumer for {ConsumerKey} in pool '{Pool}'",
            consumerKey, poolName);

        // Get the bulkhead pipeline for this pool (enforces concurrency limits)
        var bulkheadPipeline = _bulkheadManager.GetPoolPipeline(poolName);

        // Get the consumer-level circuit breaker layered on global pipeline
        var consumerPipeline = _resilienceBuilder.GetPipeline(consumerKey, _globalPipeline);

        // Wrap the consume call with bulkhead + circuit breaker
        await bulkheadPipeline.ExecuteAsync(async ct =>
        {
            await consumerPipeline.ExecuteAsync(async innerCt =>
            {
                await _inner.ConsumeAsync(
                    stream, subject, handler, queueGroup,
                    maxDegreeOfParallelism, bulkheadPool, innerCt);
            }, ct);
        }, cancellationToken);
    }

    public async Task ConsumePullAsync<T>(
        StreamName stream,
        ConsumerName consumer,
        Func<IJsMessageContext<T>, Task> handler,
        int batchSize = 10,
        int? maxDegreeOfParallelism = null,
        string? bulkheadPool = null,
        CancellationToken cancellationToken = default)
    {
        var poolName = bulkheadPool ?? "default";
        var consumerKey = $"{stream.Value}/{consumer.Value}";

        _logger.LogDebug(
            "Starting resilient pull consumer for {ConsumerKey} in pool '{Pool}'",
            consumerKey, poolName);

        // Get the bulkhead pipeline for this pool
        var bulkheadPipeline = _bulkheadManager.GetPoolPipeline(poolName);

        // Get the consumer-level circuit breaker
        var consumerPipeline = _resilienceBuilder.GetPipeline(consumerKey, _globalPipeline);

        // Wrap the consume call
        await bulkheadPipeline.ExecuteAsync(async ct =>
        {
            await consumerPipeline.ExecuteAsync(async innerCt =>
            {
                await _inner.ConsumePullAsync(
                    stream, consumer, handler, batchSize,
                    maxDegreeOfParallelism, bulkheadPool, innerCt);
            }, ct);
        }, cancellationToken);
    }
}

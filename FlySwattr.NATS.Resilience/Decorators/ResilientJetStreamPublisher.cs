using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Resilience.Builders;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.CircuitBreaker;
using Polly.Retry;

namespace FlySwattr.NATS.Resilience.Decorators;

/// <summary>
/// Decorator that wraps IJetStreamPublisher with resilience policies (circuit breaker, retry).
/// Uses HierarchicalResilienceBuilder to obtain consumer-level circuit breakers layered on top of a global pipeline.
/// </summary>
internal class ResilientJetStreamPublisher : IJetStreamPublisher
{
    private readonly IJetStreamPublisher _inner;
    private readonly ResiliencePipeline _compositePipeline;
    private readonly ILogger<ResilientJetStreamPublisher> _logger;

    /// <summary>
    /// Creates a resilient publisher decorator.
    /// </summary>
    /// <param name="inner">The underlying Core publisher (NatsJetStreamBus).</param>
    /// <param name="resilienceBuilder">Hierarchical resilience builder for layered circuit breakers.</param>
    /// <param name="logger">Logger instance.</param>
    /// <param name="retryOptions">Optional retry strategy configuration (useful for testing).</param>
    public ResilientJetStreamPublisher(
        IJetStreamPublisher inner,
        HierarchicalResilienceBuilder resilienceBuilder,
        ILogger<ResilientJetStreamPublisher> logger,
        RetryStrategyOptions? retryOptions = null)
    {
        _inner = inner;
        _logger = logger;

        // Build the global publisher pipeline with retry and circuit breaker
        // Note: Hedging requires a typed ResiliencePipelineBuilder<T> and is better suited for idempotent operations
        // For publish operations, we use retry with circuit breaker protection
        var globalPipeline = new ResiliencePipelineBuilder()
            .AddCircuitBreaker(new CircuitBreakerStrategyOptions
            {
                Name = "Publisher_Global",
                FailureRatio = 0.5,
                SamplingDuration = TimeSpan.FromSeconds(30),
                MinimumThroughput = 10,
                BreakDuration = TimeSpan.FromSeconds(60),
                ShouldHandle = new PredicateBuilder().Handle<Exception>(ex => !IsOperationCanceled(ex)),
                OnOpened = args =>
                {
                    _logger.LogWarning("Publisher global circuit breaker OPENED");
                    return default;
                },
                OnClosed = args =>
                {
                    _logger.LogInformation("Publisher global circuit breaker CLOSED");
                    return default;
                }
            })
            .AddRetry(retryOptions ?? new RetryStrategyOptions
            {
                MaxRetryAttempts = 3,
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                ShouldHandle = new PredicateBuilder().Handle<Exception>(ex => IsTransient(ex))
            })
            .Build();

        // Get the composite pipeline from the builder (adds publisher-level circuit breaker)
        _compositePipeline = resilienceBuilder.GetPipeline("publisher:global", globalPipeline);
    }

    public Task PublishAsync<T>(string subject, T message, CancellationToken cancellationToken = default)
    {
        // Pass through to the overload - inner publisher will enforce messageId requirement
        return PublishAsync(subject, message, null, cancellationToken);
    }

    public async Task PublishAsync<T>(string subject, T message, string? messageId, CancellationToken cancellationToken = default)
    {
        // Pass the messageId through - the inner publisher will validate and enforce the requirement.
        // This is critical: the retry pipeline MUST use the same messageId for all attempts
        // to leverage JetStream's deduplication. Generating a new ID per retry would defeat
        // the entire purpose of both retries AND idempotency.
        await _compositePipeline.ExecuteAsync(async ct =>
        {
            await _inner.PublishAsync(subject, message, messageId, ct);
        }, cancellationToken);
    }

    private static bool IsOperationCanceled(Exception ex) => ex is OperationCanceledException;

    private static bool IsTransient(Exception ex) =>
        ex is TimeoutException or
        System.IO.IOException or
        OperationCanceledException ||
        ex is BrokenCircuitException;
}
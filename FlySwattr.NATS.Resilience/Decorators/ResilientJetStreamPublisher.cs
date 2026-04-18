using System.Net.Sockets;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Resilience.Builders;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
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
    private readonly ResiliencePipeline _globalPipeline;
    private readonly HierarchicalResilienceBuilder _resilienceBuilder;
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
        _resilienceBuilder = resilienceBuilder;

        // Build the global publisher pipeline with retry and circuit breaker.
        // Each subject prefix gets its own composite pipeline (via _resilienceBuilder) layered on top of this global one.
        _globalPipeline = new ResiliencePipelineBuilder()
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
    }

    public async Task PublishAsync<T>(string subject, T message, string? messageId, MessageHeaders? headers = null, CancellationToken cancellationToken = default)
    {
        // Use a per-subject-prefix pipeline so a failing subject cannot trip the circuit breaker
        // for unrelated subjects. The prefix (first dot-segment) groups subjects by logical stream root.
        // This is critical: the retry pipeline MUST use the same messageId for all attempts
        // to leverage JetStream's deduplication. Generating a new ID per retry would defeat
        // the entire purpose of both retries AND idempotency.
        var pipeline = _resilienceBuilder.GetPipeline($"publisher:{ExtractPrefix(subject)}", _globalPipeline);
        await pipeline.ExecuteAsync(async ct =>
        {
            await _inner.PublishAsync(subject, message, messageId, headers, ct);
        }, cancellationToken);
    }

    public async Task PublishBatchAsync<T>(
        IReadOnlyList<BatchMessage<T>> messages,
        CancellationToken cancellationToken = default)
    {
        // Wrap the entire batch in a single resilience execution under a shared "batch" key.
        // Partial retry of individual messages within a batch is complex and
        // risks duplicate publishes; the caller should retry the full batch.
        var pipeline = _resilienceBuilder.GetPipeline("publisher:batch", _globalPipeline);
        await pipeline.ExecuteAsync(async ct =>
        {
            await _inner.PublishBatchAsync(messages, ct);
        }, cancellationToken);
    }

    private static string ExtractPrefix(string subject)
    {
        if (string.IsNullOrEmpty(subject)) return "unknown";
        var dot = subject.IndexOf('.');
        return dot < 0 ? subject : subject[..dot];
    }

    private static bool IsOperationCanceled(Exception ex) => ex is OperationCanceledException;

    private static bool IsTransient(Exception ex) =>
        ex is TimeoutException
            or IOException
            or SocketException
            or NatsNoRespondersException
            or NatsException
        || (ex is NatsJSApiException jsApi && jsApi.Error.Code is 503 or 504);
}

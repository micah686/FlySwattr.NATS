using FlySwattr.NATS.Abstractions;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.CircuitBreaker;
using Polly.Hedging;
using Polly.Retry;

namespace FlySwattr.NATS.Resilience.Decorators;

public class ResilientJetStreamPublisher : IJetStreamPublisher
{
    private readonly IJetStreamPublisher _inner;
    private readonly ResiliencePipeline<bool> _pipeline;
    private readonly ILogger _logger;

    public ResilientJetStreamPublisher(IJetStreamPublisher inner, ILogger<ResilientJetStreamPublisher> logger)
    {
        _inner = inner;
        _logger = logger;

        // Build the pipeline (Logic moved from original NatsJetStreamBus constructor)
        _pipeline = new ResiliencePipelineBuilder<bool>()
           .AddCircuitBreaker(new CircuitBreakerStrategyOptions<bool>
            {
                FailureRatio = 0.5,
                SamplingDuration = TimeSpan.FromSeconds(30),
                MinimumThroughput = 10,
                BreakDuration = TimeSpan.FromSeconds(60)
            })
           .AddHedging(new HedgingStrategyOptions<bool>
            {
                MaxHedgedAttempts = 2,
                Delay = TimeSpan.FromMilliseconds(50),
                ShouldHandle = new PredicateBuilder<bool>().Handle<Exception>() // Logic to check IsTransient
            })
           .AddRetry(new RetryStrategyOptions<bool>
            {
                MaxRetryAttempts = 3,
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true
            })
           .Build();
    }

    public Task PublishAsync<T>(string subject, T message, CancellationToken cancellationToken = default)
    {
        return PublishAsync(subject, message, null, cancellationToken);
    }

    public async Task PublishAsync<T>(string subject, T message, string? messageId, CancellationToken cancellationToken = default)
    {
        // Ensure deterministic ID for Hedging/Retry de-duplication
        var actualMsgId = messageId?? $"{subject}:{Guid.NewGuid():N}";

        await _pipeline.ExecuteAsync(async ct => 
        {
            await _inner.PublishAsync(subject, message, actualMsgId, ct);
            return true;
        }, cancellationToken);
    }
}
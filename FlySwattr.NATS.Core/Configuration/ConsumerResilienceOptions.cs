using System;
using System.Collections.Generic;

namespace FlySwattr.NATS.Core.Configuration;

/// <summary>
/// Default resilience configuration for consumer message handling.
/// These settings provide sensible defaults for retry policies applied to message consumers.
/// </summary>
public class ConsumerResilienceOptions
{
    /// <summary>
    /// Maximum number of retry attempts for transient failures.
    /// Default: 3
    /// </summary>
    public int MaxRetryAttempts { get; set; } = 3;

    /// <summary>
    /// Initial delay between retry attempts (exponential backoff base).
    /// Default: 1 second
    /// </summary>
    public TimeSpan InitialRetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Maximum delay between retry attempts.
    /// Default: 30 seconds
    /// </summary>
    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Whether to add jitter to retry delays to prevent thundering herd.
    /// Default: true
    /// </summary>
    public bool UseJitter { get; set; } = true;

    /// <summary>
    /// Circuit breaker failure ratio threshold (0.0 to 1.0).
    /// When the failure ratio exceeds this threshold, the circuit opens.
    /// Default: 0.5 (50% failure rate)
    /// </summary>
    public double CircuitBreakerFailureRatio { get; set; } = 0.5;

    /// <summary>
    /// Minimum throughput before circuit breaker activates.
    /// The circuit breaker won't open until this many operations have been attempted.
    /// Default: 10
    /// </summary>
    public int CircuitBreakerMinimumThroughput { get; set; } = 10;

    /// <summary>
    /// Duration the circuit stays open before transitioning to half-open.
    /// Default: 60 seconds
    /// </summary>
    public TimeSpan CircuitBreakerBreakDuration { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Creates default NATS server-side backoff configuration based on these settings.
    /// This generates an array of TimeSpan values for ConsumerSpec.Backoff.
    /// </summary>
    /// <returns>An array of backoff durations for NATS consumer configuration.</returns>
    public TimeSpan[] ToNatsBackoffArray()
    {
        var backoffs = new List<TimeSpan>();
        var currentDelay = InitialRetryDelay;

        for (var i = 0; i < MaxRetryAttempts; i++)
        {
            backoffs.Add(currentDelay);
            currentDelay = TimeSpan.FromTicks(Math.Min(currentDelay.Ticks * 2, MaxRetryDelay.Ticks));
        }

        return backoffs.ToArray();
    }
}

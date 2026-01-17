using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace FlySwattr.NATS.Hosting.Health;

/// <summary>
/// Health check that detects "zombie" consumers where the consume loop has silently exited.
/// </summary>
internal class NatsConsumerHealthCheck : IHealthCheck
{
    private readonly IConsumerHealthMetrics _metrics;
    private readonly NatsConsumerHealthCheckOptions _options;
    private readonly TimeProvider _timeProvider;

    public NatsConsumerHealthCheck(
        IConsumerHealthMetrics metrics,
        IOptions<NatsConsumerHealthCheckOptions> options)
        : this(metrics, options, TimeProvider.System)
    {
    }

    public NatsConsumerHealthCheck(
        IConsumerHealthMetrics metrics,
        IOptions<NatsConsumerHealthCheckOptions> options,
        TimeProvider timeProvider)
    {
        _metrics = metrics;
        _options = options.Value;
        _timeProvider = timeProvider;
    }

    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        var states = _metrics.GetAllConsumerStates();
        var now = _timeProvider.GetUtcNow();

        var unhealthyConsumers = new List<string>();
        var degradedConsumers = new List<string>();

        foreach (var (key, state) in states)
        {
            if (!state.IsActive)
            {
                // Consumer was shut down, skip it
                continue;
            }

            var timeSinceLoopIteration = now - state.LastLoopIteration;
            var timeSinceHeartbeat = now - state.LastHeartbeat;
            var timeSinceStart = now - state.StartedAt;

            // Check if the loop has stopped iterating
            if (timeSinceLoopIteration > _options.LoopIterationTimeout)
            {
                unhealthyConsumers.Add(
                    $"{key} (loop stale for {timeSinceLoopIteration.TotalSeconds:F0}s)");
            }
            // Check if no messages processed for extended time (optional degraded state)
            else if (_options.NoMessageWarningTimeout.HasValue &&
                     timeSinceHeartbeat > _options.NoMessageWarningTimeout.Value &&
                     timeSinceStart > _options.NoMessageWarningTimeout.Value)
            {
                degradedConsumers.Add(
                    $"{key} (no messages for {timeSinceHeartbeat.TotalSeconds:F0}s)");
            }
        }

        if (unhealthyConsumers.Count > 0)
        {
            return Task.FromResult(HealthCheckResult.Unhealthy(
                $"Zombie consumers detected: {string.Join(", ", unhealthyConsumers)}",
                data: new Dictionary<string, object>
                {
                    ["unhealthy_consumers"] = unhealthyConsumers,
                    ["degraded_consumers"] = degradedConsumers
                }));
        }

        if (degradedConsumers.Count > 0)
        {
            return Task.FromResult(HealthCheckResult.Degraded(
                $"Consumers with no recent messages: {string.Join(", ", degradedConsumers)}",
                data: new Dictionary<string, object>
                {
                    ["degraded_consumers"] = degradedConsumers
                }));
        }

        return Task.FromResult(HealthCheckResult.Healthy(
            $"All {states.Count(s => s.Value.IsActive)} consumers are healthy."));
    }
}

/// <summary>
/// Configuration options for the consumer health check.
/// </summary>
public class NatsConsumerHealthCheckOptions
{
    /// <summary>
    /// Maximum time allowed since the last loop iteration before reporting unhealthy.
    /// Default: 2 minutes.
    /// </summary>
    public TimeSpan LoopIterationTimeout { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>
    /// Optional: Time without processing any messages before reporting degraded.
    /// Set to null to disable this check.
    /// Default: null (disabled).
    /// </summary>
    public TimeSpan? NoMessageWarningTimeout { get; set; } = null;
}

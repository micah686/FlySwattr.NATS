using FlySwattr.NATS.Abstractions;

namespace FlySwattr.NATS.Abstractions;

/// <summary>
/// Configuration options for consuming messages from JetStream.
/// </summary>
public record JetStreamConsumeOptions
{
    /// <summary>
    /// The durable consumer name (Queue Group) to join.
    /// If null, an ephemeral consumer is used.
    /// </summary>
    public QueueGroup? QueueGroup { get; init; }

    /// <summary>
    /// Maximum number of messages to process concurrently.
    /// Controls the degree of parallelism for the consumer background service.
    /// </summary>
    public int? MaxDegreeOfParallelism { get; init; }

    /// <summary>
    /// Optional: Per-consumer concurrency limit within the shared bulkhead pool.
    /// Prevents high-volume consumers from monopolizing shared resources.
    /// (Resilience Feature)
    /// </summary>
    public int? MaxConcurrency { get; init; }

    /// <summary>
    /// Optional: The name of the bulkhead pool to use for isolation (e.g., "critical", "default").
    /// (Resilience Feature)
    /// </summary>
    public string? BulkheadPool { get; init; }
    
    /// <summary>
    /// Optional: The batch size for pull consumers.
    /// Default: 10.
    /// </summary>
    public int BatchSize { get; init; } = 10;

    /// <summary>
    /// Default options.
    /// </summary>
    public static JetStreamConsumeOptions Default => new();
}

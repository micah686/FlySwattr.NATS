using FlySwattr.NATS.Abstractions;

namespace FlySwattr.NATS.Abstractions;

/// <summary>
/// Configuration options for consuming messages from JetStream.
/// </summary>
public record JetStreamConsumeOptions
{
    /// <summary>
    /// The durable JetStream consumer name to use.
    /// If null, an ephemeral consumer is used.
    /// </summary>
    public ConsumerName? DurableName { get; init; }

    /// <summary>
    /// Optional queue delivery group for push consumers.
    /// This controls load-balanced delivery semantics and is distinct from the durable consumer identity.
    /// </summary>
    public QueueGroup? DeliverGroup { get; init; }

    /// <summary>
    /// Backward-compatible alias for <see cref="DurableName"/>.
    /// </summary>
    [Obsolete("QueueGroup conflates durable consumer identity with queue delivery semantics. Use DurableName and DeliverGroup instead.")]
    public QueueGroup? QueueGroup { get; init; }

    public string? LegacyDurableName
    {
#pragma warning disable CS0618
        get => QueueGroup?.Value;
#pragma warning restore CS0618
    }

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
    /// Optional server-side cap on unacknowledged messages for the consumer.
    /// Use this with blocking writes to enforce true backpressure.
    /// </summary>
    public int? MaxAckPending { get; init; }

    /// <summary>
    /// Default options.
    /// </summary>
    public static JetStreamConsumeOptions Default => new();
}

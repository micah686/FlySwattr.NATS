using FlySwattr.NATS.Abstractions;

namespace FlySwattr.NATS.Topology.Configuration;

/// <summary>
/// Configuration options for topology provisioning startup resilience and auto-infrastructure.
/// Controls how the application handles the "Cold Start" scenario where
/// NATS may not be immediately available (e.g., Kubernetes sidecar startup delays).
/// </summary>
public class TopologyStartupOptions
{
    /// <summary>
    /// Maximum number of retry attempts for establishing initial NATS connection.
    /// Default: 10 retries.
    /// </summary>
    public int MaxRetryAttempts { get; set; } = 10;

    /// <summary>
    /// Initial delay between connection retry attempts.
    /// The delay increases exponentially with each attempt (exponential backoff).
    /// Default: 1 second.
    /// </summary>
    public TimeSpan InitialRetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Maximum delay between connection retry attempts.
    /// The exponential backoff will not exceed this value.
    /// Default: 30 seconds.
    /// </summary>
    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Timeout for a single connection ping/healthcheck attempt.
    /// If NATS doesn't respond within this time, the attempt is considered failed.
    /// Default: 5 seconds.
    /// </summary>
    public TimeSpan ConnectionTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Total timeout for the entire startup connection guard sequence.
    /// If NATS is not reachable within this time, the application will fail to start.
    /// Set to TimeSpan.Zero or negative for no global timeout (relies on retry count).
    /// Default: 2 minutes.
    /// </summary>
    public TimeSpan TotalStartupTimeout { get; set; } = TimeSpan.FromMinutes(2);

    // --- Auto-Infrastructure Options (Batteries-Included) ---

    /// <summary>
    /// When true, automatically creates the "fs-dlq-entries" KV bucket required by the
    /// DLQ store when any consumer has a <see cref="DeadLetterPolicy"/>.
    /// Default: true
    /// </summary>
    public bool AutoCreateDlqBucket { get; set; } = true;

    /// <summary>
    /// The name of the DLQ entries KV bucket.
    /// Default: "fs-dlq-entries"
    /// </summary>
    public string DlqBucketName { get; set; } = "fs-dlq-entries";

    /// <summary>
    /// When true, automatically creates the payload offloading object store bucket
    /// if <see cref="PayloadOffloadingBucketName"/> is specified.
    /// Default: true
    /// </summary>
    public bool AutoCreatePayloadOffloadingBucket { get; set; } = true;

    /// <summary>
    /// The name of the object store bucket for payload offloading (claim check pattern).
    /// When set, the bucket will be auto-created during topology provisioning.
    /// Set to null to disable auto-creation.
    /// </summary>
    public string? PayloadOffloadingBucketName { get; set; }
}

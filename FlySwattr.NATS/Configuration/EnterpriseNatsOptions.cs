using FlySwattr.NATS.Caching.Configuration;
using FlySwattr.NATS.Core.Configuration;
using FlySwattr.NATS.Hosting.Health;
using FlySwattr.NATS.Resilience.Configuration;
using FlySwattr.NATS.Topology.Configuration;

namespace FlySwattr.NATS.Configuration;

/// <summary>
/// Unified configuration options for enterprise NATS messaging.
/// Provides a "Golden Path" with production-ready defaults that ensure all critical
/// components are properly configured and ordered.
/// </summary>
/// <remarks>
/// <para>
/// This class aggregates configuration for all FlySwattr.NATS subsystems with sensible defaults:
/// </para>
/// <list type="bullet">
///   <item><description><see cref="Core"/>: NATS connection settings</description></item>
///   <item><description><see cref="PayloadOffloading"/>: Claim Check pattern for large messages (64KB threshold)</description></item>
///   <item><description><see cref="Resilience"/>: Bulkhead isolation and circuit breakers</description></item>
///   <item><description><see cref="Caching"/>: FusionCache L1+L2 for KV Store performance</description></item>
///   <item><description><see cref="HealthCheck"/>: Consumer health monitoring for zombie detection</description></item>
///   <item><description><see cref="ConsumerResilience"/>: Default retry policies for message consumption</description></item>
/// </list>
/// <para>
/// "Batteries included" features are automatically enabled:
/// </para>
/// <list type="bullet">
///   <item><description>DLQ streams are auto-derived from DeadLetterPolicy definitions</description></item>
///   <item><description>The fs-dlq-entries KV bucket is auto-created when consumers have DLQ policies</description></item>
///   <item><description>The DLQ Advisory Listener is auto-registered when topology provisioning is enabled</description></item>
///   <item><description>The payload offloading bucket is auto-created when payload offloading is enabled</description></item>
/// </list>
/// </remarks>
public class EnterpriseNatsOptions
{
    /// <summary>
    /// Core NATS connection configuration.
    /// </summary>
    public NatsConfiguration Core { get; } = new();

    /// <summary>
    /// Payload offloading configuration for the Claim Check pattern.
    /// Default threshold is 64KB to match FrostStream behavior.
    /// </summary>
    public PayloadOffloadingOptions PayloadOffloading { get; } = new()
    {
        ThresholdBytes = 64 * 1024 // 64KB - FrostStream parity
    };

    /// <summary>
    /// Resilience configuration for bulkhead isolation and circuit breakers.
    /// </summary>
    public BulkheadConfiguration Resilience { get; } = new();

    /// <summary>
    /// Default consumer resilience configuration for message handling retries.
    /// Applied to all consumers registered via AddNatsConsumer or AddNatsTopology.
    /// </summary>
    public ConsumerResilienceOptions ConsumerResilience { get; } = new();

    /// <summary>
    /// FusionCache configuration for KV Store L1+L2 caching.
    /// </summary>
    public FusionCacheConfiguration Caching { get; } = new();

    /// <summary>
    /// Consumer health check configuration for zombie detection.
    /// </summary>
    public NatsConsumerHealthCheckOptions HealthCheck { get; } = new();

    /// <summary>
    /// Topology startup configuration for "Cold Start" resilience.
    /// Controls retry behavior when NATS is not immediately available during startup
    /// (e.g., Kubernetes sidecar startup delays).
    /// </summary>
    public TopologyStartupOptions TopologyStartup { get; } = new();

    /// <summary>
    /// Object store bucket name for claim check payloads.
    /// Default: "claim-checks"
    /// </summary>
    public string ClaimCheckBucket { get; set; } = "claim-checks";

    /// <summary>
    /// Enable payload offloading (Claim Check pattern).
    /// When enabled, messages exceeding the configured <see cref="PayloadOffloadingOptions.ThresholdBytes"/> are
    /// automatically offloaded to IObjectStore.
    /// </summary>
    /// <remarks>
    /// <para>Recommended: <c>true</c> for production to prevent large message crashes.</para>
    /// <para>Set to <c>false</c> for lightweight workloads with guaranteed small payloads.</para>
    /// </remarks>
    public bool EnablePayloadOffloading { get; set; } = true;

    /// <summary>
    /// Enable resilience patterns (circuit breakers, bulkheads, retry policies).
    /// </summary>
    /// <remarks>
    /// <para>Recommended: <c>true</c> for production to prevent cascade failures.</para>
    /// <para>Set to <c>false</c> for development/testing or lightweight microservices.</para>
    /// </remarks>
    public bool EnableResilience { get; set; } = true;

    /// <summary>
    /// Enable FusionCache L1+L2 caching for KV Store operations.
    /// </summary>
    /// <remarks>
    /// <para>Recommended: <c>true</c> for production to reduce NATS network calls.</para>
    /// <para>Set to <c>false</c> when strict real-time consistency is required.</para>
    /// </remarks>
    public bool EnableCaching { get; set; } = true;

    /// <summary>
    /// Enable distributed lock provider using NATS KV Store.
    /// </summary>
    public bool EnableDistributedLock { get; set; } = true;

    /// <summary>
    /// Enable automatic topology provisioning on startup.
    /// When enabled, registered <see cref="Abstractions.ITopologySource"/> implementations
    /// are used to create streams and consumers automatically.
    /// </summary>
    public bool EnableTopologyProvisioning { get; set; } = true;

    /// <summary>
    /// Enable the DLQ Advisory Listener for monitoring MAX_DELIVERIES events.
    /// When enabled, the listener monitors NATS JetStream advisory events for delivery failures.
    /// Default: true (when topology provisioning is enabled)
    /// </summary>
    public bool EnableDlqAdvisoryListener { get; set; } = true;
}

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

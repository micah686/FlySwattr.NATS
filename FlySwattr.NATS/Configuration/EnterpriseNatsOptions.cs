using FlySwattr.NATS.Caching.Configuration;
using FlySwattr.NATS.Core.Configuration;
using FlySwattr.NATS.Hosting.Health;
using FlySwattr.NATS.Resilience.Configuration;

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
    /// FusionCache configuration for KV Store L1+L2 caching.
    /// </summary>
    public FusionCacheConfiguration Caching { get; } = new();

    /// <summary>
    /// Consumer health check configuration for zombie detection.
    /// </summary>
    public NatsConsumerHealthCheckOptions HealthCheck { get; } = new();

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
}

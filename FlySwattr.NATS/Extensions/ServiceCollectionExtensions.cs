using FlySwattr.NATS.Caching.Extensions;
using FlySwattr.NATS.Configuration;
using FlySwattr.NATS.Core.Extensions;
using FlySwattr.NATS.DistributedLock.Extensions;
using FlySwattr.NATS.Hosting.Extensions;
using FlySwattr.NATS.Resilience.Extensions;
using FlySwattr.NATS.Topology.Extensions;
using Microsoft.Extensions.DependencyInjection;

namespace FlySwattr.NATS.Extensions;

/// <summary>
/// Extension methods for setting up FlySwattr.NATS services in an <see cref="IServiceCollection" />.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds enterprise-grade NATS messaging services with the "Golden Path" configuration.
    /// This is the recommended way to configure FlySwattr.NATS for production use.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This method configures all FlySwattr.NATS subsystems in the correct order to ensure
    /// proper decorator chaining and dependency resolution:
    /// </para>
    /// <list type="number">
    ///   <item><description>Core NATS connection and bus</description></item>
    ///   <item><description>Payload offloading (Claim Check pattern for large messages)</description></item>
    ///   <item><description>Resilience (circuit breakers, bulkheads, retry policies)</description></item>
    ///   <item><description>Caching (FusionCache L1+L2 for KV Store)</description></item>
    ///   <item><description>Topology management and optional auto-provisioning</description></item>
    ///   <item><description>Distributed locking</description></item>
    ///   <item><description>Hosting services and health checks</description></item>
    /// </list>
    /// <para>
    /// <b>Important:</b> Register <see cref="Abstractions.ITopologySource"/> implementations
    /// separately using <c>AddNatsTopologySource&lt;T&gt;()</c> for declarative topology provisioning.
    /// </para>
    /// </remarks>
    /// <param name="services">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="configure">A delegate to configure the <see cref="EnterpriseNatsOptions"/>.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    /// <example>
    /// <code>
    /// services.AddEnterpriseNATSMessaging(opts =>
    /// {
    ///     // Core Connection (required)
    ///     opts.Core.Url = "nats://my-cluster:4222";
    ///     
    ///     // Custom bulkhead pools
    ///     opts.Resilience.NamedPools["default"] = 100;
    ///     opts.Resilience.NamedPools["critical"] = 10;
    ///     
    ///     // Custom cache duration
    ///     opts.Caching.MemoryCacheDuration = TimeSpan.FromMinutes(10);
    /// });
    /// 
    /// // Register topology sources separately
    /// services.AddNatsTopologySource&lt;MyApplicationTopology&gt;();
    /// </code>
    /// </example>
    public static IServiceCollection AddEnterpriseNATSMessaging(
        this IServiceCollection services,
        Action<EnterpriseNatsOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var options = new EnterpriseNatsOptions();
        configure(options);

        // 1. Core NATS (always required)
        // Establishes connection, serializers, bus, and store factories
        services.AddFlySwattrNatsCore(coreOpts =>
        {
            coreOpts.Url = options.Core.Url;
            coreOpts.NatsAuth = options.Core.NatsAuth;
            coreOpts.TlsOpts = options.Core.TlsOpts;
            coreOpts.ReconnectWait = options.Core.ReconnectWait;
            coreOpts.MaxReconnect = options.Core.MaxReconnect;
            coreOpts.MaxConcurrency = options.Core.MaxConcurrency;
        });

        // 2. Payload Offloading (Claim Check pattern)
        // Must come BEFORE Resilience to ensure proper decorator chain:
        // Core -> Offloading -> Resilience
        if (options.EnablePayloadOffloading)
        {
            services.AddPayloadOffloading(offloadOpts =>
            {
                offloadOpts.ThresholdBytes = options.PayloadOffloading.ThresholdBytes;
                offloadOpts.ClaimCheckHeaderName = options.PayloadOffloading.ClaimCheckHeaderName;
                offloadOpts.ClaimCheckTypeHeaderName = options.PayloadOffloading.ClaimCheckTypeHeaderName;
                offloadOpts.ObjectKeyPrefix = options.PayloadOffloading.ObjectKeyPrefix;
                offloadOpts.ObjectStoreServiceKey = options.PayloadOffloading.ObjectStoreServiceKey;
            }, options.ClaimCheckBucket);
        }

        // 3. Resilience (Circuit Breakers + Bulkheads)
        // Must come AFTER Offloading to wrap the decorated publisher/consumer
        if (options.EnableResilience)
        {
            services.AddFlySwattrNatsResilience(resilienceOpts =>
            {
                // Copy named pools
                resilienceOpts.NamedPools.Clear();
                foreach (var pool in options.Resilience.NamedPools)
                {
                    resilienceOpts.NamedPools[pool.Key] = pool.Value;
                }
                resilienceOpts.QueueLimitMultiplier = options.Resilience.QueueLimitMultiplier;
            });
        }

        // 4. Caching (FusionCache L1+L2 for KV Store)
        // Critical for KV Store performance - without this, every read is a network call
        if (options.EnableCaching)
        {
            services.AddFlySwattrNatsCaching(cacheOpts =>
            {
                cacheOpts.MemoryCacheDuration = options.Caching.MemoryCacheDuration;
                cacheOpts.FailSafeMaxDuration = options.Caching.FailSafeMaxDuration;
                cacheOpts.FactorySoftTimeout = options.Caching.FactorySoftTimeout;
                cacheOpts.FactoryHardTimeout = options.Caching.FactoryHardTimeout;
                cacheOpts.NotFoundCacheDuration = options.Caching.NotFoundCacheDuration;
            });
        }

        // 5. Topology Management
        // Always register the manager and ready signal for startup coordination
        services.AddFlySwattrNatsTopology();

        // Auto-provisioning is optional - requires ITopologySource registrations
        if (options.EnableTopologyProvisioning)
        {
            services.AddNatsTopologyProvisioning();
        }

        // 6. Distributed Lock
        if (options.EnableDistributedLock)
        {
            services.AddFlySwattrNatsDistributedLock();
        }

        // 7. Hosting & Health Checks
        // Consumer health metrics, startup checks, and health endpoints
        services.AddFlySwattrNatsHosting(healthOpts =>
        {
            healthOpts.LoopIterationTimeout = options.HealthCheck.LoopIterationTimeout;
            healthOpts.NoMessageWarningTimeout = options.HealthCheck.NoMessageWarningTimeout;
        });

        return services;
    }
}

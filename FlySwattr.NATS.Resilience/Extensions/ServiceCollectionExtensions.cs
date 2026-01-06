using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Resilience.Builders;
using FlySwattr.NATS.Resilience.Configuration;
using FlySwattr.NATS.Resilience.Decorators;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace FlySwattr.NATS.Resilience.Extensions;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds resilience capabilities (circuit breakers, bulkhead isolation, per-consumer fairness, retry/hedging) 
    /// to NATS services. This decorates the Core IJetStreamPublisher and IJetStreamConsumer with resilient wrappers.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional bulkhead configuration for pool limits.</param>
    /// <returns>The service collection for chaining.</returns>
    /// <remarks>
    /// <para>
    /// Call this AFTER AddFlySwattrNatsCore() to properly decorate the Core implementations.
    /// </para>
    /// <para>
    /// The decorated consumer implements a two-tier concurrency model:
    /// <list type="bullet">
    ///   <item>Global Bulkhead: Pool-wide limits shared across consumers</item>
    ///   <item>Per-Consumer Semaphore: Optional limits to prevent high-volume consumers from monopolizing resources</item>
    /// </list>
    /// </para>
    /// </remarks>
    public static IServiceCollection AddFlySwattrNatsResilience(
        this IServiceCollection services, 
        Action<BulkheadConfiguration>? configure = null)
    {
        // 1. Register Configuration & Managers
        services.AddOptions<BulkheadConfiguration>().Configure(configure ?? (_ => { }));
        services.AddSingleton<BulkheadManager>();
        services.AddSingleton<ConsumerSemaphoreManager>();
        services.AddSingleton<HierarchicalResilienceBuilder>();

        // 2. Decorate Publisher
        // Replace the existing IJetStreamPublisher with the Resilient decorator
        services.Replace(ServiceDescriptor.Singleton<IJetStreamPublisher>(sp =>
        {
            // Resolve the Core implementation (NatsJetStreamBus) explicitly
            var coreBus = sp.GetRequiredService<FlySwattr.NATS.Core.NatsJetStreamBus>();
            var resilienceBuilder = sp.GetRequiredService<HierarchicalResilienceBuilder>();
            var logger = sp.GetRequiredService<ILogger<ResilientJetStreamPublisher>>();

            return new ResilientJetStreamPublisher(coreBus, resilienceBuilder, logger);
        }));

        // 3. Decorate Consumer
        // Replace the existing IJetStreamConsumer with the Resilient decorator
        services.Replace(ServiceDescriptor.Singleton<IJetStreamConsumer>(sp =>
        {
            // Resolve the Core implementation (NatsJetStreamBus) explicitly
            var coreBus = sp.GetRequiredService<FlySwattr.NATS.Core.NatsJetStreamBus>();
            var bulkheadManager = sp.GetRequiredService<BulkheadManager>();
            var semaphoreManager = sp.GetRequiredService<ConsumerSemaphoreManager>();
            var resilienceBuilder = sp.GetRequiredService<HierarchicalResilienceBuilder>();
            var logger = sp.GetRequiredService<ILogger<ResilientJetStreamConsumer>>();

            return new ResilientJetStreamConsumer(coreBus, bulkheadManager, semaphoreManager, resilienceBuilder, logger);
        }));

        return services;
    }
}
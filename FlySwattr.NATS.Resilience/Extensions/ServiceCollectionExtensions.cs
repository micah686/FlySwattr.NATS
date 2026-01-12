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

        // 2. Decorate Publisher and Consumer generically
        Decorate<IJetStreamPublisher, ResilientJetStreamPublisher>(services);
        Decorate<IJetStreamConsumer, ResilientJetStreamConsumer>(services);

        return services;
    }

    private static void Decorate<TInterface, TDecorator>(IServiceCollection services)
        where TInterface : class
        where TDecorator : class, TInterface
    {
        // Find the last registered descriptor for the interface
        var descriptor = services.LastOrDefault(d => d.ServiceType == typeof(TInterface));
        if (descriptor == null)
        {
            throw new InvalidOperationException($"Service {typeof(TInterface).Name} is not registered. Ensure NATS Core is registered before adding Resilience.");
        }

        // Generate a unique key for the inner service
        var innerKey = $"resilience-inner-{typeof(TInterface).Name}";

        // Remove the original registration
        services.Remove(descriptor);

        // Re-register the original implementation as a keyed service
        // We assume Singleton lifetime as NATS services are typically singletons
        if (descriptor.ImplementationInstance != null)
        {
            services.AddKeyedSingleton(typeof(TInterface), innerKey, descriptor.ImplementationInstance);
        }
        else if (descriptor.ImplementationFactory != null)
        {
            services.AddKeyedSingleton(typeof(TInterface), innerKey, (sp, key) => descriptor.ImplementationFactory(sp));
        }
        else if (descriptor.ImplementationType != null)
        {
            services.AddKeyedSingleton(typeof(TInterface), innerKey, descriptor.ImplementationType);
        }

        // Register the decorator
        services.AddSingleton<TInterface>(sp =>
        {
            var inner = sp.GetRequiredKeyedService<TInterface>(innerKey);
            return ActivatorUtilities.CreateInstance<TDecorator>(sp, inner);
        });
    }
}
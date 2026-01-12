using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Topology.Configuration;
using FlySwattr.NATS.Topology.Managers;
using FlySwattr.NATS.Topology.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace FlySwattr.NATS.Topology.Extensions;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds core NATS topology management services including the <see cref="ITopologyReadySignal"/>
    /// for coordinating startup of dependent services.
    /// Call <see cref="AddNatsTopologySource{TSource}"/> to register topology sources,
    /// then call <see cref="AddNatsTopologyProvisioning"/> to enable auto-provisioning on startup.
    /// </summary>
    public static IServiceCollection AddFlySwattrNatsTopology(this IServiceCollection services)
    {
        services.TryAddSingleton<ITopologyManager, NatsTopologyManager>();
        services.TryAddSingleton<ITopologyReadySignal, TopologyReadySignal>();
        return services;
    }

    /// <summary>
    /// Registers an <see cref="ITopologySource"/> implementation that provides stream and consumer specifications.
    /// Multiple sources can be registered, and all will be collected during startup provisioning.
    /// </summary>
    /// <typeparam name="TSource">The topology source implementation type.</typeparam>
    public static IServiceCollection AddNatsTopologySource<TSource>(this IServiceCollection services)
        where TSource : class, ITopologySource
    {
        services.AddSingleton<ITopologySource, TSource>();
        return services;
    }

    /// <summary>
    /// Enables automatic topology provisioning on application startup.
    /// This registers a hosted service that collects all <see cref="ITopologySource"/> implementations
    /// and calls <see cref="ITopologyManager"/> to ensure streams and consumers exist.
    /// 
    /// The provisioning service includes a "Cold Start" protection that waits for NATS connection to be
    /// established before attempting JetStream management operations. This prevents application crash
    /// loops during infrastructure instability (e.g., Kubernetes sidecar startup delays).
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration for startup resilience options.</param>
    public static IServiceCollection AddNatsTopologyProvisioning(
        this IServiceCollection services,
        Action<TopologyStartupOptions>? configure = null)
    {
        // Register startup configuration options
        services.AddOptions<TopologyStartupOptions>()
            .Configure(configure ?? (_ => { }));

        services.AddHostedService<TopologyProvisioningService>();
        return services;
    }
}
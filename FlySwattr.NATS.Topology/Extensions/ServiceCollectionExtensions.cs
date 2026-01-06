using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Topology.Managers;
using FlySwattr.NATS.Topology.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace FlySwattr.NATS.Topology.Extensions;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds core NATS topology management services.
    /// Call <see cref="AddNatsTopologySource{TSource}"/> to register topology sources,
    /// then call <see cref="AddNatsTopologyProvisioning"/> to enable auto-provisioning on startup.
    /// </summary>
    public static IServiceCollection AddFlySwattrNatsTopology(this IServiceCollection services)
    {
        services.TryAddSingleton<ITopologyManager, NatsTopologyManager>();
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
    /// </summary>
    public static IServiceCollection AddNatsTopologyProvisioning(this IServiceCollection services)
    {
        services.AddHostedService<TopologyProvisioningService>();
        return services;
    }
}
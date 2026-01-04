using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Topology.Managers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace FlySwattr.NATS.Topology.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddFlySwattrNatsTopology(this IServiceCollection services)
    {
        services.TryAddSingleton<ITopologyManager, NatsTopologyManager>();
        return services;
    }
}
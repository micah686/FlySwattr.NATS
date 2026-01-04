using FlySwattr.NATS.DistributedLock.Services;
using Medallion.Threading;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace FlySwattr.NATS.DistributedLock.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddFlySwattrNatsDistributedLock(this IServiceCollection services)
    {
        services.TryAddSingleton<IDistributedLockProvider, NatsDistributedLockProvider>();
        return services;
    }
}
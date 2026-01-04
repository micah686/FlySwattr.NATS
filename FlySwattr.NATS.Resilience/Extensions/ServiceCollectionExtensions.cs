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
    public static IServiceCollection AddFlySwattrNatsResilience(this IServiceCollection services, Action<BulkheadConfiguration>? configure = null)
    {
        // 1. Register Configuration & Managers
        services.AddOptions<BulkheadConfiguration>().Configure(configure?? (_ => { }));
        services.AddSingleton<BulkheadManager>();
        services.AddSingleton<HierarchicalResilienceBuilder>();

        // 2. Decorate Publisher
        // We replace the existing IJetStreamPublisher with the Resilient one
        services.Replace(ServiceDescriptor.Singleton<IJetStreamPublisher>(sp =>
        {
            // Resolve the Core implementation (NatsJetStreamBus) explicitly
            var coreBus = sp.GetRequiredService<FlySwattr.NATS.Core.NatsJetStreamBus>();
            var logger = sp.GetRequiredService<ILogger<ResilientJetStreamPublisher>>();
            
            return new ResilientJetStreamPublisher(coreBus, logger);
        }));

        return services;
    }
}
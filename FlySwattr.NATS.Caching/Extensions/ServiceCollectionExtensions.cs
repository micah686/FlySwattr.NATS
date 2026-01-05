using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Caching.Stores;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using ZiggyCreatures.Caching.Fusion;

namespace FlySwattr.NATS.Caching.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddFlySwattrNatsCaching(this IServiceCollection services, Action<FusionCacheOptions>? configure = null)
    {
        // 1. Register FusionCache
        services.AddFusionCache()
               .WithDefaultEntryOptions(opts => 
                {
                    opts.Duration = TimeSpan.FromMinutes(5);
                    opts.IsFailSafeEnabled = true;
                    opts.FailSafeMaxDuration = TimeSpan.FromHours(1); // Critical for offline resilience
                    opts.FactorySoftTimeout = TimeSpan.FromMilliseconds(500);
                });

        if (configure!= null)
        {
            services.Configure(configure);
        }

        // 2. Decorate the Store Factory
        // We capture the previous factory (from Core) to create the inner instances
        // Note: This assumes AddFlySwattrNatsCore has already been called.
        
        // We need to replace the existing Func registration.
        // Since we can't easily resolving the "previous" singleton of the same type in a clean way without Scrutor,
        // we will manually assume responsibility for creating the stack here.
        
        services.Replace(ServiceDescriptor.Singleton<Func<string, IKeyValueStore>>(sp =>
        {
            var fusionCache = sp.GetRequiredService<IFusionCache>();
            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
            
            // We re-resolve the dependencies needed for the Core NatsKeyValueStore
            // This allows us to instantiate it directly inside our wrapper.
            var kvContext = sp.GetRequiredService<global::NATS.Client.KeyValueStore.INatsKVContext>();
            
            return bucket =>
            {
                // 1. Inner Store (Raw NATS)
                var innerLogger = loggerFactory.CreateLogger<FlySwattr.NATS.Core.Stores.NatsKeyValueStore>();
                var inner = new FlySwattr.NATS.Core.Stores.NatsKeyValueStore(kvContext, bucket, innerLogger);

                // 2. Outer Store (Cached)
                var cacheLogger = loggerFactory.CreateLogger<CachingKeyValueStore>();
                return new CachingKeyValueStore(inner, fusionCache, bucket, cacheLogger);
            };
        }));

        return services;
    }
}
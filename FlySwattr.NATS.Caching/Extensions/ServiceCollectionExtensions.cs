using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Caching.Configuration;
using FlySwattr.NATS.Caching.Stores;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ZiggyCreatures.Caching.Fusion;

namespace FlySwattr.NATS.Caching.Extensions;

internal static class CachingServiceKeys
{
    internal const string InnerKeyValueStoreFactory = "caching-inner-key-value-store-factory";
}

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds FusionCache-based caching layer for NATS Key-Value stores.
    /// Decorates the <see cref="IKeyValueStore"/> factory from Core with <see cref="CachingKeyValueStore"/>.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configureCache">Optional configuration action for cache settings.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddFlySwattrNatsCaching(
        this IServiceCollection services, 
        Action<FusionCacheConfiguration>? configureCache = null)
    {
        if (!services.Any(d => d.ServiceType == typeof(NatsCoreServicesMarker)))
        {
            throw new InvalidOperationException("AddFlySwattrNatsCaching requires AddFlySwattrNatsCore to be registered first.");
        }

        // 1. Register configuration with Options pattern
        if (configureCache != null)
        {
            services.Configure(configureCache);
        }
        else
        {
            // Ensure defaults are registered
            services.TryAddSingleton(Options.Create(new FusionCacheConfiguration()));
        }
        
        // 2. Register FusionCache with defaults from configuration
        services.AddFusionCache()
            .WithDefaultEntryOptions(opts =>
            {
                opts.Duration = TimeSpan.FromMinutes(5);
                opts.IsFailSafeEnabled = true;
                opts.FailSafeMaxDuration = TimeSpan.FromHours(1);
                opts.FactorySoftTimeout = TimeSpan.FromMilliseconds(500);
            });

        services.TryAddSingleton<NatsCachingMarker>();

        // 3. Decorate the Store Factory via a keyed inner registration so other decorators
        // can compose without reconstructing the raw store graph.
        var descriptor = services.LastOrDefault(d => d.ServiceType == typeof(Func<string, IKeyValueStore>));
        if (descriptor == null)
        {
            throw new InvalidOperationException("Func<string, IKeyValueStore> is not registered. Ensure NATS Core is registered before adding caching.");
        }

        services.Remove(descriptor);

        if (descriptor.ImplementationInstance != null)
        {
            services.AddKeyedSingleton(typeof(Func<string, IKeyValueStore>), CachingServiceKeys.InnerKeyValueStoreFactory, descriptor.ImplementationInstance);
        }
        else if (descriptor.ImplementationFactory != null)
        {
            services.AddKeyedSingleton(typeof(Func<string, IKeyValueStore>), CachingServiceKeys.InnerKeyValueStoreFactory, (sp, _) => descriptor.ImplementationFactory(sp));
        }
        else if (descriptor.ImplementationType != null)
        {
            services.AddKeyedSingleton(typeof(Func<string, IKeyValueStore>), CachingServiceKeys.InnerKeyValueStoreFactory, descriptor.ImplementationType);
        }

        services.AddSingleton<Func<string, IKeyValueStore>>(sp =>
        {
            var fusionCache = sp.GetRequiredService<IFusionCache>();
            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
            var cacheConfig = sp.GetRequiredService<IOptions<FusionCacheConfiguration>>().Value;
            var innerFactory = sp.GetRequiredKeyedService<Func<string, IKeyValueStore>>(CachingServiceKeys.InnerKeyValueStoreFactory);
            
            return bucket =>
            {
                var inner = innerFactory(bucket);
                var cacheLogger = loggerFactory.CreateLogger<CachingKeyValueStore>();
                return new CachingKeyValueStore(inner, fusionCache, cacheConfig, bucket, cacheLogger);
            };
        });

        return services;
    }
}

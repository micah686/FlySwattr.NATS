using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Caching.Configuration;
using FlySwattr.NATS.Caching.Stores;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ZiggyCreatures.Caching.Fusion;

namespace FlySwattr.NATS.Caching.Extensions;

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

        // 3. Decorate the Store Factory
        // We replace the factory from Core to wrap stores with caching
        // Note: This assumes AddFlySwattrNatsCore has already been called.
        services.Replace(ServiceDescriptor.Singleton<Func<string, IKeyValueStore>>(sp =>
        {
            var fusionCache = sp.GetRequiredService<IFusionCache>();
            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
            var cacheConfig = sp.GetRequiredService<IOptions<FusionCacheConfiguration>>().Value;
            
            // Re-resolve the dependencies needed for the Core NatsKeyValueStore
            var kvContext = sp.GetRequiredService<global::NATS.Client.KeyValueStore.INatsKVContext>();
            
            return bucket =>
            {
                // 1. Inner Store (Raw NATS)
                var innerLogger = loggerFactory.CreateLogger<FlySwattr.NATS.Core.Stores.NatsKeyValueStore>();
                var inner = new FlySwattr.NATS.Core.Stores.NatsKeyValueStore(kvContext, bucket, innerLogger);

                // 2. Outer Store (Cached)
                var cacheLogger = loggerFactory.CreateLogger<CachingKeyValueStore>();
                return new CachingKeyValueStore(inner, fusionCache, cacheConfig, bucket, cacheLogger);
            };
        }));

        return services;
    }
}
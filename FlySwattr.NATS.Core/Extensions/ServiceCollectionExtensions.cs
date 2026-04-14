using System.Text.Json;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Configuration;
using FlySwattr.NATS.Core.Serializers;
using FlySwattr.NATS.Core.Services;
using FlySwattr.NATS.Core.Stores;
using MemoryPack;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.KeyValueStore;
using NATS.Client.ObjectStore;

namespace FlySwattr.NATS.Core.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddFlySwattrNatsCore(
        this IServiceCollection services, 
        Action<NatsConfiguration> configure)
    {
        // 1. Configuration
        services.AddOptions<NatsConfiguration>()
            .Configure(configure)
            .ValidateOnStart();
        services.AddOptions<MessageTypeAliasOptions>();
        services.AddOptions<WireCompatibilityOptions>();
        services.AddOptions<DlqStoreFailureOptions>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<NatsConfiguration>, NatsConfigurationOptionsValidator>());
        services.AddSingleton(sp => sp.GetRequiredService<IOptions<NatsConfiguration>>().Value);
        services.TryAddSingleton<NatsCoreServicesMarker>();

        // 2. Serializers
        services.AddSingleton<INatsSerializerRegistry, HybridSerializerRegistry>();
        services.AddSingleton<IMessageSerializer>(sp =>
        {
            var config = sp.GetRequiredService<NatsConfiguration>();
            return new HybridNatsSerializer(
                maxPayloadSize: config.MaxPayloadSize,
                enforceSchemaFingerprint: config.EnforceSchemaFingerprint,
                logger: sp.GetRequiredService<ILogger<HybridNatsSerializer>>());
        });
        
        
        // 3. Connection & Contexts
        services.AddSingleton<NatsConnection>(sp =>
        {
            var natsConfig = sp.GetRequiredService<NatsConfiguration>();
            var serializerRegistry = sp.GetRequiredService<INatsSerializerRegistry>();

            var tlsOpts = natsConfig.TlsOpts ?? NatsTlsOpts.Default;

            var opts = NatsOpts.Default with
            {
                Url = natsConfig.Url,
                AuthOpts = natsConfig.NatsAuth ?? NatsAuthOpts.Default,
                TlsOpts = tlsOpts with { InsecureSkipVerify = false },
                ReconnectWaitMin = natsConfig.ReconnectWait ?? NatsOpts.Default.ReconnectWaitMin,
                MaxReconnectRetry = natsConfig.MaxReconnect ?? NatsOpts.Default.MaxReconnectRetry,
                SerializerRegistry = serializerRegistry
            };
            return new NatsConnection(opts);
        });
        services.AddSingleton<INatsConnection>(sp => sp.GetRequiredService<NatsConnection>());
        services.AddSingleton<INatsJSContext>(sp => new NatsJSContext(sp.GetRequiredService<NatsConnection>()));
        services.AddSingleton<INatsKVContext>(sp => new NatsKVContext(sp.GetRequiredService<INatsJSContext>()));
        services.AddSingleton<INatsObjContext>(sp => new NatsObjContext(sp.GetRequiredService<INatsJSContext>()));

        services.AddSingleton<IMessageBus, NatsMessageBus>();
        services.AddSingleton<BackgroundTaskManager>();
        services.AddSingleton<IMessageTypeAliasRegistry, MessageTypeAliasRegistry>();
        
        // Register NatsJetStreamBus as the implementation
        services.AddSingleton<NatsJetStreamBus>(sp =>
        {
            return new NatsJetStreamBus(
                sp.GetRequiredService<INatsJSContext>(),
                sp.GetRequiredService<ILogger<NatsJetStreamBus>>(),
                sp.GetRequiredService<IMessageSerializer>(),
                sp.GetRequiredService<BackgroundTaskManager>(),
                sp.GetService<IOptions<WireCompatibilityOptions>>());
        });
        
        // 4. Stores (Factories returning Core implementations)
        services.AddSingleton<Func<string, IKeyValueStore>>(sp => bucket => 
            new NatsKeyValueStore(
                sp.GetRequiredService<INatsKVContext>(),
                bucket,
                sp.GetRequiredService<ILogger<NatsKeyValueStore>>()
            ));
        
        services.AddSingleton<Func<string, IObjectStore>>(sp => bucket =>
            new NatsObjectStore(
                sp.GetRequiredService<INatsObjContext>(),
                bucket,
                sp.GetRequiredService<ILogger<NatsObjectStore>>()
            ));

        // 5. Core Bus (No resilience dependencies)
        //services.AddSingleton<NatsJetStreamBus>();
        services.AddSingleton<IJetStreamPublisher>(sp => sp.GetRequiredService<NatsJetStreamBus>());
        services.AddSingleton<IJetStreamConsumer>(sp => sp.GetRequiredService<NatsJetStreamBus>());
        services.AddSingleton<IAsyncDisposable>(sp => sp.GetRequiredService<NatsJetStreamBus>());

        // 6. DLQ Services
        services.AddSingleton<IDlqPolicyRegistry, DlqPolicyRegistry>();
        services.AddSingleton<IDlqStore, NatsDlqStore>();
        services.AddSingleton<IDlqNotificationService, LoggingDlqNotificationService>();
        services.AddSingleton<IDlqRemediationService>(sp => new Services.NatsDlqRemediationService(
            sp.GetRequiredService<IDlqStore>(),
            sp.GetRequiredService<IJetStreamPublisher>(),
            sp.GetRequiredService<IMessageSerializer>(),
            sp.GetRequiredService<IMessageTypeAliasRegistry>(),
            sp.GetRequiredService<ILogger<Services.NatsDlqRemediationService>>(),
            sp.GetRequiredService<NatsJetStreamBus>(),
            sp.GetService<IObjectStore>(),
            sp.GetService<IDlqNotificationService>()
        ));

        return services;
    }

    public static IServiceCollection AddMessageTypeAlias<TMessage>(
        this IServiceCollection services,
        string alias)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(alias);

        services.Configure<MessageTypeAliasOptions>(options =>
        {
            options.AliasMappings[alias] = typeof(TMessage);
        });

        return services;
    }
    
    /// <summary>
    /// Adds automatic large payload offloading (Claim Check pattern) to NATS services.
    /// Messages exceeding the configured threshold are automatically offloaded to IObjectStore.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration for offloading behavior.</param>
    /// <param name="objectStoreBucket">The object store bucket name for offloaded payloads. Default: "claim-checks"</param>
    /// <returns>The service collection for chaining.</returns>
    /// <remarks>
    /// Call this AFTER AddFlySwattrNatsCore() to properly decorate the Core implementations.
    /// If using AddFlySwattrNatsResilience(), call this BEFORE resilience to ensure proper decorator order:
    /// Core -> Offloading -> Resilience
    /// </remarks>
    public static IServiceCollection AddPayloadOffloading(
        this IServiceCollection services,
        Action<PayloadOffloadingOptions>? configure = null,
        string objectStoreBucket = "claim-checks")
    {
        if (!services.Any(d => d.ServiceType == typeof(NatsCoreServicesMarker)))
        {
            throw new InvalidOperationException("AddPayloadOffloading requires AddFlySwattrNatsCore to be registered first.");
        }

        if (services.Any(d => d.ServiceType == typeof(NatsResilienceMarker)))
        {
            throw new InvalidOperationException("AddPayloadOffloading must be registered before AddFlySwattrNatsResilience so resilience wraps the offloading decorators.");
        }

        // 1. Register Configuration — set ObjectStoreServiceKey so all paths use the same resolution model.
        // The bucket name IS the keyed service key; callers that need a different bucket override the parameter.
        services.AddOptions<PayloadOffloadingOptions>().Configure(options =>
        {
            configure?.Invoke(options);
            // Sync the service key so publisher, consumer, and Hosting consumer all
            // resolve the same IObjectStore instance without duplicating the bucket name.
            options.ObjectStoreServiceKey ??= objectStoreBucket;
        });
        services.TryAddSingleton<NatsPayloadOffloadingMarker>();

        // 2. Register a keyed IObjectStore for claim checks
        services.AddKeyedSingleton<IObjectStore>(objectStoreBucket, (sp, _) =>
            new NatsObjectStore(
                sp.GetRequiredService<INatsObjContext>(),
                objectStoreBucket,
                sp.GetRequiredService<ILogger<NatsObjectStore>>()
            ));

        // 3. Replace IJetStreamPublisher with Offloading decorator
        // Note: We resolve NatsJetStreamBus directly to avoid circular dependency
        services.Replace(ServiceDescriptor.Singleton<IJetStreamPublisher>(sp =>
        {
            var coreBus = sp.GetRequiredService<NatsJetStreamBus>();
            var options = sp.GetRequiredService<IOptions<PayloadOffloadingOptions>>();
            var serviceKey = options.Value.ObjectStoreServiceKey ?? objectStoreBucket;
            var objectStore = sp.GetRequiredKeyedService<IObjectStore>(serviceKey);
            var serializer = sp.GetRequiredService<IMessageSerializer>();
            var typeAliasRegistry = sp.GetRequiredService<IMessageTypeAliasRegistry>();
            var logger = sp.GetRequiredService<ILogger<Decorators.OffloadingJetStreamPublisher>>();

            return new Decorators.OffloadingJetStreamPublisher(coreBus, coreBus, objectStore, serializer, typeAliasRegistry, options, logger,
                sp.GetService<IOptions<WireCompatibilityOptions>>());
        }));

        // 4. Replace IJetStreamConsumer with Offloading decorator
        services.Replace(ServiceDescriptor.Singleton<IJetStreamConsumer>(sp =>
        {
            var coreBus = sp.GetRequiredService<NatsJetStreamBus>();
            var options = sp.GetRequiredService<IOptions<PayloadOffloadingOptions>>();
            var serviceKey = options.Value.ObjectStoreServiceKey ?? objectStoreBucket;
            var objectStore = sp.GetRequiredKeyedService<IObjectStore>(serviceKey);
            var serializer = sp.GetRequiredService<IMessageSerializer>();
            var logger = sp.GetRequiredService<ILogger<Decorators.OffloadingJetStreamConsumer>>();

            return new Decorators.OffloadingJetStreamConsumer(coreBus, objectStore, serializer, options, logger);
        }));

        return services;
    }
}

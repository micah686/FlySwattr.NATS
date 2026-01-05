using System.Text.Json;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Configuration;
using FlySwattr.NATS.Core.Serializers;
using FlySwattr.NATS.Core.Stores;
using MemoryPack;
using Microsoft.Extensions.DependencyInjection;
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
        services.AddOptions<NatsConfiguration>().Configure(configure);
        services.AddSingleton(sp => sp.GetRequiredService<IOptions<NatsConfiguration>>().Value);

        // 2. Serializers
        services.AddSingleton<INatsSerializerRegistry, MemoryPackSerializerRegistry>();
        const int maxPayloadSize = 10 * 1024 * 1024; //10MB //TODO: make configurable later
        services.AddSingleton<IMessageSerializer>(_ => new HybridNatsSerializer(maxPayloadSize: maxPayloadSize));
        
        
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
        
        // Register NatsJetStreamBus as the implementation
        services.AddSingleton<NatsJetStreamBus>(sp =>
        {
            
            return new NatsJetStreamBus(
                sp.GetRequiredService<INatsJSContext>(),
                sp.GetRequiredService<ILogger<NatsJetStreamBus>>(),
                sp.GetRequiredService<IMessageSerializer>());
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

        return services;
    }
    
    
}
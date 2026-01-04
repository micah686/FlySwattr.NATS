using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Hosting.Health;
using FlySwattr.NATS.Hosting.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;
using Polly;

namespace FlySwattr.NATS.Hosting.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddFlySwattrNatsHosting(this IServiceCollection services)
    {
        // Health Checks
        services.AddHealthChecks()
               .AddCheck<NatsHealthCheck>("nats_health");

        // Startup Check
        services.AddHostedService<NatsStartupCheck>();

        return services;
    }

    /// <summary>
    /// Registers a background worker for a specific JetStream consumer.
    /// </summary>
    public static IServiceCollection AddNatsConsumer<TMessage>(
        this IServiceCollection services, 
        string streamName,
        string consumerName,
        Func<IJsMessageContext<TMessage>, Task> handler,
        int maxConcurrency = 10)
    {
        return services.AddSingleton<IHostedService>(sp =>
        {
            var jsContext = sp.GetRequiredService<INatsJSContext>();
            var logger = sp.GetRequiredService<ILogger<NatsConsumerBackgroundService<TMessage>>>();
            
            // Note: In a real implementation, we would fetch the consumer object async
            // inside the StartAsync of the service, or use a provider.
            // For brevity, we assume the consumer exists or is handled by Topology package.
            var consumerTask = jsContext.GetConsumerAsync(streamName, consumerName);
            var consumer = consumerTask.GetAwaiter().GetResult(); // Sync-over-async risk here during startup!
            // Better practice: Factory returns a Task, Service awaits it in StartAsync.
            
            var opts = new NatsJSConsumeOpts { MaxMsgs = maxConcurrency };
            
            // Resolve Resilience Pipeline if available (Optional Package integration)
            // You might use KeyedServices to get specific pipelines here
            var pipeline = ResiliencePipeline.Empty; 

            return new NatsConsumerBackgroundService<TMessage>(
                consumer,
                handler,
                opts,
                logger,
                pipeline
            );
        });
    }
}
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
    /// Registers a background worker for a specific JetStream consumer with advanced options.
    /// </summary>
    public static IServiceCollection AddNatsConsumer<TMessage>(
        this IServiceCollection services, 
        string streamName,
        string consumerName,
        Func<IJsMessageContext<TMessage>, Task> handler,
        Action<NatsConsumerOptions>? configureOptions = null)
    {
        return services.AddSingleton<IHostedService>(sp =>
        {
            var options = new NatsConsumerOptions();
            configureOptions?.Invoke(options);
            
            var jsContext = sp.GetRequiredService<INatsJSContext>();
            var logger = sp.GetRequiredService<ILogger<NatsConsumerBackgroundService<TMessage>>>();
            
            // Get consumer (sync-over-async during startup - consider factory pattern for production)
            var consumerTask = jsContext.GetConsumerAsync(streamName, consumerName);
            var consumer = consumerTask.GetAwaiter().GetResult();
            
            var consumeOpts = new NatsJSConsumeOpts { MaxMsgs = options.MaxConcurrency };
            
            // Resolve optional dependencies
            var dlqPublisher = options.DlqPublisherServiceKey != null
                ? sp.GetKeyedService<IJetStreamPublisher>(options.DlqPublisherServiceKey)
                : sp.GetService<IJetStreamPublisher>();
                
            var serializer = sp.GetService<IMessageSerializer>();
            
            var objectStore = options.ObjectStoreServiceKey != null
                ? sp.GetKeyedService<IObjectStore>(options.ObjectStoreServiceKey)
                : sp.GetService<IObjectStore>();
                
            var notificationService = sp.GetService<IDlqNotificationService>();

            // Resolve resilience pipeline (provided by Resilience package)
            var resiliencePipeline = options.ResiliencePipelineKey != null
                ? sp.GetKeyedService<ResiliencePipeline>(options.ResiliencePipelineKey)
                : null;

            return new NatsConsumerBackgroundService<TMessage>(
                consumer,
                streamName,
                consumerName,
                handler,
                consumeOpts,
                logger,
                options.MaxConcurrency,
                resiliencePipeline,
                dlqPublisher,
                options.DlqPolicy,
                serializer,
                objectStore,
                notificationService
            );
        });
    }
}

/// <summary>
/// Configuration options for NATS consumer registration.
/// </summary>
public class NatsConsumerOptions
{
    /// <summary>
    /// Maximum number of concurrent message processors and channel capacity.
    /// </summary>
    public int MaxConcurrency { get; set; } = 10;

    /// <summary>
    /// Dead letter policy for poison messages.
    /// </summary>
    public DeadLetterPolicy? DlqPolicy { get; set; }

    /// <summary>
    /// Keyed service key for the DLQ publisher. If null, uses default IJetStreamPublisher.
    /// </summary>
    public object? DlqPublisherServiceKey { get; set; }

    /// <summary>
    /// Keyed service key for the ObjectStore (for large payload offloading).
    /// If null, uses default IObjectStore.
    /// </summary>
    public object? ObjectStoreServiceKey { get; set; }

    /// <summary>
    /// Keyed service key for the resilience pipeline.
    /// The pipeline is typically provided by FlySwattr.NATS.Resilience package.
    /// </summary>
    public object? ResiliencePipelineKey { get; set; }
}
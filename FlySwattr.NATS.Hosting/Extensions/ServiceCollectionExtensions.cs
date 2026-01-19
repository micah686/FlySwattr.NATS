using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core;
using FlySwattr.NATS.Hosting.Configuration;
using FlySwattr.NATS.Hosting.Health;
using FlySwattr.NATS.Hosting.Middleware;
using FlySwattr.NATS.Hosting.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;
using Polly;

// TopologyBuilder and related types are in FlySwattr.NATS.Abstractions namespace

namespace FlySwattr.NATS.Hosting.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddFlySwattrNatsHosting(
        this IServiceCollection services,
        Action<NatsConsumerHealthCheckOptions>? configureHealthCheck = null)
    {
        // Register consumer health metrics singleton for zombie detection
        services.AddSingleton<IConsumerHealthMetrics, NatsConsumerHealthMetrics>();

        // Configure consumer health check options
        if (configureHealthCheck != null)
        {
            services.Configure(configureHealthCheck);
        }

        // Health Checks
        services.AddHealthChecks()
            .AddCheck<NatsHealthCheck>("nats_health")
            .AddCheck<NatsConsumerHealthCheck>("nats_consumer_health");

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

            // Resolve health metrics for zombie detection
            var healthMetrics = sp.GetService<IConsumerHealthMetrics>();

            // Resolve topology signal for Safety Mode startup coordination
            var topologyReadySignal = sp.GetService<ITopologyReadySignal>();

            // Resolve middleware pipeline
            var middlewares = ResolveMiddlewares<TMessage>(sp, options);
            
            // Resolve or create poison message handler
            IPoisonMessageHandler<TMessage> poisonHandler;
            if (options.PoisonHandlerKey != null)
            {
                poisonHandler = sp.GetRequiredKeyedService<IPoisonMessageHandler<TMessage>>(options.PoisonHandlerKey);
            }
            else
            {
                // Register policy if provided in options and not already registered (best effort)
                // In a real app, policies should be registered via TopologyManager, but for manual consumer setup:
                var registry = sp.GetRequiredService<IDlqPolicyRegistry>();
                if (options.DlqPolicy != null)
                {
                    registry.Register(streamName, consumerName, options.DlqPolicy);
                }
                
                poisonHandler = new DefaultDlqPoisonHandler<TMessage>(
                    dlqPublisher,
                    serializer,
                    objectStore,
                    notificationService,
                    registry,
                    sp.GetRequiredService<ILogger<DefaultDlqPoisonHandler<TMessage>>>()
                );
            }

            return new NatsConsumerBackgroundService<TMessage>(
                consumer,
                streamName,
                consumerName,
                handler,
                consumeOpts,
                logger,
                poisonHandler,
                options.MaxConcurrency,
                resiliencePipeline,
                healthMetrics,
                topologyReadySignal,
                middlewares
            );
        });
    }

    /// <summary>
    /// Resolves and instantiates the middleware pipeline for a consumer.
    /// </summary>
    private static IEnumerable<IConsumerMiddleware<TMessage>> ResolveMiddlewares<TMessage>(
        IServiceProvider sp, 
        NatsConsumerOptions options)
    {
        var middlewares = new List<IConsumerMiddleware<TMessage>>();

        // Add built-in middleware if enabled
        if (options.EnableLoggingMiddleware)
        {
            middlewares.Add(ActivatorUtilities.CreateInstance<LoggingMiddleware<TMessage>>(sp));
        }

        if (options.EnableValidationMiddleware)
        {
            middlewares.Add(ActivatorUtilities.CreateInstance<ValidationMiddleware<TMessage>>(sp));
        }

        // Add custom middleware types
        foreach (var middlewareType in options.MiddlewareTypes)
        {
            if (ActivatorUtilities.CreateInstance(sp, middlewareType) is IConsumerMiddleware<TMessage> middleware)
            {
                middlewares.Add(middleware);
            }
        }

        return middlewares;
    }

    /// <summary>
    /// Adds the DLQ Advisory Listener service that monitors NATS JetStream advisory events
    /// for consumer delivery failures (MAX_DELIVERIES exceeded).
    /// </summary>
    /// <remarks>
    /// This provides "Control Plane" active monitoring, giving operations teams visibility into
    /// server-side delivery failures that would otherwise go unnoticed. When a consumer exceeds
    /// its MaxDeliver limit, the NATS server publishes an advisory event that this service captures.
    /// </remarks>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration for advisory filtering and behavior.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddNatsDlqAdvisoryListener(
        this IServiceCollection services,
        Action<DlqAdvisoryListenerOptions>? configure = null)
    {
        // Configure options
        if (configure != null)
        {
            services.Configure(configure);
        }
        else
        {
            services.Configure<DlqAdvisoryListenerOptions>(_ => { });
        }

        // Register default logging handler (uses TryAddEnumerable to allow multiple handlers)
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IDlqAdvisoryHandler, LoggingDlqAdvisoryHandler>());
        
        // Register DLQ store handler to persist entries for remediation UI
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IDlqAdvisoryHandler, DlqStoreAdvisoryHandler>());

        // Register the background service
        services.AddHostedService<DlqAdvisoryListenerService>();

        return services;
    }

    /// <summary>
    /// Registers a custom <see cref="IDlqAdvisoryHandler"/> implementation for handling
    /// NATS JetStream advisory events.
    /// </summary>
    /// <remarks>
    /// Multiple handlers can be registered and will all be invoked when an advisory is received.
    /// Use this to integrate with external alerting systems like PagerDuty, Slack, or custom monitoring.
    /// </remarks>
    /// <typeparam name="THandler">The handler implementation type.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddDlqAdvisoryHandler<THandler>(this IServiceCollection services)
        where THandler : class, IDlqAdvisoryHandler
    {
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IDlqAdvisoryHandler, THandler>());
        return services;
    }

    /// <summary>
    /// Registers an <see cref="ITopologySource"/> implementation with unified consumer handler mappings
    /// and starts the consumers automatically.
    /// This is the "batteries included" unified registration API.
    /// </summary>
    /// <typeparam name="TSource">The topology source implementation type.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Configuration delegate to map consumers to handlers.</param>
    /// <returns>The service collection for chaining.</returns>
    /// <remarks>
    /// <para>
    /// This method combines topology registration with consumer handler registration and startup,
    /// eliminating the need to define consumers in two places (topology source and AddNatsConsumer).
    /// </para>
    /// <para>
    /// The topology source's ConsumerSpec definitions provide the stream name, durable name,
    /// and other JetStream configuration. The handler mappings connect those consumers to
    /// your message processing logic.
    /// </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// builder.Services.AddNatsTopologyWithConsumers&lt;OrdersTopology&gt;(topology =>
    /// {
    ///     topology.MapConsumer&lt;OrderPlacedEvent&gt;("orders-consumer", async ctx =>
    ///     {
    ///         // Process the order
    ///         await ctx.AckAsync();
    ///     });
    ///
    ///     topology.MapConsumer&lt;PaymentProcessedEvent&gt;("payments-consumer", async ctx =>
    ///     {
    ///         // Process payment
    ///         await ctx.AckAsync();
    ///     });
    /// });
    /// </code>
    /// </example>
    public static IServiceCollection AddNatsTopologyWithConsumers<TSource>(
        this IServiceCollection services,
        Action<TopologyBuilder<TSource>> configure)
        where TSource : class, ITopologySource
    {
        ArgumentNullException.ThrowIfNull(configure);

        // Register the topology source
        services.AddSingleton<ITopologySource, TSource>();
        services.AddSingleton<TSource>();

        // Build the handler mappings
        var builder = new TopologyBuilder<TSource>();
        configure(builder);

        // Register the builder's mappings for use by TopologyConsumerHostedService
        services.AddSingleton(builder);

        // Register the hosted service that creates consumers from topology
        services.AddHostedService<TopologyConsumerHostedService<TSource>>();

        return services;
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
    
    /// <summary>
    /// Keyed service key for a custom IPoisonMessageHandler&lt;T&gt;.
    /// If null, a DefaultDlqPoisonHandler is created.
    /// </summary>
    public object? PoisonHandlerKey { get; set; }

    /// <summary>
    /// Whether to enable the built-in logging middleware.
    /// When enabled, logs message handling start/end with duration.
    /// Default: true.
    /// </summary>
    public bool EnableLoggingMiddleware { get; set; } = true;

    /// <summary>
    /// Whether to enable the built-in validation middleware.
    /// When enabled, validates messages using registered IValidator&lt;T&gt; implementations.
    /// Validation failures are routed directly to DLQ without retries.
    /// Default: true.
    /// </summary>
    public bool EnableValidationMiddleware { get; set; } = true;

    /// <summary>
    /// Custom middleware types to include in the pipeline.
    /// Middleware types must implement IConsumerMiddleware&lt;TMessage&gt;.
    /// Middleware executes in the order they are added.
    /// </summary>
    public List<Type> MiddlewareTypes { get; } = new();

    /// <summary>
    /// Adds a custom middleware type to the pipeline.
    /// </summary>
    /// <typeparam name="TMiddleware">The middleware type.</typeparam>
    /// <returns>The options instance for chaining.</returns>
    public NatsConsumerOptions AddMiddleware<TMiddleware>() where TMiddleware : class
    {
        MiddlewareTypes.Add(typeof(TMiddleware));
        return this;
    }
}
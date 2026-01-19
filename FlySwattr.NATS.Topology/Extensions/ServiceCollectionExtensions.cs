using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Topology.Configuration;
using FlySwattr.NATS.Topology.Managers;
using FlySwattr.NATS.Topology.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace FlySwattr.NATS.Topology.Extensions;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds core NATS topology management services including the <see cref="ITopologyReadySignal"/>
    /// for coordinating startup of dependent services.
    /// Call <see cref="AddNatsTopologySource{TSource}"/> to register topology sources,
    /// then call <see cref="AddNatsTopologyProvisioning"/> to enable auto-provisioning on startup.
    /// </summary>
    public static IServiceCollection AddFlySwattrNatsTopology(this IServiceCollection services)
    {
        services.TryAddSingleton<ITopologyManager, NatsTopologyManager>();
        services.TryAddSingleton<ITopologyReadySignal, TopologyReadySignal>();
        return services;
    }

    /// <summary>
    /// Registers an <see cref="ITopologySource"/> implementation that provides stream and consumer specifications.
    /// Multiple sources can be registered, and all will be collected during startup provisioning.
    /// </summary>
    /// <typeparam name="TSource">The topology source implementation type.</typeparam>
    public static IServiceCollection AddNatsTopologySource<TSource>(this IServiceCollection services)
        where TSource : class, ITopologySource
    {
        services.AddSingleton<ITopologySource, TSource>();
        return services;
    }

    /// <summary>
    /// Registers an <see cref="ITopologySource"/> implementation with unified consumer handler mappings.
    /// This provides the "batteries included" experience where topology and handlers are defined together.
    /// </summary>
    /// <typeparam name="TSource">The topology source implementation type.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Configuration delegate to map consumers to handlers.</param>
    /// <returns>The service collection for chaining.</returns>
    /// <remarks>
    /// <para>
    /// This method combines topology registration with consumer handler registration, eliminating
    /// the need to define consumers in two places (topology source and AddNatsConsumer).
    /// </para>
    /// <para>
    /// The topology source's ConsumerSpec definitions provide the stream name, durable name,
    /// and other JetStream configuration. The handler mappings connect those consumers to
    /// your message processing logic.
    /// </para>
    /// <para>
    /// <b>Note:</b> This method registers the topology source and handler mappings, but does not
    /// start the consumers. Use <c>AddNatsTopologyWithConsumers</c> from FlySwattr.NATS.Hosting
    /// if you want the consumers to start automatically.
    /// </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// builder.Services.AddNatsTopology&lt;OrdersTopology&gt;(topology =>
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
    public static IServiceCollection AddNatsTopology<TSource>(
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

        // Register the builder's mappings
        services.AddSingleton(builder);

        return services;
    }

    /// <summary>
    /// Enables automatic topology provisioning on application startup.
    /// This registers a hosted service that collects all <see cref="ITopologySource"/> implementations
    /// and calls <see cref="ITopologyManager"/> to ensure streams and consumers exist.
    ///
    /// The provisioning service includes a "Cold Start" protection that waits for NATS connection to be
    /// established before attempting JetStream management operations. This prevents application crash
    /// loops during infrastructure instability (e.g., Kubernetes sidecar startup delays).
    ///
    /// "Batteries included" features are automatically enabled:
    /// - DLQ streams are auto-derived from consumer DeadLetterPolicy definitions
    /// - The fs-dlq-entries KV bucket is auto-created when consumers have DLQ policies
    /// - The payload offloading bucket is auto-created when configured
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration for startup resilience options.</param>
    public static IServiceCollection AddNatsTopologyProvisioning(
        this IServiceCollection services,
        Action<TopologyStartupOptions>? configure = null)
    {
        // Register startup configuration options
        services.AddOptions<TopologyStartupOptions>()
            .Configure(configure ?? (_ => { }));

        services.AddHostedService<TopologyProvisioningService>();
        return services;
    }
}
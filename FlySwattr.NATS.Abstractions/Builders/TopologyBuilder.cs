// ReSharper disable CheckNamespace
// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Abstractions;

/// <summary>
/// Builder for configuring topology with consumer handlers.
/// Enables unified registration of topology specifications and message handlers.
/// </summary>
/// <remarks>
/// This provides the "batteries included" developer experience where consumers
/// are defined once in the topology source, and handlers are mapped directly to them.
/// </remarks>
public class TopologyBuilder<TTopology> where TTopology : class, ITopologySource
{
    private readonly Dictionary<string, ConsumerHandlerRegistration> _handlerMappings = new();

    /// <summary>
    /// Gets the registered consumer handler mappings.
    /// Key is the consumer's durable name.
    /// </summary>
    public IReadOnlyDictionary<string, ConsumerHandlerRegistration> HandlerMappings => _handlerMappings;

    /// <summary>
    /// Maps a message handler to a consumer defined in the topology source.
    /// </summary>
    /// <typeparam name="TMessage">The message type this consumer handles.</typeparam>
    /// <param name="consumerName">The durable name of the consumer as defined in the topology source.</param>
    /// <param name="handler">The handler function to invoke for each message.</param>
    /// <returns>The builder for chaining.</returns>
    /// <example>
    /// <code>
    /// builder.Services.AddNatsTopology&lt;OrdersTopology&gt;(topology =>
    /// {
    ///     topology.MapConsumer&lt;OrderPlacedEvent&gt;("orders-consumer", async ctx =>
    ///     {
    ///         // Process the order
    ///         await ctx.AckAsync();
    ///     });
    /// });
    /// </code>
    /// </example>
    public TopologyBuilder<TTopology> MapConsumer<TMessage>(
        string consumerName,
        Func<IJsMessageContext<TMessage>, Task> handler)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerName);
        ArgumentNullException.ThrowIfNull(handler);

        _handlerMappings[consumerName] = new ConsumerHandlerRegistration(
            typeof(TMessage),
            consumerName,
            handler
        );

        return this;
    }

    /// <summary>
    /// Maps a message handler to a consumer with additional options.
    /// </summary>
    /// <typeparam name="TMessage">The message type this consumer handles.</typeparam>
    /// <param name="consumerName">The durable name of the consumer as defined in the topology source.</param>
    /// <param name="handler">The handler function to invoke for each message.</param>
    /// <param name="configureOptions">Optional configuration for the consumer.</param>
    /// <returns>The builder for chaining.</returns>
    public TopologyBuilder<TTopology> MapConsumer<TMessage>(
        string consumerName,
        Func<IJsMessageContext<TMessage>, Task> handler,
        Action<TopologyConsumerOptions>? configureOptions)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerName);
        ArgumentNullException.ThrowIfNull(handler);

        var options = new TopologyConsumerOptions();
        configureOptions?.Invoke(options);

        _handlerMappings[consumerName] = new ConsumerHandlerRegistration(
            typeof(TMessage),
            consumerName,
            handler,
            options
        );

        return this;
    }
}

/// <summary>
/// Represents a registered consumer handler mapping.
/// </summary>
public class ConsumerHandlerRegistration
{
    public ConsumerHandlerRegistration(
        Type messageType,
        string consumerName,
        object handler,
        TopologyConsumerOptions? options = null)
    {
        MessageType = messageType;
        ConsumerName = consumerName;
        Handler = handler;
        Options = options ?? new TopologyConsumerOptions();
    }

    /// <summary>
    /// The type of message this consumer handles.
    /// </summary>
    public Type MessageType { get; }

    /// <summary>
    /// The durable name of the consumer.
    /// </summary>
    public string ConsumerName { get; }

    /// <summary>
    /// The handler delegate (typed as Func&lt;IJsMessageContext&lt;TMessage&gt;, Task&gt;).
    /// </summary>
    public object Handler { get; }

    /// <summary>
    /// Additional consumer options.
    /// </summary>
    public TopologyConsumerOptions Options { get; }
}

/// <summary>
/// Configuration options for a consumer registered via the topology builder.
/// </summary>
public class TopologyConsumerOptions
{
    /// <summary>
    /// Maximum number of concurrent message processors.
    /// If not specified, uses the value from ConsumerSpec.DegreeOfParallelism or defaults to 10.
    /// </summary>
    public int? MaxConcurrency { get; set; }

    /// <summary>
    /// Whether to enable the built-in logging middleware.
    /// Default: true
    /// </summary>
    public bool EnableLoggingMiddleware { get; set; } = true;

    /// <summary>
    /// Whether to enable the built-in validation middleware.
    /// Default: true
    /// </summary>
    public bool EnableValidationMiddleware { get; set; } = true;

    /// <summary>
    /// Keyed service key for a custom resilience pipeline.
    /// </summary>
    public object? ResiliencePipelineKey { get; set; }

    /// <summary>
    /// Custom middleware types to include in the pipeline.
    /// </summary>
    public List<Type> MiddlewareTypes { get; } = new();

    /// <summary>
    /// Adds a custom middleware type to the pipeline.
    /// </summary>
    public TopologyConsumerOptions AddMiddleware<TMiddleware>() where TMiddleware : class
    {
        MiddlewareTypes.Add(typeof(TMiddleware));
        return this;
    }
}

using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core;
using FlySwattr.NATS.Core.Configuration;
using FlySwattr.NATS.Hosting.Extensions;
using FlySwattr.NATS.Hosting.Health;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.JetStream;
using Polly;
using Polly.Retry;

namespace FlySwattr.NATS.Hosting.Services;

/// <summary>
/// A hosted service that creates and manages consumer background workers based on topology mappings.
/// This service bridges the gap between topology definitions and consumer handlers, providing
/// the "batteries included" unified registration experience.
/// </summary>
/// <typeparam name="TSource">The topology source type.</typeparam>
internal class TopologyConsumerHostedService<TSource> : IHostedService
    where TSource : class, ITopologySource
{
    private readonly TSource _topologySource;
    private readonly TopologyBuilder<TSource> _builder;
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<TopologyConsumerHostedService<TSource>> _logger;
    private readonly ITopologyReadySignal? _readySignal;
    private readonly List<IHostedService> _consumers = new();
    private CancellationTokenSource? _cts;

    public TopologyConsumerHostedService(
        TSource topologySource,
        TopologyBuilder<TSource> builder,
        IServiceProvider serviceProvider,
        ILogger<TopologyConsumerHostedService<TSource>> logger,
        ITopologyReadySignal? readySignal = null)
    {
        _topologySource = topologySource;
        _builder = builder;
        _serviceProvider = serviceProvider;
        _logger = logger;
        _readySignal = readySignal;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        // Wait for topology to be ready before starting consumers
        if (_readySignal != null)
        {
            _logger.LogInformation("Waiting for topology to be ready before starting consumers...");
            await _readySignal.WaitAsync(cancellationToken);
        }

        // Get all consumer specs from the topology source
        var consumerSpecs = _topologySource.GetConsumers().ToList();

        foreach (var spec in consumerSpecs)
        {
            // Check if there's a handler mapping for this consumer
            if (!_builder.HandlerMappings.TryGetValue(spec.DurableName.Value, out var registration))
            {
                _logger.LogDebug(
                    "No handler mapped for consumer {Consumer} on stream {Stream}. Skipping.",
                    spec.DurableName, spec.StreamName);
                continue;
            }

            try
            {
                await StartConsumerAsync(spec, registration, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Failed to start consumer {Consumer} on stream {Stream}. Continuing with remaining consumers.",
                    spec.DurableName, spec.StreamName);
            }
        }

        _logger.LogInformation("Started {Count} consumer(s) from topology mappings.", _consumers.Count);
    }

    private async Task StartConsumerAsync(
        ConsumerSpec spec,
        ConsumerHandlerRegistration registration,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting consumer {Consumer} on stream {Stream} for message type {MessageType}...",
            spec.DurableName, spec.StreamName, registration.MessageType.Name);

        // Get the JetStream context
        var jsContext = _serviceProvider.GetRequiredService<INatsJSContext>();

        // Get the consumer from JetStream
        var consumer = await jsContext.GetConsumerAsync(spec.StreamName.Value, spec.DurableName.Value, cancellationToken);

        // Determine concurrency
        var maxConcurrency = registration.Options.MaxConcurrency
                             ?? spec.DegreeOfParallelism
                             ?? 10;

        var consumeOpts = new NatsJSConsumeOpts { MaxMsgs = maxConcurrency };

        // Create the options for the background service
        var options = new NatsConsumerOptions
        {
            MaxConcurrency = maxConcurrency,
            EnableLoggingMiddleware = registration.Options.EnableLoggingMiddleware,
            EnableValidationMiddleware = registration.Options.EnableValidationMiddleware,
            ResiliencePipelineKey = registration.Options.ResiliencePipelineKey,
            DlqPolicy = spec.DeadLetterPolicy,
            DlqPublisherServiceKey = registration.Options.DlqPublisherServiceKey,
            PoisonHandlerKey = registration.Options.PoisonHandlerKey,
            AckTimeout = registration.Options.AckTimeout,
            InProgressHeartbeatInterval = registration.Options.InProgressHeartbeatInterval
        };

        foreach (var middlewareType in registration.Options.MiddlewareTypes)
        {
            options.MiddlewareTypes.Add(middlewareType);
        }

        // Use reflection to create the typed NatsConsumerBackgroundService
        var workerType = typeof(NatsConsumerBackgroundService<>).MakeGenericType(registration.MessageType);
        var loggerType = typeof(ILogger<>).MakeGenericType(workerType);

        // Get poison handler
        var poisonHandlerType = typeof(IPoisonMessageHandler<>).MakeGenericType(registration.MessageType);
        var defaultPoisonHandlerType = typeof(DefaultDlqPoisonHandler<>).MakeGenericType(registration.MessageType);

        var offloadingOptions = _serviceProvider.GetService<Microsoft.Extensions.Options.IOptions<FlySwattr.NATS.Core.Configuration.PayloadOffloadingOptions>>()?.Value;
        var dlqRegistry = _serviceProvider.GetRequiredService<IDlqPolicyRegistry>();

        // Register policy if provided
        if (spec.DeadLetterPolicy != null)
        {
            dlqRegistry.Register(spec.StreamName.Value, spec.DurableName.Value, spec.DeadLetterPolicy);
        }

        // Resolve poison handler: use keyed service if specified, otherwise construct default
        object poisonHandler;
        if (registration.Options.PoisonHandlerKey != null)
        {
            poisonHandler = _serviceProvider.GetRequiredKeyedService(poisonHandlerType, registration.Options.PoisonHandlerKey);
        }
        else
        {
            // Use ActivatorUtilities so all DI-registered constructor params are resolved
            // automatically (IJetStreamPublisher, IMessageSerializer, IObjectStore, loggers, etc.)
            // without enumerating them positionally here.
            poisonHandler = ActivatorUtilities.CreateInstance(_serviceProvider, defaultPoisonHandlerType);
        }

        // Resolve services still needed for the worker (claim-check hydration support)
        var serializer = _serviceProvider.GetService<IMessageSerializer>();

        // Resolve IObjectStore: prefer the consumer-specific key, then fall back to the
        // key embedded in PayloadOffloadingOptions (set by AddPayloadOffloading), then default.
        var objectStoreKey = registration.Options.ObjectStoreServiceKey ?? offloadingOptions?.ObjectStoreServiceKey;
        var objectStore = objectStoreKey != null
            ? _serviceProvider.GetKeyedService<IObjectStore>(objectStoreKey)
            : _serviceProvider.GetService<IObjectStore>();

        // Fail fast: if offloading is configured but the object store is not resolvable,
        // offloaded messages would arrive with empty payloads and be Term()'d as fatal errors.
        if (offloadingOptions != null && objectStore == null)
        {
            throw new InvalidOperationException(
                $"Payload offloading is configured (PayloadOffloadingOptions is registered) but no IObjectStore " +
                $"could be resolved for consumer '{spec.DurableName}' on stream '{spec.StreamName}'. " +
                $"Ensure AddPayloadOffloading() is called with the correct bucket, or set " +
                $"NatsConsumerOptions.ObjectStoreServiceKey to the keyed IObjectStore service key.");
        }

        // Resolve optional services
        var healthMetrics = _serviceProvider.GetService<IConsumerHealthMetrics>();
        var resiliencePipeline = registration.Options.ResiliencePipelineKey != null
            ? _serviceProvider.GetKeyedService<ResiliencePipeline>(registration.Options.ResiliencePipelineKey)
            : null;

        if (resiliencePipeline == null)
        {
            var consumerResilienceOpts = _serviceProvider.GetService<Microsoft.Extensions.Options.IOptions<ConsumerResilienceOptions>>()?.Value;
            if (consumerResilienceOpts != null)
                resiliencePipeline = BuildDefaultResiliencePipeline(consumerResilienceOpts);
        }

        // Build middleware
        var middlewares = ResolveMiddlewares(registration.MessageType, _serviceProvider, options);

        var workerLogger = _serviceProvider.GetRequiredService(loggerType);

        // Note: NatsConsumerBackgroundService<T> is generic and constructed per message type at
        // runtime. The static Create factory is the single point where the constructor signature
        // is defined; if the factory signature changes, the compiler will enforce updates here.
        var createMethod = workerType.GetMethod(
            "Create",
            System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic);

        var worker = createMethod!.Invoke(null, new object?[]
        {
            consumer,
            spec.StreamName.Value,
            spec.DurableName.Value,
            registration.Handler,
            consumeOpts,
            workerLogger,
            poisonHandler,
            serializer,
            objectStore,
            offloadingOptions,
            maxConcurrency,
            options.AckTimeout,
            options.InProgressHeartbeatInterval,
            resiliencePipeline,
            healthMetrics,
            _readySignal,
            middlewares
        });

        if (worker is IHostedService hostedService)
        {
            await hostedService.StartAsync(cancellationToken);
            _consumers.Add(hostedService);
        }
    }

    /// <summary>
    /// Resolves and instantiates all configured middlewares for a given message type.
    /// Returns a typed array (<c>IConsumerMiddleware&lt;T&gt;[]</c>) so the caller does not
    /// need an additional reflection-based <c>Add</c> loop.  The instances themselves are
    /// created with <see cref="ActivatorUtilities.CreateInstance"/> rather than
    /// <see cref="Activator.CreateInstance"/> so their own DI dependencies are resolved
    /// automatically.
    /// </summary>
    private static object ResolveMiddlewares(Type messageType, IServiceProvider sp, NatsConsumerOptions options)
    {
        var middlewareInterfaceType = typeof(IConsumerMiddleware<>).MakeGenericType(messageType);

        // Accumulate into a plain List<object> so we avoid a reflected generic List<T> with a
        // reflected Add() call.  We convert to a typed array at the end — arrays are covariant
        // over IEnumerable<T>, so the consumer constructor's IEnumerable<IConsumerMiddleware<T>>?
        // parameter accepts the result without further reflection.
        var instances = new List<object>();

        if (ServiceCollectionExtensions.ShouldEnableWireCompatibilityMiddleware(sp))
        {
            var wireCompatibilityMiddlewareType = typeof(Middleware.WireVersionCheckMiddleware<>).MakeGenericType(messageType);
            instances.Add(ActivatorUtilities.CreateInstance(sp, wireCompatibilityMiddlewareType));
        }

        if (options.EnableLoggingMiddleware)
        {
            var loggingMiddlewareType = typeof(Middleware.LoggingMiddleware<>).MakeGenericType(messageType);
            instances.Add(ActivatorUtilities.CreateInstance(sp, loggingMiddlewareType));
        }

        if (options.EnableValidationMiddleware)
        {
            var validationMiddlewareType = typeof(Middleware.ValidationMiddleware<>).MakeGenericType(messageType);
            instances.Add(ActivatorUtilities.CreateInstance(sp, validationMiddlewareType));
        }

        foreach (var middlewareType in options.MiddlewareTypes)
        {
            instances.Add(ActivatorUtilities.CreateInstance(sp, middlewareType));
        }

        // Build a typed IConsumerMiddleware<T>[] so the consumer background service receives a
        // correctly typed IEnumerable without requiring additional reflection at the call site.
        var typedArray = Array.CreateInstance(middlewareInterfaceType, instances.Count);
        for (var i = 0; i < instances.Count; i++)
        {
            typedArray.SetValue(instances[i], i);
        }

        return typedArray;
    }

    private static ResiliencePipeline BuildDefaultResiliencePipeline(ConsumerResilienceOptions opts)
    {
        var retryStrategy = new RetryStrategyOptions
        {
            ShouldHandle = new PredicateBuilder().Handle<Exception>(),
            BackoffType = DelayBackoffType.Exponential,
            MaxRetryAttempts = opts.MaxRetryAttempts,
            UseJitter = opts.UseJitter,
        };

        return new ResiliencePipelineBuilder()
            .AddRetry(retryStrategy)
            .Build();
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping topology consumer hosted service...");

        if (_cts != null)
        {
            await _cts.CancelAsync();
        }

        // Stop all consumers
        foreach (var consumer in _consumers)
        {
            try
            {
                await consumer.StopAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error stopping consumer.");
            }
        }

        _consumers.Clear();
    }
}

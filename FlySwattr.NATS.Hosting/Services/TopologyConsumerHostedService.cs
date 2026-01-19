using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core;
using FlySwattr.NATS.Hosting.Extensions;
using FlySwattr.NATS.Hosting.Health;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;
using Polly;

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
            DlqPolicy = spec.DeadLetterPolicy
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

        var dlqPublisher = _serviceProvider.GetService<IJetStreamPublisher>();
        var serializer = _serviceProvider.GetService<IMessageSerializer>();
        var objectStore = _serviceProvider.GetService<IObjectStore>();
        var notificationService = _serviceProvider.GetService<IDlqNotificationService>();
        var dlqRegistry = _serviceProvider.GetRequiredService<IDlqPolicyRegistry>();

        // Register policy if provided
        if (spec.DeadLetterPolicy != null)
        {
            dlqRegistry.Register(spec.StreamName.Value, spec.DurableName.Value, spec.DeadLetterPolicy);
        }

        var poisonHandlerLoggerType = typeof(ILogger<>).MakeGenericType(defaultPoisonHandlerType);
        var poisonHandlerLogger = _serviceProvider.GetRequiredService(poisonHandlerLoggerType);

        var poisonHandler = Activator.CreateInstance(
            defaultPoisonHandlerType,
            dlqPublisher,
            serializer,
            objectStore,
            notificationService,
            dlqRegistry,
            poisonHandlerLogger);

        // Resolve optional services
        var healthMetrics = _serviceProvider.GetService<IConsumerHealthMetrics>();
        var resiliencePipeline = registration.Options.ResiliencePipelineKey != null
            ? _serviceProvider.GetKeyedService<ResiliencePipeline>(registration.Options.ResiliencePipelineKey)
            : null;

        // Build middleware
        var middlewares = ResolveMiddlewares(registration.MessageType, _serviceProvider, options);

        var workerLogger = _serviceProvider.GetRequiredService(loggerType);

        var worker = Activator.CreateInstance(
            workerType,
            consumer,
            spec.StreamName.Value,
            spec.DurableName.Value,
            registration.Handler,
            consumeOpts,
            workerLogger,
            poisonHandler,
            maxConcurrency,
            resiliencePipeline,
            healthMetrics,
            _readySignal,
            middlewares);

        if (worker is IHostedService hostedService)
        {
            await hostedService.StartAsync(cancellationToken);
            _consumers.Add(hostedService);
        }
    }

    private static object ResolveMiddlewares(Type messageType, IServiceProvider sp, NatsConsumerOptions options)
    {
        var middlewareInterfaceType = typeof(IConsumerMiddleware<>).MakeGenericType(messageType);
        var listType = typeof(List<>).MakeGenericType(middlewareInterfaceType);
        var middlewares = Activator.CreateInstance(listType)!;
        var addMethod = listType.GetMethod("Add")!;

        if (options.EnableLoggingMiddleware)
        {
            var loggingMiddlewareType = typeof(Middleware.LoggingMiddleware<>).MakeGenericType(messageType);
            var loggingMiddleware = ActivatorUtilities.CreateInstance(sp, loggingMiddlewareType);
            addMethod.Invoke(middlewares, new[] { loggingMiddleware });
        }

        if (options.EnableValidationMiddleware)
        {
            var validationMiddlewareType = typeof(Middleware.ValidationMiddleware<>).MakeGenericType(messageType);
            var validationMiddleware = ActivatorUtilities.CreateInstance(sp, validationMiddlewareType);
            addMethod.Invoke(middlewares, new[] { validationMiddleware });
        }

        foreach (var middlewareType in options.MiddlewareTypes)
        {
            var middleware = ActivatorUtilities.CreateInstance(sp, middlewareType);
            addMethod.Invoke(middlewares, new[] { middleware });
        }

        return middlewares;
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

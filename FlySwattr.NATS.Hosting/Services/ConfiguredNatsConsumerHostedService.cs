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

namespace FlySwattr.NATS.Hosting.Services;

internal sealed class ConfiguredNatsConsumerHostedService<TMessage> : IHostedService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly string _streamName;
    private readonly string _consumerName;
    private readonly Func<IJsMessageContext<TMessage>, Task> _handler;
    private readonly NatsConsumerOptions _options;
    private NatsConsumerBackgroundService<TMessage>? _worker;

    public ConfiguredNatsConsumerHostedService(
        IServiceProvider serviceProvider,
        string streamName,
        string consumerName,
        Func<IJsMessageContext<TMessage>, Task> handler,
        NatsConsumerOptions options)
    {
        _serviceProvider = serviceProvider;
        _streamName = streamName;
        _consumerName = consumerName;
        _handler = handler;
        _options = options;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var jsContext = _serviceProvider.GetRequiredService<INatsJSContext>();
        var consumer = await jsContext.GetConsumerAsync(_streamName, _consumerName, cancellationToken);
        var logger = _serviceProvider.GetRequiredService<ILogger<NatsConsumerBackgroundService<TMessage>>>();
        var serializer = _serviceProvider.GetService<IMessageSerializer>();
        var objectStore = _options.ObjectStoreServiceKey != null
            ? _serviceProvider.GetKeyedService<IObjectStore>(_options.ObjectStoreServiceKey)
            : _serviceProvider.GetService<IObjectStore>();

        var dlqPublisher = _options.DlqPublisherServiceKey != null
            ? _serviceProvider.GetKeyedService<IJetStreamPublisher>(_options.DlqPublisherServiceKey)
            : _serviceProvider.GetService<IJetStreamPublisher>();

        var notificationService = _serviceProvider.GetService<IDlqNotificationService>();
        var resiliencePipeline = _options.ResiliencePipelineKey != null
            ? _serviceProvider.GetKeyedService<ResiliencePipeline>(_options.ResiliencePipelineKey)
            : null;
        var healthMetrics = _serviceProvider.GetService<IConsumerHealthMetrics>();
        var topologyReadySignal = _serviceProvider.GetService<ITopologyReadySignal>();
        var middlewares = ServiceCollectionExtensions.ResolveMiddlewares<TMessage>(_serviceProvider, _options);
        var offloadingOptions = _serviceProvider.GetService<IOptions<PayloadOffloadingOptions>>()?.Value;

        IPoisonMessageHandler<TMessage> poisonHandler;
        if (_options.PoisonHandlerKey != null)
        {
            poisonHandler = _serviceProvider.GetRequiredKeyedService<IPoisonMessageHandler<TMessage>>(_options.PoisonHandlerKey);
        }
        else
        {
            var registry = _serviceProvider.GetRequiredService<IDlqPolicyRegistry>();
            if (_options.DlqPolicy != null)
            {
                registry.Register(_streamName, _consumerName, _options.DlqPolicy);
            }

            poisonHandler = new DefaultDlqPoisonHandler<TMessage>(
                dlqPublisher,
                serializer,
                _serviceProvider.GetRequiredService<IMessageTypeAliasRegistry>(),
                objectStore,
                notificationService,
                registry,
                _serviceProvider.GetRequiredService<ILogger<DefaultDlqPoisonHandler<TMessage>>>());
        }

        _worker = new NatsConsumerBackgroundService<TMessage>(
            consumer,
            _streamName,
            _consumerName,
            _handler,
            new NatsJSConsumeOpts { MaxMsgs = _options.MaxConcurrency },
            logger,
            poisonHandler,
            serializer,
            objectStore,
            offloadingOptions,
            _options.MaxConcurrency,
            resiliencePipeline,
            healthMetrics,
            topologyReadySignal,
            middlewares);

        await _worker.StartAsync(cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken)
        => _worker?.StopAsync(cancellationToken) ?? Task.CompletedTask;
}

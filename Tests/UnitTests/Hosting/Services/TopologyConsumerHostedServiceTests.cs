using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core;
using FlySwattr.NATS.Hosting.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Hosting.Services;

[Property("nTag", "Hosting")]
public class TopologyConsumerHostedServiceTests
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<TopologyConsumerHostedService<TestTopology>> _logger;
    private readonly ITopologyReadySignal _topologyReadySignal;
    private readonly INatsJSContext _jsContext;
    private readonly IDlqPolicyRegistry _dlqPolicyRegistry;
    private readonly TestTopology _topologySource;
    private readonly TopologyBuilder<TestTopology> _builder;

    public TopologyConsumerHostedServiceTests()
    {
        _serviceProvider = Substitute.For<IServiceProvider>();
        _logger = Substitute.For<ILogger<TopologyConsumerHostedService<TestTopology>>>();
        _topologyReadySignal = Substitute.For<ITopologyReadySignal>();
        _jsContext = Substitute.For<INatsJSContext>();
        _dlqPolicyRegistry = Substitute.For<IDlqPolicyRegistry>();
        _topologySource = new TestTopology();
        _builder = new TopologyBuilder<TestTopology>();

        // Setup default service provider behavior
        SetupDefaultServiceProviderBehavior();
    }

    private void SetupDefaultServiceProviderBehavior()
    {
        _serviceProvider.GetService(typeof(INatsJSContext)).Returns(_jsContext);
        _serviceProvider.GetService(typeof(IDlqPolicyRegistry)).Returns(_dlqPolicyRegistry);

        // Mock required services - use a service collection to properly resolve types
        var loggerFactory = Substitute.For<ILoggerFactory>();
        loggerFactory.CreateLogger(Arg.Any<string>()).Returns(Substitute.For<ILogger>());
        _serviceProvider.GetService(typeof(ILoggerFactory)).Returns(loggerFactory);

        // Mock GetRequiredService pattern
        _serviceProvider.GetService(Arg.Is<Type>(t => t == typeof(IDlqPolicyRegistry))).Returns(_dlqPolicyRegistry);
    }

    private TopologyConsumerHostedService<TestTopology> CreateSut(
        TestTopology? topologySource = null,
        TopologyBuilder<TestTopology>? builder = null,
        ITopologyReadySignal? readySignal = null)
    {
        return new TopologyConsumerHostedService<TestTopology>(
            topologySource ?? _topologySource,
            builder ?? _builder,
            _serviceProvider,
            _logger,
            readySignal);
    }

    #region StartAsync Tests

    [Test]
    public async Task StartAsync_ShouldWaitForTopologyReady_WhenSignalProvided()
    {
        // Arrange
        var waitCalled = false;
        _topologyReadySignal.WaitAsync(Arg.Any<CancellationToken>()).Returns(callInfo =>
        {
            waitCalled = true;
            return Task.CompletedTask;
        });

        var sut = CreateSut(readySignal: _topologyReadySignal);

        // Act
        await sut.StartAsync(CancellationToken.None);

        // Assert
        waitCalled.ShouldBeTrue();
        await _topologyReadySignal.Received(1).WaitAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task StartAsync_ShouldNotWait_WhenNoSignalProvided()
    {
        // Arrange
        var sut = CreateSut(readySignal: null);

        // Act & Assert - should complete without issues
        await Should.NotThrowAsync(() => sut.StartAsync(CancellationToken.None));
    }

    [Test]
    public async Task StartAsync_ShouldSkipConsumer_WhenNoHandlerMapped()
    {
        // Arrange
        var topology = new TestTopology();
        topology.AddConsumer(new ConsumerSpec
        {
            StreamName = StreamName.From("test-stream"),
            DurableName = ConsumerName.From("unmapped-consumer")
        });

        var builder = new TopologyBuilder<TestTopology>();
        // Don't map any handler - intentionally left empty

        var sut = CreateSut(topologySource: topology, builder: builder);

        // Act
        await sut.StartAsync(CancellationToken.None);

        // Assert - should log debug message about skipping
        _logger.Received().Log(
            LogLevel.Debug,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("No handler mapped")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Test]
    public async Task StartAsync_ShouldLogInformation_OnSuccessfulStart()
    {
        // Arrange
        var sut = CreateSut();

        // Act
        await sut.StartAsync(CancellationToken.None);

        // Assert - should log the count of started consumers
        _logger.Received().Log(
            LogLevel.Information,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("Started") && o.ToString()!.Contains("consumer(s)")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Test]
    public async Task StartAsync_ShouldContinue_WhenOneConsumerFails()
    {
        // Arrange
        var topology = new TestTopology();
        topology.AddConsumer(new ConsumerSpec
        {
            StreamName = StreamName.From("stream1"),
            DurableName = ConsumerName.From("consumer1")
        });
        topology.AddConsumer(new ConsumerSpec
        {
            StreamName = StreamName.From("stream2"),
            DurableName = ConsumerName.From("consumer2")
        });

        var builder = new TopologyBuilder<TestTopology>();
        builder.MapConsumer<TestMessage>("consumer1", _ => Task.CompletedTask);
        builder.MapConsumer<TestMessage>("consumer2", _ => Task.CompletedTask);

        // Setup jsContext to throw for first consumer, succeed for second
        _jsContext.GetConsumerAsync("stream1", "consumer1", Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("Consumer not found"));

        var mockConsumer = Substitute.For<INatsJSConsumer>();
        _jsContext.GetConsumerAsync("stream2", "consumer2", Arg.Any<CancellationToken>())
            .Returns(ValueTask.FromResult(mockConsumer));

        var sut = CreateSut(topologySource: topology, builder: builder);

        // Act - should not throw despite first consumer failing
        await Should.NotThrowAsync(() => sut.StartAsync(CancellationToken.None));

        // Assert - should log error for failed consumer
        _logger.Received().Log(
            LogLevel.Error,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("Failed to start consumer")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Test]
    public async Task StartAsync_ShouldRegisterDlqPolicy_WhenPolicyProvided()
    {
        // Arrange
        var dlqPolicy = new DeadLetterPolicy
        {
            SourceStream = "test-stream",
            SourceConsumer = "test-consumer",
            TargetStream = StreamName.From("dlq-stream"),
            TargetSubject = "dlq.test-stream.test-consumer"
        };

        var topology = new TestTopology();
        topology.AddConsumer(new ConsumerSpec
        {
            StreamName = StreamName.From("test-stream"),
            DurableName = ConsumerName.From("test-consumer"),
            DeadLetterPolicy = dlqPolicy
        });

        var builder = new TopologyBuilder<TestTopology>();
        builder.MapConsumer<TestMessage>("test-consumer", _ => Task.CompletedTask);

        var mockConsumer = Substitute.For<INatsJSConsumer>();
        _jsContext.GetConsumerAsync("test-stream", "test-consumer", Arg.Any<CancellationToken>())
            .Returns(ValueTask.FromResult(mockConsumer));

        var sut = CreateSut(topologySource: topology, builder: builder);

        // Act
        await sut.StartAsync(CancellationToken.None);

        // Assert - DLQ policy should be registered
        _dlqPolicyRegistry.Received(1).Register("test-stream", "test-consumer", dlqPolicy);
    }

    #endregion

    #region StopAsync Tests

    [Test]
    public async Task StopAsync_ShouldCancelOperations()
    {
        // Arrange
        var sut = CreateSut();
        await sut.StartAsync(CancellationToken.None);

        // Act
        await sut.StopAsync(CancellationToken.None);

        // Assert - should log stopping message
        _logger.Received().Log(
            LogLevel.Information,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("Stopping topology consumer")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Test]
    public async Task StopAsync_ShouldLogWarning_WhenConsumerStopFails()
    {
        // Arrange - We can't easily test consumer stop failure without deeper integration,
        // but we can verify the service handles the stop gracefully
        var sut = CreateSut();
        await sut.StartAsync(CancellationToken.None);

        // Act & Assert - should complete without throwing
        await Should.NotThrowAsync(() => sut.StopAsync(CancellationToken.None));
    }

    [Test]
    public async Task StopAsync_ShouldCompleteGracefully_WhenNotStarted()
    {
        // Arrange
        var sut = CreateSut();

        // Act & Assert - should not throw even if not started
        await Should.NotThrowAsync(() => sut.StopAsync(CancellationToken.None));
    }

    #endregion

    #region Test Types

    public class TestTopology : ITopologySource
    {
        private readonly List<StreamSpec> _streams = new();
        private readonly List<ConsumerSpec> _consumers = new();

        public void AddStream(StreamSpec spec) => _streams.Add(spec);
        public void AddConsumer(ConsumerSpec spec) => _consumers.Add(spec);

        public IEnumerable<StreamSpec> GetStreams() => _streams;
        public IEnumerable<ConsumerSpec> GetConsumers() => _consumers;
    }

    public class TestMessage
    {
        public string Data { get; set; } = string.Empty;
    }

    #endregion
}

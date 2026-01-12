using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Topology.Configuration;
using FlySwattr.NATS.Topology.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NSubstitute;
using TUnit.Core;

namespace UnitTests.ControlPlane;

[Property("nTag", "Topology")]
public class TopologyProvisioningServiceTests
{
    private readonly IEnumerable<ITopologySource> _topologySources;
    private readonly ITopologyManager _topologyManager;
    private readonly INatsConnection _connection;
    private readonly ITopologyReadySignal _readySignal;
    private readonly ILogger<TopologyProvisioningService> _logger;
    private readonly IOptions<TopologyStartupOptions> _options;
    private readonly TopologyProvisioningService _sut;

    public TopologyProvisioningServiceTests()
    {
        _topologySources = new List<ITopologySource>();
        _topologyManager = Substitute.For<ITopologyManager>();
        _connection = Substitute.For<INatsConnection>();
        _readySignal = Substitute.For<ITopologyReadySignal>();
        _logger = Substitute.For<ILogger<TopologyProvisioningService>>();
        
        var config = new TopologyStartupOptions 
        { 
            MaxRetryAttempts = 1, 
            InitialRetryDelay = TimeSpan.FromMilliseconds(10),
            MaxRetryDelay = TimeSpan.FromMilliseconds(100),
            TotalStartupTimeout = TimeSpan.FromSeconds(1)
        };
        _options = Options.Create(config);

        _sut = new TopologyProvisioningService(
            _topologySources, 
            _topologyManager, 
            _connection, 
            _logger, 
            _options, 
            _readySignal);
    }

    [Test]
    public async Task StartAsync_ShouldWaitForConnection_WhenClosed()
    {
        // Arrange
        _connection.ConnectionState.Returns(NatsConnectionState.Closed);
        
        // Simulate connection becoming open after ping
        // PingAsync returns ValueTask<TimeSpan>. 
        // We use a Func that returns ValueTask<TimeSpan>.
        _connection.PingAsync(Arg.Any<CancellationToken>()).Returns(x => 
        {
            _connection.ConnectionState.Returns(NatsConnectionState.Open);
            return new ValueTask<TimeSpan>(TimeSpan.Zero);
        });

        // Act
        await _sut.StartAsync(CancellationToken.None);

        // Assert
        await _connection.Received(1).PingAsync(Arg.Any<CancellationToken>());
        _readySignal.Received(1).SignalReady();
    }

    [Test]
    public async Task StartAsync_ShouldProvisionTopology_WhenConnected()
    {
        // Arrange
        _connection.ConnectionState.Returns(NatsConnectionState.Open);
        
        var source = Substitute.For<ITopologySource>();
        var stream = new StreamSpec { Name = StreamName.From("test-stream") };
        var consumer = new ConsumerSpec { StreamName = StreamName.From("test-stream"), DurableName = ConsumerName.From("test-consumer") };
        
        source.GetStreams().Returns(new[] { stream });
        source.GetConsumers().Returns(new[] { consumer });
        
        var sources = new[] { source };
        var sut = new TopologyProvisioningService(sources, _topologyManager, _connection, _logger, _options, _readySignal);

        // Act
        await sut.StartAsync(CancellationToken.None);

        // Assert
        await _topologyManager.Received(1).EnsureStreamAsync(stream, Arg.Any<CancellationToken>());
        await _topologyManager.Received(1).EnsureConsumerAsync(consumer, Arg.Any<CancellationToken>());
        _readySignal.Received(1).SignalReady();
    }

    [Test]
    public async Task StartAsync_ShouldSignalFailure_OnUnrecoverableError()
    {
        // Arrange
        _connection.ConnectionState.Returns(NatsConnectionState.Open);
        
        // Simulate a critical failure (not just a provisioning error, but something fatal in the loop logic or source)
        // However, the service is designed to catch individual provisioning errors and continue.
        // To trigger critical failure, we can throw from source enumeration or manager if we want to test that path,
        // BUT standard provisioning errors are swallowed/logged.
        // Let's verify that a source throwing exception propagates.
        
        var failingSource = Substitute.For<ITopologySource>();
        failingSource.GetStreams().Returns(x => throw new Exception("Critical source error"));
        
        var sut = new TopologyProvisioningService(new[] { failingSource }, _topologyManager, _connection, _logger, _options, _readySignal);

        // Act & Assert
        try
        {
            await sut.StartAsync(CancellationToken.None);
            Assert.Fail("Should have thrown");
        }
        catch (Exception)
        {
            // Expected
        }

        _readySignal.Received(1).SignalFailed(Arg.Any<Exception>());
    }

    [Test]
    public async Task StartAsync_ShouldContinue_OnIndividualProvisioningErrors()
    {
        // Arrange
        _connection.ConnectionState.Returns(NatsConnectionState.Open);
        
        var source = Substitute.For<ITopologySource>();
        var goodStream = new StreamSpec { Name = StreamName.From("good") };
        var badStream = new StreamSpec { Name = StreamName.From("bad") };
        
        source.GetStreams().Returns(new[] { badStream, goodStream });
        
        _topologyManager.EnsureStreamAsync(badStream, Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new Exception("Provisioning failed")));

        var sut = new TopologyProvisioningService(new[] { source }, _topologyManager, _connection, _logger, _options, _readySignal);

        // Act
        await sut.StartAsync(CancellationToken.None);

        // Assert
        // Should have tried both
        await _topologyManager.Received(1).EnsureStreamAsync(badStream, Arg.Any<CancellationToken>());
        await _topologyManager.Received(1).EnsureStreamAsync(goodStream, Arg.Any<CancellationToken>());
        
        // Should still signal ready (partial success is allowed)
        _readySignal.Received(1).SignalReady();
    }
}

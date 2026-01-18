using FlySwattr.NATS.Topology.Services;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Shouldly;
using TUnit.Core;

namespace UnitTests.ControlPlane;

[Property("nTag", "Topology")]
public class TopologyReadySignalTests
{
    private readonly ILogger<TopologyReadySignal> _logger;
    private readonly TopologyReadySignal _sut;

    public TopologyReadySignalTests()
    {
        _logger = Substitute.For<ILogger<TopologyReadySignal>>();
        _sut = new TopologyReadySignal(_logger);
    }

    [Test]
    public async Task SignalReady_ShouldCompleteWaitTask()
    {
        // Arrange
        var waitTask = _sut.WaitAsync(CancellationToken.None);
        _sut.IsSignaled.ShouldBeFalse();

        // Act
        _sut.SignalReady();

        // Assert
        await waitTask; // Should complete successfully
        _sut.IsSignaled.ShouldBeTrue();
    }

    [Test]
    public async Task SignalFailed_ShouldFaultWaitTask()
    {
        // Arrange
        var waitTask = _sut.WaitAsync(CancellationToken.None);
        var exception = new Exception("Startup failed");

        // Act
        _sut.SignalFailed(exception);

        // Assert
        try
        {
            await waitTask;
            Assert.Fail("Should have thrown");
        }
        catch (Exception ex)
        {
            ex.ShouldBe(exception);
        }
        _sut.IsSignaled.ShouldBeTrue(); // Signaled (even if failed)
    }

    [Test]
    public void SignalReady_ShouldBeIdempotent()
    {
        // Act
        _sut.SignalReady();
        _sut.SignalReady();

        // Assert
        // Should verify log called only once
        _logger.Received(1).Log(
            LogLevel.Information,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("Topology ready signal dispatched")),
            null,
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Test]
    public async Task WaitAsync_ShouldReturnImmediately_IfAlreadySignaled()
    {
        // Arrange
        _sut.SignalReady();

        // Act
        await _sut.WaitAsync(CancellationToken.None);

        // Assert
        _sut.IsSignaled.ShouldBeTrue();
    }
}

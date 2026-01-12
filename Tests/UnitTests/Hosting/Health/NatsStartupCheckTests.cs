using FlySwattr.NATS.Hosting.Health;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Hosting.Health;

[Property("nTag", "Hosting")]
public class NatsStartupCheckTests
{
    private readonly INatsConnection _connection;
    private readonly ILogger<NatsStartupCheck> _logger;
    private readonly NatsStartupCheck _startupCheck;

    public NatsStartupCheckTests()
    {
        _connection = Substitute.For<INatsConnection>();
        _logger = Substitute.For<ILogger<NatsStartupCheck>>();
        _startupCheck = new NatsStartupCheck(_connection, _logger);
    }

    [Test]
    public async Task StartupCheck_ShouldBlock_UntilConnectionEstablished()
    {
        // Arrange
        _connection.PingAsync(Arg.Any<CancellationToken>()).Returns(ValueTask.FromResult(TimeSpan.Zero));

        // Act
        await _startupCheck.StartAsync(CancellationToken.None);

        // Assert
        await _connection.Received(1).PingAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task StartupCheck_ShouldTimeout_WhenConnectionFails()
    {
        // Arrange
        var exception = new NatsException("Connection failed");
        _connection.PingAsync(Arg.Any<CancellationToken>()).Throws(exception);

        // Act & Assert
        var ex = await Assert.ThrowsAsync<NatsException>(async () => 
            await _startupCheck.StartAsync(CancellationToken.None));
            
        ex?.Message.ShouldBe("Connection failed");
    }
}

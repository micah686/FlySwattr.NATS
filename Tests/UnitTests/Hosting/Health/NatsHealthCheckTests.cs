using FlySwattr.NATS.Hosting.Health;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NSubstitute;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Hosting.Health;

[Property("nTag", "Hosting")]
public class NatsHealthCheckTests
{
    private readonly INatsConnection _connection;
    private readonly INatsJSContext _jsContext;
    private readonly NatsHealthCheck _healthCheck;

    public NatsHealthCheckTests()
    {
        _connection = Substitute.For<INatsConnection>();
        _jsContext = Substitute.For<INatsJSContext>();
        _healthCheck = new NatsHealthCheck(_connection, _jsContext);
    }

    [Test]
    public async Task HealthCheck_ShouldReturnHealthy_WhenConnectionOpenAndJsResponding()
    {
        // Arrange
        _connection.ConnectionState.Returns(NatsConnectionState.Open);
        _jsContext.GetAccountInfoAsync(Arg.Any<CancellationToken>())
            .Returns(ValueTask.FromResult(new AccountInfoResponse()));

        // Act
        var result = await _healthCheck.CheckHealthAsync(new HealthCheckContext());

        // Assert
        result.Status.ShouldBe(HealthStatus.Healthy);
        result.Description.ShouldBe("NATS connection is open and JetStream is responding.");
    }

    [Test]
    public async Task HealthCheck_ShouldReturnUnhealthy_WhenConnectionNotOpen()
    {
        // Arrange
        _connection.ConnectionState.Returns(NatsConnectionState.Closed);

        // Act
        var result = await _healthCheck.CheckHealthAsync(new HealthCheckContext());

        // Assert
        result.Status.ShouldBe(HealthStatus.Unhealthy);
        result.Description.ShouldStartWith("NATS connection state is Closed");
    }

    [Test]
    public async Task HealthCheck_ShouldReturnDegraded_WhenJetStreamUnavailable()
    {
        // Arrange
        _connection.ConnectionState.Returns(NatsConnectionState.Open);
        _jsContext.GetAccountInfoAsync(Arg.Any<CancellationToken>())
            .Returns(ValueTask.FromException<AccountInfoResponse>(new Exception("JS Error")));

        // Act
        var result = await _healthCheck.CheckHealthAsync(new HealthCheckContext());

        // Assert
        result.Status.ShouldBe(HealthStatus.Degraded);
        result.Description.ShouldStartWith("NATS connection is open but JetStream is unavailable");
        result.Description.ShouldContain("JS Error");
    }
}

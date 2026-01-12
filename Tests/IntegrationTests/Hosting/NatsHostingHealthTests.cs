using FlySwattr.NATS.Hosting.Health;
using IntegrationTests.Infrastructure;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;
using NATS.Client.JetStream;
using Shouldly;
using TUnit.Core;

namespace IntegrationTests.Hosting;

[Property("nTag", "Hosting")]
public class NatsHostingHealthTests
{
    [Test]
    public async Task HealthCheck_ShouldReflectConnectionState()
    {
        // Arrange
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts { Url = fixture.ConnectionString };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();
        var js = new NatsJSContext(conn);

        var healthCheck = new NatsHealthCheck(conn, js);

        // Act & Assert: Initial state (Healthy)
        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext());
        result.Status.ShouldBe(HealthStatus.Healthy);

        // Act: Stop NATS
        await fixture.Container.StopAsync();
        
        // Wait for NATS client to detect disconnect (ConnectionState changed)
        await AwaitConditionAsync(() => conn.ConnectionState != NatsConnectionState.Open, TimeSpan.FromSeconds(10));

        // Assert: Unhealthy
        result = await healthCheck.CheckHealthAsync(new HealthCheckContext());
        result.Status.ShouldBe(HealthStatus.Unhealthy);

        // Act: Start NATS
        await fixture.Container.StartAsync();
        
        // Wait for reconnect
        await AwaitConditionAsync(() => conn.ConnectionState == NatsConnectionState.Open, TimeSpan.FromSeconds(20));

        // Assert: Healthy again
        result = await healthCheck.CheckHealthAsync(new HealthCheckContext());
        result.Status.ShouldBe(HealthStatus.Healthy);
    }

    [Test]
    public async Task StartupCheck_ShouldPass_WhenConnectionWorks()
    {
        // Arrange
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts { Url = fixture.ConnectionString };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();

        var startupCheck = new NatsStartupCheck(conn, NullLogger<NatsStartupCheck>.Instance);

        // Act & Assert
        await Should.NotThrowAsync(async () => await startupCheck.StartAsync(CancellationToken.None));
    }

    [Test]
    public async Task StartupCheck_ShouldThrow_WhenConnectionFails()
    {
        // Arrange
        // Use an invalid port to force failure
        var opts = new NatsOpts { Url = "nats://localhost:12345", ConnectTimeout = TimeSpan.FromSeconds(1) };
        await using var conn = new NatsConnection(opts);
        // Note: Don't call ConnectAsync here because StartupCheck will attempt to use the connection.
        // Actually NatsConnection constructor doesn't connect.

        var startupCheck = new NatsStartupCheck(conn, NullLogger<NatsStartupCheck>.Instance);

        // Act & Assert
        await Should.ThrowAsync<Exception>(async () => await startupCheck.StartAsync(CancellationToken.None));
    }

    private async Task AwaitConditionAsync(Func<bool> condition, TimeSpan timeout)
    {
        var start = DateTime.UtcNow;
        while (!condition() && DateTime.UtcNow - start < timeout)
        {
            await Task.Delay(100);
        }
    }
}

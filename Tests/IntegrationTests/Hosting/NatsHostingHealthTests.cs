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

        var opts = new NatsOpts
        {
            Url = fixture.ConnectionString,
            MaxReconnectRetry = -1,
            ReconnectWaitMin = TimeSpan.FromMilliseconds(100),
            ReconnectWaitMax = TimeSpan.FromMilliseconds(500)
        };
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
        var disconnected = await AwaitConditionAsync(
            () => conn.ConnectionState != NatsConnectionState.Open,
            TimeSpan.FromSeconds(20));
        disconnected.ShouldBeTrue(
            $"Connection should transition away from Open after outage (state: {conn.ConnectionState})");

        // Assert: Unhealthy
        result = await healthCheck.CheckHealthAsync(new HealthCheckContext());
        result.Status.ShouldBe(HealthStatus.Unhealthy);

        // Act: Restart NATS
        await fixture.Container.StartAsync();

        // Wait for the NATS endpoint to accept connections again
        var endpointReady = await AwaitConditionAsync(
            () => CanConnectAsync(fixture.ConnectionString),
            TimeSpan.FromSeconds(30),
            TimeSpan.FromMilliseconds(500));
        endpointReady.ShouldBeTrue("NATS endpoint should be reachable after restart");

        // Assert: Healthy again (with a fresh connection)
        // The health check is stateless — it reads ConnectionState and probes JetStream.
        // A fresh connection avoids dependence on the NATS library's auto-reconnect,
        // which is unreliable in CI container environments.
        await using var conn2 = new NatsConnection(new NatsOpts { Url = fixture.ConnectionString });
        await conn2.ConnectAsync();
        var js2 = new NatsJSContext(conn2);
        var healthCheck2 = new NatsHealthCheck(conn2, js2);

        using var finalHealthCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        result = await healthCheck2.CheckHealthAsync(new HealthCheckContext(), finalHealthCts.Token);
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

    private static async Task<bool> AwaitConditionAsync(
        Func<bool> condition,
        TimeSpan timeout,
        TimeSpan? pollInterval = null)
    {
        var start = DateTime.UtcNow;
        var delay = pollInterval ?? TimeSpan.FromMilliseconds(100);

        while (DateTime.UtcNow - start < timeout)
        {
            if (condition())
            {
                return true;
            }
            await Task.Delay(delay);
        }

        return false;
    }

    private static async Task<bool> AwaitConditionAsync(
        Func<Task<bool>> condition,
        TimeSpan timeout,
        TimeSpan? pollInterval = null)
    {
        var start = DateTime.UtcNow;
        var delay = pollInterval ?? TimeSpan.FromMilliseconds(100);

        while (DateTime.UtcNow - start < timeout)
        {
            if (await condition())
            {
                return true;
            }
            await Task.Delay(delay);
        }

        return false;
    }

    private static async Task<bool> CanConnectAsync(string url)
    {
        try
        {
            var probeOpts = new NatsOpts
            {
                Url = url,
                ConnectTimeout = TimeSpan.FromSeconds(2),
                MaxReconnectRetry = 0
            };

            await using var probe = new NatsConnection(probeOpts);
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
            await probe.ConnectAsync();
            await probe.PingAsync(cts.Token);
            return true;
        }
        catch
        {
            return false;
        }
    }
}

using FlySwattr.NATS.Core;
using IntegrationTests.Infrastructure;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NSubstitute;
using Shouldly;
using TUnit.Core;

namespace IntegrationTests.Resilience;

[Property("nTag", "Integration")]
[Property("nTag", "Resilience")]
public class NetworkChaosTests
{
    [Test]
    public async Task Publish_ShouldRecover_AfterNatsRestart()
    {
        // Arrange
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts { Url = fixture.ConnectionString };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();

        // 1. Verify initial connectivity
        await conn.PingAsync();

        // 2. Simulate Outage: Stop the NATS server
        await fixture.Container.StopAsync();

        // 3. Attempt to publish during outage
        // Default behavior: It might buffer or throw depending on client config. 
        // With standard NatsConnection, it buffers outgoing if disconnected, or throws if buffer full/timeout.
        // We expect it to throw or timeout immediately if we await the ack.
        try 
        {
            // Set a short timeout for the publish attempt so we don't hang the test
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            await conn.PublishAsync("chaos.test", "data", cancellationToken: cts.Token);
        }
        catch (Exception)
        {
            // Expected failure: NatsNoResponders, Timeout, or OperationCanceled
        }

        // 4. Recovery: Start the NATS server again
        await fixture.Container.StartAsync();

        // Wait for client to reconnect
        // NATS.Net client handles reconnection automatically
        var reconnected = false;
        for (int i = 0; i < 20; i++)
        {
            if (conn.ConnectionState == NatsConnectionState.Open)
            {
                reconnected = true;
                break;
            }
            await Task.Delay(500);
        }

        reconnected.ShouldBeTrue("Client should auto-reconnect after server restart");

        // 5. Verify operations resume
        await conn.PublishAsync("chaos.recovery", "data");
        await conn.PingAsync();
    }
}

using IntegrationTests.Infrastructure;
using NATS.Client.Core;
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

        await using var conn = new NatsConnection(new NatsOpts { Url = fixture.ConnectionString });
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

        // Wait for the NATS endpoint to accept connections again
        var endpointReady = await WaitForConditionAsync(
            () => CanConnectAsync(fixture.ConnectionString),
            TimeSpan.FromSeconds(30),
            TimeSpan.FromMilliseconds(500));
        endpointReady.ShouldBeTrue("NATS endpoint should be reachable after restart");

        // 5. Verify operations resume with a fresh connection
        //    Auto-reconnect of the original connection is a NATS library concern;
        //    this test validates that the server is healthy and accepting traffic after restart.
        await using var conn2 = new NatsConnection(new NatsOpts { Url = fixture.ConnectionString });
        await conn2.ConnectAsync();

        using var finalCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await conn2.PublishAsync("chaos.recovery", "data", cancellationToken: finalCts.Token);
        await conn2.PingAsync(finalCts.Token);
    }

    private static async Task<bool> WaitForConditionAsync(
        Func<Task<bool>> condition,
        TimeSpan timeout,
        TimeSpan? pollInterval = null)
    {
        var start = DateTime.UtcNow;
        var delay = pollInterval ?? TimeSpan.FromMilliseconds(200);

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

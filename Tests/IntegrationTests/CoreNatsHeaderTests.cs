using FlySwattr.NATS.Core;
using IntegrationTests.Infrastructure;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;
using NATS.Client.Serializers.Json;
using Shouldly;
using TUnit.Core;

namespace IntegrationTests;

[Property("nTag", "Abstractions")]
public class CoreNatsHeaderTests
{
    public record DataMessage(string Id);

    [Test]
    public async Task Verify_Header_Propagation()
    {
        // 1. Setup
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();
        
        var opts = new NatsOpts
        {
            Url = fixture.ConnectionString,
            Name = "HeaderTester",
            SerializerRegistry = NatsJsonSerializerRegistry.Default
        };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();
        
        await using var bus = new NatsMessageBus(conn, NullLogger<NatsMessageBus>.Instance);

        var receivedHeaders = new Dictionary<string, string>();
        var tcs = new TaskCompletionSource<bool>();

        // 2. Subscribe using the Abstraction (IMessageBus -> NatsMessageBus)
        // We use a wildcard subject or specific one
        await bus.SubscribeAsync<DataMessage>("data.sync", async ctx => 
        {
            foreach (var kvp in ctx.Headers.Headers)
            {
                receivedHeaders[kvp.Key] = kvp.Value.ToString();
            }
            tcs.SetResult(true);
            await Task.CompletedTask;
        });

        await Task.Delay(500);

        // 3. Publish with Headers
        // NatsMessageBus implementation of PublishAsync currently doesn't support headers in the interface.
        // So we use the underlying connection to publish with headers to verify that the CONSUMER abstraction
        // correctly exposes them.
        
        var headers = new NatsHeaders
        {
            { "Trace-Id", "abc-123" },
            { "Source", "IntegrationTest" }
        };

        // Bypass abstraction for publishing to inject headers
        await conn.PublishAsync("data.sync", new DataMessage("M1"), headers: headers);

        // 4. Wait
        await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // 5. Assert
        receivedHeaders["Trace-Id"].ShouldBe("abc-123");
        receivedHeaders["Source"].ShouldBe("IntegrationTest");
    }
}

using System.Collections.Concurrent;
using FlySwattr.NATS.Core;
using IntegrationTests.Infrastructure;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;
using NATS.Client.Serializers.Json;
using Shouldly;
using TUnit.Core;

namespace IntegrationTests.Core;

[Property("nTag", "Core")]
public class CoreNatsWildcardTests
{
    public record LogEvent(string Source, string Msg);

    [Test]
    public async Task Verify_Wildcard_Matching_Rules()
    {
        // 1. Setup
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();
        var connectionString = fixture.ConnectionString;

        // 2. Setup Bus
        var opts = new NatsOpts { Url = connectionString, Name = "WildcardTester", SerializerRegistry = NatsJsonSerializerRegistry.Default };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();
        await using var bus = new NatsMessageBus(conn, NullLogger<NatsMessageBus>.Instance);

        var receivedSingle = new ConcurrentBag<string>();
        var receivedMulti = new ConcurrentBag<string>();

        // 3. Subscribe to "service.*" (Single Token)
        // Should match "service.auth", "service.orders"
        // Should NOT match "service.auth.login"
        await bus.SubscribeAsync<LogEvent>("service.*", async ctx => 
        {
            receivedSingle.Add(ctx.Subject);
            await Task.CompletedTask;
        });

        // 4. Subscribe to "logs.>" (Multi Token)
        // Should match "logs.app", "logs.app.error", "logs.a.b.c"
        await bus.SubscribeAsync<LogEvent>("logs.>", async ctx => 
        {
            receivedMulti.Add(ctx.Subject);
            await Task.CompletedTask;
        });

        await Task.Delay(500);

        // 5. Publish matching and non-matching messages
        await bus.PublishAsync("service.auth", new LogEvent("auth", "ok"));         // Match Single
        await bus.PublishAsync("service.orders", new LogEvent("orders", "ok"));     // Match Single
        await bus.PublishAsync("service.auth.login", new LogEvent("auth", "fail")); // No Match Single
        
        await bus.PublishAsync("logs.app", new LogEvent("app", "info"));            // Match Multi
        await bus.PublishAsync("logs.app.error", new LogEvent("app", "err"));       // Match Multi
        await bus.PublishAsync("logs.sys.net.io", new LogEvent("sys", "io"));       // Match Multi

        await Task.Delay(500);

        // 6. Verify Single Token Matches
        await Assert.That(receivedSingle.Count).IsEqualTo(2);
        await Assert.That(receivedSingle).Contains("service.auth");
        await Assert.That(receivedSingle).Contains("service.orders");
        await Assert.That(receivedSingle).DoesNotContain("service.auth.login");

        // 7. Verify Multi Token Matches
        await Assert.That(receivedMulti.Count).IsEqualTo(3);
        await Assert.That(receivedMulti).Contains("logs.app");
        await Assert.That(receivedMulti).Contains("logs.app.error");
        await Assert.That(receivedMulti).Contains("logs.sys.net.io");
    }
}

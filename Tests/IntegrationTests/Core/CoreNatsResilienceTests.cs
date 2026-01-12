using FlySwattr.NATS.Core;
using IntegrationTests.Infrastructure;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;
using NATS.Client.Serializers.Json;
using Shouldly;
using TUnit.Core;

namespace IntegrationTests.Core;

[Property("nTag", "Core")]
public class CoreNatsResilienceTests
{
    public record Ping(int Id);

    [Test]
    public async Task Verify_Connection_State()
    {
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();
        
        var opts = new NatsOpts { Url = fixture.ConnectionString };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();
        
        await Assert.That(conn.ConnectionState).IsEqualTo(NatsConnectionState.Open);
        
        await conn.PingAsync(); // Should succeed
    }
    
    [Test]
    public async Task Verify_Handler_Exception_Resilience()
    {
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();
        
        var opts = new NatsOpts { Url = fixture.ConnectionString, SerializerRegistry = NatsJsonSerializerRegistry.Default };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();
        
        await using var bus = new NatsMessageBus(conn, NullLogger<NatsMessageBus>.Instance);
        
        int calls = 0;
        await bus.SubscribeAsync<Ping>("resilience.test", async ctx => 
        {
            Interlocked.Increment(ref calls);
            if (ctx.Message.Id == 1) throw new Exception("Boom");
            await Task.CompletedTask;
        });

        // First message causes handler crash (but should not crash subscription)
        await bus.PublishAsync("resilience.test", new Ping(1)); 
        await Task.Delay(500);
        
        // Second message should still be processed
        await bus.PublishAsync("resilience.test", new Ping(2));
        await Task.Delay(500);
        
        await Assert.That(calls).IsEqualTo(2);
    }
}

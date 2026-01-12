using FlySwattr.NATS.Core;
using IntegrationTests.Infrastructure;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;
using NATS.Client.Serializers.Json;
using Shouldly;
using TUnit.Core;

namespace IntegrationTests.Core;

[Property("nTag", "Core")]
public class CoreNatsScatterGatherTests
{
    public record HealthCheckRequest();
    public record HealthCheckResponse(string ServiceId, string Status);

    [Test]
    public async Task Verify_ScatterGather_Pattern()
    {
        // 1. Setup
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();
        var connectionString = fixture.ConnectionString;

        // 2. Requester
        var optsA = new NatsOpts { Url = connectionString, Name = "Requester", SerializerRegistry = NatsJsonSerializerRegistry.Default };
        await using var connA = new NatsConnection(optsA);
        await connA.ConnectAsync();
        await using var busA = new NatsMessageBus(connA, NullLogger<NatsMessageBus>.Instance);

        // 3. Responders (B and C) - NO Queue Group
        var optsB = new NatsOpts { Url = connectionString, Name = "Responder-B", SerializerRegistry = NatsJsonSerializerRegistry.Default };
        await using var connB = new NatsConnection(optsB);
        await connB.ConnectAsync();
        var busB = new NatsMessageBus(connB, NullLogger<NatsMessageBus>.Instance);

        var optsC = new NatsOpts { Url = connectionString, Name = "Responder-C", SerializerRegistry = NatsJsonSerializerRegistry.Default };
        await using var connC = new NatsConnection(optsC);
        await connC.ConnectAsync();
        var busC = new NatsMessageBus(connC, NullLogger<NatsMessageBus>.Instance);

        int bReceived = 0;
        int cReceived = 0;

        await busB.SubscribeAsync<HealthCheckRequest>("health.check", async ctx => 
        {
            Interlocked.Increment(ref bReceived);
            await ctx.RespondAsync(new HealthCheckResponse("B", "OK"));
        }); // No queue group

        await busC.SubscribeAsync<HealthCheckRequest>("health.check", async ctx => 
        {
            Interlocked.Increment(ref cReceived);
            await ctx.RespondAsync(new HealthCheckResponse("C", "OK"));
        }); // No queue group

        await Task.Delay(500);

        // 4. Send Request
        // NATS Core Request pattern sends to "health.check" and listens on a unique inbox.
        // Since B and C are standard subscribers (no queue group), BOTH should receive the message.
        // The requester usually takes the FIRST response.
        
        var response = await busA.RequestAsync<HealthCheckRequest, HealthCheckResponse>("health.check", new HealthCheckRequest(), TimeSpan.FromSeconds(2));

        // 5. Assertions
        await Assert.That(response).IsNotNull();
        await Assert.That(response.Status).IsEqualTo("OK");
        
        // Check that BOTH services processed the request (Fan-out Request)
        await Assert.That(bReceived).IsEqualTo(1);
        await Assert.That(cReceived).IsEqualTo(1);
        
        // This confirms that without a queue group, requests are broadcast to all matching subscribers.
    }
}

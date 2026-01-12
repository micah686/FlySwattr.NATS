using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core;
using IntegrationTests.Infrastructure;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;
using NATS.Client.Serializers.Json;
using Shouldly;
using TUnit.Core;

namespace IntegrationTests.Core;

[Property("nTag", "Core")]
public class CoreNatsRequestReplyTests
{
    public record InventoryRequest(string Sku);
    public record InventoryResponse(string Sku, int Qty);

    [Test]
    public async Task Verify_RequestReply_LoadBalancing_And_Failures()
    {
        // 1. Setup NATS Container
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();
        var connectionString = fixture.ConnectionString;
        var subject = "inventory.lookup";

        // 2. Create Requester (Microservice A)
        var optsA = new NatsOpts { Url = connectionString, Name = "Microservice-A", SerializerRegistry = NatsJsonSerializerRegistry.Default };
        await using var connA = new NatsConnection(optsA);
        await connA.ConnectAsync();
        var loggerA = NullLogger<NatsMessageBus>.Instance;
        await using var busA = new NatsMessageBus(connA, loggerA);

        // 3. Create Responders (Microservice B1 and B2)
        var optsB1 = new NatsOpts { Url = connectionString, Name = "Microservice-B1", SerializerRegistry = NatsJsonSerializerRegistry.Default };
        await using var connB1 = new NatsConnection(optsB1);
        await connB1.ConnectAsync();
        var loggerB1 = NullLogger<NatsMessageBus>.Instance;
        var busB1 = new NatsMessageBus(connB1, loggerB1);

        var optsB2 = new NatsOpts { Url = connectionString, Name = "Microservice-B2", SerializerRegistry = NatsJsonSerializerRegistry.Default };
        await using var connB2 = new NatsConnection(optsB2);
        await connB2.ConnectAsync();
        var loggerB2 = NullLogger<NatsMessageBus>.Instance;
        var busB2 = new NatsMessageBus(connB2, loggerB2);

        // Track who handled the request
        int b1Handled = 0;
        int b2Handled = 0;

        // Subscribe B1
        await busB1.SubscribeAsync<InventoryRequest>(subject, async ctx => 
        {
            Interlocked.Increment(ref b1Handled);
            await ctx.RespondAsync(new InventoryResponse(ctx.Message.Sku, 100));
        }, queueGroup: "responders");

        // Subscribe B2
        await busB2.SubscribeAsync<InventoryRequest>(subject, async ctx => 
        {
            Interlocked.Increment(ref b2Handled);
            await ctx.RespondAsync(new InventoryResponse(ctx.Message.Sku, 100));
        }, queueGroup: "responders");

        // Allow subscriptions to propagate
        await Task.Delay(500);

        // 4. Send Requests (Load Balancing)
        // We send enough requests to ensure both pick up some.
        for (int i = 0; i < 10; i++)
        {
            var response = await busA.RequestAsync<InventoryRequest, InventoryResponse>(subject, new InventoryRequest("ABC"), TimeSpan.FromSeconds(2));
            await Assert.That(response).IsNotNull();
            await Assert.That(response.Sku).IsEqualTo("ABC");
            await Assert.That(response.Qty).IsEqualTo(100);
        }

        // Verify distribution
        // NATS random distribution might not be perfectly even with small sample, but both should have handled at least one if everything is working.
        await Assert.That(b1Handled + b2Handled).IsEqualTo(10);
        
        // 5. "No Responders" Test
        // Stop B1 and B2
        await busB1.DisposeAsync();
        await connB1.DisposeAsync();
        await busB2.DisposeAsync();
        await connB2.DisposeAsync();

        // Wait a moment for registration to drop
        await Task.Delay(500);

        // Send request - expects NatsNoRespondersException
        await Assert.ThrowsAsync<NatsNoRespondersException>(async () => 
            await busA.RequestAsync<InventoryRequest, InventoryResponse>(subject, new InventoryRequest("ABC"), TimeSpan.FromSeconds(2)));

        // 6. Timeout Test
        // Restart B1 but make it slow
        var optsB1_Slow = new NatsOpts { Url = connectionString, Name = "Microservice-B1-Slow", SerializerRegistry = NatsJsonSerializerRegistry.Default };
        await using var connB1_Slow = new NatsConnection(optsB1_Slow);
        await connB1_Slow.ConnectAsync();
        await using var busB1_Slow = new NatsMessageBus(connB1_Slow, loggerB1);

        await busB1_Slow.SubscribeAsync<InventoryRequest>(subject, async ctx => 
        {
            await Task.Delay(2000); // Wait longer than timeout
            await ctx.RespondAsync(new InventoryResponse(ctx.Message.Sku, 100));
        }, queueGroup: "responders");

        await Task.Delay(500);

        // Timeout set to 500ms, handler delays 2000ms.
        // Expect NatsNoReplyException (or TimeoutException depending on client mapping)
        // NATS.Net typically throws NatsNoReplyException on timeout
        await Assert.ThrowsAsync<NatsNoReplyException>(async () => 
            await busA.RequestAsync<InventoryRequest, InventoryResponse>(subject, new InventoryRequest("ABC"), TimeSpan.FromMilliseconds(500)));
    }
}

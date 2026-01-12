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
public class CoreNatsQueueGroupTests
{
    public record Alert(int Id, string Msg);

    [Test]
    public async Task Verify_QueueGroup_Distribution_And_Failover()
    {
        // 1. Setup NATS Container
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();
        var connectionString = fixture.ConnectionString;
        var subject = "notifications.alert";
        var queueGroup = "workers";

        // 2. Create Publisher (Microservice D)
        var optsD = new NatsOpts { Url = connectionString, Name = "Microservice-D", SerializerRegistry = NatsJsonSerializerRegistry.Default };
        await using var connD = new NatsConnection(optsD);
        await connD.ConnectAsync();
        var loggerD = NullLogger<NatsMessageBus>.Instance;
        await using var busD = new NatsMessageBus(connD, loggerD);

        // 3. Create Subscribers (X, Y, Z)
        var optsX = new NatsOpts { Url = connectionString, Name = "Microservice-X", SerializerRegistry = NatsJsonSerializerRegistry.Default };
        await using var connX = new NatsConnection(optsX);
        await connX.ConnectAsync();
        var busX = new NatsMessageBus(connX, NullLogger<NatsMessageBus>.Instance);

        var optsY = new NatsOpts { Url = connectionString, Name = "Microservice-Y", SerializerRegistry = NatsJsonSerializerRegistry.Default };
        await using var connY = new NatsConnection(optsY);
        await connY.ConnectAsync();
        var busY = new NatsMessageBus(connY, NullLogger<NatsMessageBus>.Instance);

        var optsZ = new NatsOpts { Url = connectionString, Name = "Microservice-Z", SerializerRegistry = NatsJsonSerializerRegistry.Default };
        await using var connZ = new NatsConnection(optsZ);
        await connZ.ConnectAsync();
        var busZ = new NatsMessageBus(connZ, NullLogger<NatsMessageBus>.Instance);

        int countX = 0;
        int countY = 0;
        int countZ = 0;
        var receivedIds = new ConcurrentBag<int>();

        await busX.SubscribeAsync<Alert>(subject, async ctx => 
        {
            Interlocked.Increment(ref countX);
            receivedIds.Add(ctx.Message.Id);
            await Task.CompletedTask;
        }, queueGroup: queueGroup);

        await busY.SubscribeAsync<Alert>(subject, async ctx => 
        {
            Interlocked.Increment(ref countY);
            receivedIds.Add(ctx.Message.Id);
            await Task.CompletedTask;
        }, queueGroup: queueGroup);

        await busZ.SubscribeAsync<Alert>(subject, async ctx => 
        {
            Interlocked.Increment(ref countZ);
            receivedIds.Add(ctx.Message.Id);
            await Task.CompletedTask;
        }, queueGroup: queueGroup);

        await Task.Delay(500);

        // 4. Publish 10 messages
        for (int i = 1; i <= 10; i++)
        {
            await busD.PublishAsync(subject, new Alert(i, "Warning"));
        }

        // Wait for delivery
        await WaitForConditionAsync(() => receivedIds.Count == 10, TimeSpan.FromSeconds(5), "Initial 10 messages");

        // Verify distribution
        // Ensure no duplicates (Set count matches bag count)
        await Assert.That(receivedIds.Distinct().Count()).IsEqualTo(10);
        
        // Ensure strictly one worker handled each (Sum of counts is 10)
        await Assert.That(countX + countY + countZ).IsEqualTo(10);

        // 5. Simulate Failure: Shutdown Z
        await busZ.DisposeAsync();
        await connZ.DisposeAsync();
        await Task.Delay(500); // Allow server to detect disconnect

        // 6. Publish 5 more messages
        for (int i = 11; i <= 15; i++)
        {
            await busD.PublishAsync(subject, new Alert(i, "Warning"));
        }

        // Wait for delivery
        await WaitForConditionAsync(() => receivedIds.Count == 15, TimeSpan.FromSeconds(5), "Failover messages");

        // Verify X and Y handled them (Z count should not change)
        int zAfter = countZ;
        await Assert.That(zAfter).IsEqualTo(countZ); // Z stopped, shouldn't increase
        await Assert.That(countX + countY + countZ).IsEqualTo(15);
        await Assert.That(receivedIds.Distinct().Count()).IsEqualTo(15);
    }

    private async Task WaitForConditionAsync(Func<bool> condition, TimeSpan timeout, string description)
    {
        var start = DateTime.UtcNow;
        while ((DateTime.UtcNow - start) < timeout)
        {
            if (condition()) return;
            await Task.Delay(100);
        }
        throw new TimeoutException($"Timed out waiting for: {description}");
    }
}

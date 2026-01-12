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
public class CoreNatsLoadTests
{
    public record Metric(int Id, string Payload);

    [Test]
    public async Task Verify_HighLoad_Throughput()
    {
        // 1. Setup NATS Container
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();
        var connectionString = fixture.ConnectionString;
        var subject = "metrics.update";
        const int MessageCount = 500; // Reduced from 10_000 for faster CI integration test

        // 2. Create Publisher
        var optsPub = new NatsOpts { Url = connectionString, Name = "Pub", SerializerRegistry = NatsJsonSerializerRegistry.Default };
        await using var connPub = new NatsConnection(optsPub);
        await connPub.ConnectAsync();
        var busPub = new NatsMessageBus(connPub, NullLogger<NatsMessageBus>.Instance);

        // 3. Create Subscribers
        var optsSub1 = new NatsOpts { Url = connectionString, Name = "Sub1", SerializerRegistry = NatsJsonSerializerRegistry.Default };
        await using var connSub1 = new NatsConnection(optsSub1);
        await connSub1.ConnectAsync();
        var busSub1 = new NatsMessageBus(connSub1, NullLogger<NatsMessageBus>.Instance);

        var optsSub2 = new NatsOpts { Url = connectionString, Name = "Sub2", SerializerRegistry = NatsJsonSerializerRegistry.Default };
        await using var connSub2 = new NatsConnection(optsSub2);
        await connSub2.ConnectAsync();
        var busSub2 = new NatsMessageBus(connSub2, NullLogger<NatsMessageBus>.Instance);

        // Capture data
        var received1 = new ConcurrentQueue<int>();
        var received2 = new ConcurrentQueue<int>();
        
        await busSub1.SubscribeAsync<Metric>(subject, async ctx => 
        {
            received1.Enqueue(ctx.Message.Id);
            await Task.CompletedTask;
        });

        await busSub2.SubscribeAsync<Metric>(subject, async ctx => 
        {
            received2.Enqueue(ctx.Message.Id);
            await Task.CompletedTask;
        });

        await Task.Delay(1000); // Warmup

        // 4. Publish High Volume
        var start = DateTime.UtcNow;
        
        // We publish sequentially to ensure ordering at the source (if we did Parallel.For, source order is undefined)
        for (int i = 0; i < MessageCount; i++)
        {
            // We await each publish to guarantee order on the wire
            await busPub.PublishAsync(subject, new Metric(i, "data"));
        }
        
        var publishDuration = DateTime.UtcNow - start;
        Console.WriteLine($"Published {MessageCount} messages in {publishDuration.TotalMilliseconds}ms");

        // 5. Wait for delivery
        await WaitForConditionAsync(() => received1.Count == MessageCount && received2.Count == MessageCount, TimeSpan.FromSeconds(30), "High load delivery");

        // 6. Assertions
        await Assert.That(received1.Count).IsEqualTo(MessageCount);
        await Assert.That(received2.Count).IsEqualTo(MessageCount);

        // Verify Ordering
        int expectedId = 0;
        foreach (var id in received1)
        {
            if (id != expectedId)
            {
                await Assert.That(id).IsEqualTo(expectedId); // Fail
            }
            expectedId++;
        }
        
        expectedId = 0;
        foreach (var id in received2)
        {
            if (id != expectedId)
            {
                 await Assert.That(id).IsEqualTo(expectedId); // Fail
            }
            expectedId++;
        }
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

using FlySwattr.NATS.Core;
using FlySwattr.NATS.Core.Serializers;
using IntegrationTests.Infrastructure;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Shouldly;
using TUnit.Core;

namespace IntegrationTests.Chaos;

/// <summary>
/// Integration tests for connection resilience during NATS outages.
/// Uses stop/start to simulate network failures.
/// </summary>
[Property("nTag", "Integration")]
[Property("nTag", "Chaos")]
public class ConnectionResilienceTests
{
    [Test]
    public async Task Publisher_ShouldRecover_AfterNatsRestart()
    {
        // Arrange
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts
        {
            Url = fixture.ConnectionString,
            SerializerRegistry = HybridSerializerRegistry.Default
        };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();

        var js = new NatsJSContext(conn);
        var streamName = $"CHAOS_PUB_{Guid.NewGuid():N}";
        var subject = "chaos.pub.test";
        await js.CreateStreamAsync(new StreamConfig(streamName, [subject])
        {
            Storage = StreamConfigStorage.Memory
        });

        var serializer = new HybridNatsSerializer();
        await using var bus = new NatsJetStreamBus(js, NullLogger<NatsJetStreamBus>.Instance, serializer);

        // Act 1: Publish before outage
        await bus.PublishAsync(subject, "before-outage", "msg-1", cancellationToken: CancellationToken.None);

        // Act 2: Stop NATS
        await fixture.Container.StopAsync();

        // Act 3: Attempt publish during outage (should fail)
        var publishDuringOutage = async () =>
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
            await bus.PublishAsync(subject, "during-outage", "msg-2", cancellationToken: cts.Token);
        };
        await Should.ThrowAsync<Exception>(publishDuringOutage);

        // Act 4: Restart NATS
        await fixture.Container.StartAsync();
        await WaitForNatsAsync(fixture.ConnectionString);

        // Act 5: Publish after recovery with a fresh connection
        // Testcontainers can assign a new mapped host port after a stop/start in CI,
        // so build fresh connection options from the current endpoint.
        await using var conn2 = new NatsConnection(new NatsOpts
        {
            Url = fixture.ConnectionString,
            SerializerRegistry = HybridSerializerRegistry.Default
        });
        await conn2.ConnectAsync();
        var js2 = new NatsJSContext(conn2);
        await using var bus2 = new NatsJetStreamBus(js2, NullLogger<NatsJetStreamBus>.Instance, serializer);

        // Stream was in-memory, so recreate it
        await js2.CreateStreamAsync(new StreamConfig(streamName, [subject])
        {
            Storage = StreamConfigStorage.Memory
        });

        await bus2.PublishAsync(subject, "after-recovery", "msg-3", cancellationToken: CancellationToken.None);

        // Assert: Verify the message arrived
        var consumer = await js2.CreateOrUpdateConsumerAsync(streamName, new ConsumerConfig("verify-consumer")
        {
            DurableName = "verify-consumer",
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            DeliverPolicy = ConsumerConfigDeliverPolicy.All
        });

        using var fetchCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var messages = new List<string>();
        await foreach (var msg in consumer.FetchAsync<string>(new NatsJSFetchOpts { MaxMsgs = 10 },
                           cancellationToken: fetchCts.Token))
        {
            if (msg.Data != null)
                messages.Add(msg.Data);
            await msg.AckAsync(cancellationToken: fetchCts.Token);
        }

        messages.ShouldContain("after-recovery");
    }

    [Test]
    public async Task Consumer_ShouldResumeProcessing_AfterNatsRestart()
    {
        // Arrange
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts
        {
            Url = fixture.ConnectionString,
            SerializerRegistry = HybridSerializerRegistry.Default
        };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();

        var js = new NatsJSContext(conn);
        var streamName = $"CHAOS_CON_{Guid.NewGuid():N}";
        var subject = "chaos.con.test";
        await js.CreateStreamAsync(new StreamConfig(streamName, [subject])
        {
            Storage = StreamConfigStorage.File // File storage survives restart
        });

        // Publish a message before consumer starts
        var ack = await js.PublishAsync(subject, "test-data-1", opts: new NatsJSPubOpts { MsgId = "msg-1" });
        ack.EnsureSuccess();

        // Create durable consumer
        var consumer = await js.CreateOrUpdateConsumerAsync(streamName, new ConsumerConfig("chaos-consumer")
        {
            DurableName = "chaos-consumer",
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            FilterSubject = subject
        });

        // Fetch and ack the message
        using var cts1 = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var received = false;
        await foreach (var msg in consumer.FetchAsync<string>(new NatsJSFetchOpts { MaxMsgs = 1 },
                           cancellationToken: cts1.Token))
        {
            msg.Data.ShouldBe("test-data-1");
            await msg.AckAsync(cancellationToken: cts1.Token);
            received = true;
        }
        received.ShouldBeTrue();

        // Stop and restart NATS (file storage preserves data)
        await fixture.Container.StopAsync();
        await fixture.Container.StartAsync();
        await WaitForNatsAsync(fixture.ConnectionString);

        // Reconnect and verify consumer can resume
        // Testcontainers can assign a new mapped host port after a stop/start in CI,
        // so build fresh connection options from the current endpoint.
        await using var conn2 = new NatsConnection(new NatsOpts
        {
            Url = fixture.ConnectionString,
            SerializerRegistry = HybridSerializerRegistry.Default
        });
        await conn2.ConnectAsync();
        var js2 = new NatsJSContext(conn2);

        // Publish new message after restart
        var ack2 = await js2.PublishAsync(subject, "test-data-2", opts: new NatsJSPubOpts { MsgId = "msg-2" });
        ack2.EnsureSuccess();

        // Fetch with the same durable consumer — should get message 2 (message 1 was acked)
        var consumer2 = await js2.GetConsumerAsync(streamName, "chaos-consumer");
        using var cts2 = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var messagesAfterRestart = new List<string>();
        await foreach (var msg in consumer2.FetchAsync<string>(new NatsJSFetchOpts { MaxMsgs = 10 },
                           cancellationToken: cts2.Token))
        {
            if (msg.Data != null)
                messagesAfterRestart.Add(msg.Data);
            await msg.AckAsync(cancellationToken: cts2.Token);
        }

        messagesAfterRestart.ShouldContain("test-data-2");
        messagesAfterRestart.ShouldNotContain("test-data-1"); // Already acked
    }

    private static async Task WaitForNatsAsync(string url, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(30));
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                await using var probe = new NatsConnection(new NatsOpts
                {
                    Url = url,
                    ConnectTimeout = TimeSpan.FromSeconds(2),
                    MaxReconnectRetry = 0
                });
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
                await probe.ConnectAsync();
                await probe.PingAsync(cts.Token);
                return;
            }
            catch
            {
                await Task.Delay(500);
            }
        }

        throw new TimeoutException($"NATS did not become available at {url} within the timeout.");
    }
}

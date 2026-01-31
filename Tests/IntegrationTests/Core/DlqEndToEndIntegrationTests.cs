using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core;
using FlySwattr.NATS.Core.Serializers;
using FlySwattr.NATS.Core.Services;
using FlySwattr.NATS.Core.Stores;
using FlySwattr.NATS.Hosting.Services;
using IntegrationTests.Infrastructure;
using MemoryPack;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.KeyValueStore;
using Shouldly;
using TUnit.Core;

namespace IntegrationTests.Core;

/// <summary>
/// Integration tests for the complete DLQ flow:
/// Publish → Consume → Failure → DLQ Storage → Replay → Success
/// </summary>
[Property("nTag", "Integration")]
public partial class DlqEndToEndIntegrationTests
{
    [MemoryPackable]
    public partial record OrderEvent(string OrderId, decimal Amount, string Status);

    /// <summary>
    /// End-to-end DLQ flow test:
    /// 1. Publish a message
    /// 2. Consumer fails processing (throws exception)
    /// 3. After max retries, message goes to DLQ
    /// 4. Use remediation service to replay the message
    /// 5. Consumer successfully processes the replayed message
    /// </summary>
    [Test]
    public async Task DlqFlow_PublishFailReplaySuccess_EndToEnd()
    {
        // =====================================================
        // ARRANGE: Set up NATS infrastructure
        // =====================================================
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
        var kv = new NatsKVContext(js);

        // Create main stream
        var streamName = "ORDERS_DLQ_TEST";
        var subject = "orders.dlq.test";
        await js.CreateStreamAsync(new StreamConfig(streamName, [subject])
        {
            Storage = StreamConfigStorage.Memory
        });

        // Create DLQ stream
        var dlqStreamName = "DLQ_ORDERS";
        var dlqSubject = "dlq.orders.test";
        await js.CreateStreamAsync(new StreamConfig(dlqStreamName, [dlqSubject])
        {
            Storage = StreamConfigStorage.Memory
        });

        // Create consumer with MaxDeliver = 3 and fast AckWait
        var consumerName = "orders-processor";
        var maxDeliver = 3;
        var consumer = await js.CreateOrUpdateConsumerAsync(streamName, new ConsumerConfig(consumerName)
        {
            DurableName = consumerName,
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            AckWait = TimeSpan.FromMilliseconds(500),
            MaxDeliver = maxDeliver
        });

        // Create KV bucket for DLQ entries
        var dlqBucket = await kv.CreateStoreAsync(new NatsKVConfig("fs-dlq-entries")
        {
            Storage = NatsKVStorageType.Memory
        });

        // Set up services
        var serializer = new HybridNatsSerializer();
        var logger = new ConsoleLogger<NatsJetStreamBus>();
        await using var bus = new NatsJetStreamBus(js, logger, serializer);

        // DLQ policy registry
        var dlqPolicyRegistry = new DlqPolicyRegistry(new ConsoleLogger<DlqPolicyRegistry>());
        var dlqPolicy = new DeadLetterPolicy
        {
            SourceStream = streamName,
            SourceConsumer = consumerName,
            TargetStream = StreamName.From(dlqStreamName),
            TargetSubject = dlqSubject
        };
        dlqPolicyRegistry.Register(streamName, consumerName, dlqPolicy);

        // DLQ store
        IKeyValueStore kvStore = new NatsKeyValueStore(kv, "fs-dlq-entries", new ConsoleLogger<NatsKeyValueStore>());
        var dlqStore = new NatsDlqStore(kv, _ => kvStore, new ConsoleLogger<NatsDlqStore>());

        // Poison handler
        var poisonHandler = new DefaultDlqPoisonHandler<OrderEvent>(
            bus,
            serializer,
            null, // No object store
            null, // No notification service
            dlqPolicyRegistry,
            new ConsoleLogger<DefaultDlqPoisonHandler<OrderEvent>>());

        // DLQ remediation service
        var remediationService = new NatsDlqRemediationService(
            dlqStore,
            bus,
            serializer,
            new ConsoleLogger<NatsDlqRemediationService>());

        // =====================================================
        // ACT PHASE 1: Publish message and let it fail
        // =====================================================

        // Track handler calls
        var failureCount = new CountdownEvent(maxDeliver);
        var processedOrders = new List<string>();
        var shouldFail = true;

        Func<IJsMessageContext<OrderEvent>, Task> handler = async ctx =>
        {
            Console.WriteLine($"[Handler] Received order {ctx.Message.OrderId}, Delivery #{ctx.NumDelivered}, ShouldFail={shouldFail}");

            if (shouldFail)
            {
                failureCount.Signal();
                throw new InvalidOperationException($"Simulated failure for order {ctx.Message.OrderId}");
            }

            // Success path - after replay
            processedOrders.Add(ctx.Message.OrderId);
            await ctx.AckAsync();
        };

        // Start background service
        var service = new NatsConsumerBackgroundService<OrderEvent>(
            consumer,
            streamName,
            consumerName,
            handler,
            new NatsJSConsumeOpts { MaxMsgs = 1 },
            new ConsoleLogger<NatsConsumerBackgroundService<OrderEvent>>(),
            poisonHandler);

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        await Task.Delay(500); // Let service start

        // Publish the order
        var order = new OrderEvent("ORD-12345", 99.99m, "Created");
        await js.PublishAsync(subject, order);
        Console.WriteLine($"[Test] Published order {order.OrderId}");

        // Wait for all failure attempts
        var allFailed = failureCount.Wait(TimeSpan.FromSeconds(15));
        allFailed.ShouldBeTrue($"Expected {maxDeliver} failures but got {maxDeliver - failureCount.CurrentCount}");

        // Wait for poison handler to publish to DLQ stream
        await Task.Delay(2000);

        // =====================================================
        // BRIDGE: Consume from DLQ stream and store to KV
        // (In production, this would be a separate hosted service)
        // =====================================================

        var dlqConsumer = await js.CreateOrUpdateConsumerAsync(dlqStreamName, new ConsumerConfig("dlq-storer")
        {
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            DeliverPolicy = ConsumerConfigDeliverPolicy.All
        });

        await foreach (var msg in dlqConsumer.FetchAsync<DlqMessage>(new NatsJSFetchOpts { MaxMsgs = 10, Expires = TimeSpan.FromSeconds(2) }))
        {
            if (msg.Data != null)
            {
                Console.WriteLine($"[Test] DLQ stream received message: Stream={msg.Data.OriginalStream}, Seq={msg.Data.OriginalSequence}");

                // Convert DlqMessage to DlqMessageEntry and store to KV
                var entry = new DlqMessageEntry
                {
                    Id = $"seq-{msg.Data.OriginalSequence}",
                    OriginalStream = msg.Data.OriginalStream,
                    OriginalConsumer = msg.Data.OriginalConsumer,
                    OriginalSubject = msg.Data.OriginalSubject,
                    OriginalSequence = msg.Data.OriginalSequence,
                    DeliveryCount = msg.Data.DeliveryCount,
                    StoredAt = msg.Data.FailedAt,
                    ErrorReason = msg.Data.ErrorReason,
                    Payload = msg.Data.Payload,
                    PayloadEncoding = msg.Data.PayloadEncoding,
                    Status = DlqMessageStatus.Pending
                };

                await dlqStore.StoreAsync(entry);
                await msg.AckAsync();
            }
        }

        // =====================================================
        // VERIFY: Check DLQ entry was created
        // =====================================================

        var dlqEntries = await remediationService.ListAsync(filterStream: streamName);
        Console.WriteLine($"[Test] Found {dlqEntries.Count} DLQ entries");

        dlqEntries.Count.ShouldBeGreaterThanOrEqualTo(1, "Should have at least one DLQ entry");
        var dlqEntry = dlqEntries.First();
        dlqEntry.OriginalStream.ShouldBe(streamName);
        dlqEntry.OriginalConsumer.ShouldBe(consumerName);
        dlqEntry.DeliveryCount.ShouldBe(maxDeliver);
        dlqEntry.ErrorReason!.ShouldContain("Simulated failure");

        Console.WriteLine($"[Test] DLQ Entry: ID={dlqEntry.Id}, Stream={dlqEntry.OriginalStream}, Error={dlqEntry.ErrorReason}");

        // =====================================================
        // ACT PHASE 2: Replay with modification (fixed payload)
        // =====================================================

        // Stop failing - next message will succeed
        shouldFail = false;

        // Build the full key for the DLQ entry
        var dlqKey = $"{streamName}.{consumerName}.{dlqEntry.Id}";

        // Replay with modified payload
        var fixedOrder = new OrderEvent(order.OrderId, order.Amount, "Fixed");
        var replayResult = await remediationService.ReplayWithModificationAsync(dlqKey, fixedOrder);

        Console.WriteLine($"[Test] Replay result: Success={replayResult.Success}, Action={replayResult.Action}");
        replayResult.Success.ShouldBeTrue("Replay should succeed");
        replayResult.Action.ShouldBe(DlqRemediationAction.Replayed);

        // Wait for the replayed message to be processed
        await Task.Delay(2000);

        // =====================================================
        // ASSERT: Verify success
        // =====================================================

        processedOrders.ShouldContain(order.OrderId, "Order should be processed after replay");
        Console.WriteLine($"[Test] Successfully processed orders: {string.Join(", ", processedOrders)}");

        // Verify DLQ entry is now resolved
        var updatedEntry = await remediationService.InspectAsync(dlqKey);
        updatedEntry.ShouldNotBeNull();
        updatedEntry!.Status.ShouldBe(DlqMessageStatus.Resolved);

        // Cleanup
        await cts.CancelAsync();
        await service.StopAsync(CancellationToken.None);
    }

    /// <summary>
    /// Test that InspectAsync returns the full DLQ entry details.
    /// </summary>
    [Test]
    public async Task DlqRemediation_InspectAsync_ReturnsEntryDetails()
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
        var kv = new NatsKVContext(js);

        // Create DLQ stream
        var dlqStreamName = "DLQ_INSPECT_TEST";
        var dlqSubject = "dlq.inspect.test";
        await js.CreateStreamAsync(new StreamConfig(dlqStreamName, [dlqSubject])
        {
            Storage = StreamConfigStorage.Memory
        });

        // Create KV bucket
        await kv.CreateStoreAsync(new NatsKVConfig("fs-dlq-entries")
        {
            Storage = NatsKVStorageType.Memory
        });

        var serializer = new HybridNatsSerializer();
        await using var bus = new NatsJetStreamBus(js, new ConsoleLogger<NatsJetStreamBus>(), serializer);

        IKeyValueStore kvStore = new NatsKeyValueStore(kv, "fs-dlq-entries", new ConsoleLogger<NatsKeyValueStore>());
        var dlqStore = new NatsDlqStore(kv, _ => kvStore, new ConsoleLogger<NatsDlqStore>());

        var remediationService = new NatsDlqRemediationService(
            dlqStore,
            bus,
            serializer,
            new ConsoleLogger<NatsDlqRemediationService>());

        // Manually store a DLQ entry
        var entry = new DlqMessageEntry
        {
            Id = "test-msg-123",
            OriginalStream = "test-stream",
            OriginalConsumer = "test-consumer",
            OriginalSubject = "test.subject",
            OriginalSequence = 42,
            DeliveryCount = 5,
            StoredAt = DateTimeOffset.UtcNow,
            ErrorReason = "Test error reason",
            Payload = System.Text.Encoding.UTF8.GetBytes("{\"test\":\"data\"}"),
            PayloadEncoding = "application/json"
        };
        await dlqStore.StoreAsync(entry);

        // Act
        var key = $"{entry.OriginalStream}.{entry.OriginalConsumer}.{entry.Id}";
        var result = await remediationService.InspectAsync(key);

        // Assert
        result.ShouldNotBeNull();
        result!.Id.ShouldBe(entry.Id);
        result.OriginalStream.ShouldBe(entry.OriginalStream);
        result.OriginalConsumer.ShouldBe(entry.OriginalConsumer);
        result.OriginalSubject.ShouldBe(entry.OriginalSubject);
        result.OriginalSequence.ShouldBe(entry.OriginalSequence);
        result.DeliveryCount.ShouldBe(entry.DeliveryCount);
        result.ErrorReason.ShouldBe(entry.ErrorReason);
        result.Payload.ShouldNotBeNull();
        result.Payload!.Length.ShouldBeGreaterThan(0);
    }

    /// <summary>
    /// Test archive functionality marks entry as archived.
    /// </summary>
    [Test]
    public async Task DlqRemediation_ArchiveAsync_MarksEntryAsArchived()
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
        var kv = new NatsKVContext(js);

        await js.CreateStreamAsync(new StreamConfig("DLQ_ARCHIVE_TEST", ["dlq.archive.test"])
        {
            Storage = StreamConfigStorage.Memory
        });

        await kv.CreateStoreAsync(new NatsKVConfig("fs-dlq-entries")
        {
            Storage = NatsKVStorageType.Memory
        });

        var serializer = new HybridNatsSerializer();
        await using var bus = new NatsJetStreamBus(js, new ConsoleLogger<NatsJetStreamBus>(), serializer);

        IKeyValueStore kvStore = new NatsKeyValueStore(kv, "fs-dlq-entries", new ConsoleLogger<NatsKeyValueStore>());
        var dlqStore = new NatsDlqStore(kv, _ => kvStore, new ConsoleLogger<NatsDlqStore>());

        var remediationService = new NatsDlqRemediationService(
            dlqStore,
            bus,
            serializer,
            new ConsoleLogger<NatsDlqRemediationService>());

        // Store an entry
        var entry = new DlqMessageEntry
        {
            Id = "archive-test-msg",
            OriginalStream = "archive-stream",
            OriginalConsumer = "archive-consumer",
            OriginalSubject = "archive.subject",
            OriginalSequence = 1,
            DeliveryCount = 3,
            StoredAt = DateTimeOffset.UtcNow,
            ErrorReason = "Obsolete message"
        };
        await dlqStore.StoreAsync(entry);

        var key = $"{entry.OriginalStream}.{entry.OriginalConsumer}.{entry.Id}";

        // Act
        var result = await remediationService.ArchiveAsync(key, "No longer needed");

        // Assert
        result.Success.ShouldBeTrue();
        result.Action.ShouldBe(DlqRemediationAction.Archived);

        var updatedEntry = await remediationService.InspectAsync(key);
        updatedEntry.ShouldNotBeNull();
        updatedEntry!.Status.ShouldBe(DlqMessageStatus.Archived);
    }

    public class ConsoleLogger : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            Console.WriteLine($"[{logLevel}] {formatter(state, exception)}");
            if (exception != null) Console.WriteLine(exception);
        }
    }

    public class ConsoleLogger<T> : ConsoleLogger, ILogger<T> { }
}

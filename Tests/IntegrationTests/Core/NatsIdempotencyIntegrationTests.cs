using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core;
using FlySwattr.NATS.Core.Serializers;
using IntegrationTests.Infrastructure;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using TUnit.Core;

namespace IntegrationTests.Core;

[Property("nTag", "Core")]
public class NatsIdempotencyIntegrationTests
{
    public record OrderCreatedEvent(string OrderId, decimal Amount);

    [Test]
    public async Task PublishAsync_WithSameMessageId_ShouldDeduplicateOnServer()
    {
        // Arrange
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();
        
        var opts = new NatsOpts { Url = fixture.ConnectionString };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();
        
        var jsContext = new NatsJSContext(conn);
        
        // Create stream with de-duplication window
        var streamName = "ORDERS";
        var subject = "orders.created";
        await jsContext.CreateStreamAsync(new StreamConfig
        {
            Name = streamName,
            Subjects = [subject],
            DuplicateWindow = TimeSpan.FromMinutes(1), // 1-minute deduplication window
            Storage = StreamConfigStorage.Memory
        });
        
        var serializer = new HybridNatsSerializer();
        var logger = NullLogger<NatsJetStreamBus>.Instance;
        await using var bus = new NatsJetStreamBus(jsContext, logger, serializer);
        
        var orderEvent = new OrderCreatedEvent("ORD-12345", 99.99m);
        var messageId = "order-ORD-12345-created-v1"; // Business-key derived ID
        
        // Act - publish the SAME message with the SAME messageId twice
        // First publish succeeds
        await bus.PublishAsync(subject, orderEvent, messageId);
        
        // Second publish should throw NatsJSDuplicateMessageException (via EnsureSuccess)
        // because NATS detects it as a duplicate within the deduplication window.
        // This is the EXPECTED behavior - it proves deduplication is working!
        var duplicateException = await Assert.ThrowsAsync<NatsJSDuplicateMessageException>(async () =>
            await bus.PublishAsync(subject, orderEvent, messageId));
        
        await Assert.That(duplicateException!.Message).Contains("Duplicate");
        
        // Assert - query stream info and verify only 1 message exists
        var stream = await jsContext.GetStreamAsync(streamName);
        var streamInfo = stream.Info;
        
        await Assert.That((long)streamInfo.State.Messages).IsEqualTo(1L);
    }

    [Test]
    public async Task PublishAsync_WithDifferentMessageIds_ShouldStoreBothMessages()
    {
        // Arrange
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();
        
        var opts = new NatsOpts { Url = fixture.ConnectionString };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();
        
        var jsContext = new NatsJSContext(conn);
        
        var streamName = "ORDERS_DIFF";
        var subject = "orders.created.diff";
        await jsContext.CreateStreamAsync(new StreamConfig
        {
            Name = streamName,
            Subjects = [subject],
            DuplicateWindow = TimeSpan.FromMinutes(1),
            Storage = StreamConfigStorage.Memory
        });
        
        var serializer = new HybridNatsSerializer();
        var logger = NullLogger<NatsJetStreamBus>.Instance;
        await using var bus = new NatsJetStreamBus(jsContext, logger, serializer);
        
        var orderEvent1 = new OrderCreatedEvent("ORD-001", 50.00m);
        var orderEvent2 = new OrderCreatedEvent("ORD-002", 75.00m);
        
        // Act - publish with DIFFERENT messageIds
        await bus.PublishAsync(subject, orderEvent1, "order-ORD-001-created");
        await bus.PublishAsync(subject, orderEvent2, "order-ORD-002-created");
        
        // Assert - both messages should be stored
        var stream = await jsContext.GetStreamAsync(streamName);
        
        await Assert.That((long)stream.Info.State.Messages).IsEqualTo(2L);
    }
}

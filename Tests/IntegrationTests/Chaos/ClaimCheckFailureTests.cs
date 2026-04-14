using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core;
using FlySwattr.NATS.Core.Configuration;
using FlySwattr.NATS.Core.Decorators;
using FlySwattr.NATS.Core.Serializers;
using FlySwattr.NATS.Core.Services;
using FlySwattr.NATS.Core.Stores;
using IntegrationTests.Infrastructure;
using MemoryPack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.ObjectStore;
using Shouldly;
using TUnit.Core;

namespace IntegrationTests.Chaos;

/// <summary>
/// Integration tests for claim-check (payload offloading) failure scenarios.
/// </summary>
[Property("nTag", "Integration")]
[Property("nTag", "Chaos")]
public partial class ClaimCheckFailureTests
{
    [MemoryPackable]
    internal partial record TestPayload(string Id, byte[] Data);

    [Test]
    public async Task ClaimCheckCleanupMiddleware_ShouldDeleteObjectAfterProcessing()
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
        var objContext = new NatsObjContext(js);

        var streamName = $"CLAIM_CLEANUP_{Guid.NewGuid():N}";
        var subject = "claim.cleanup.test";
        await js.CreateStreamAsync(new StreamConfig(streamName, [subject])
        {
            Storage = StreamConfigStorage.Memory
        });

        var consumerName = "cleanup-consumer";
        await js.CreateOrUpdateConsumerAsync(streamName, new ConsumerConfig(consumerName)
        {
            DurableName = consumerName,
            AckPolicy = ConsumerConfigAckPolicy.Explicit
        });

        var bucketName = $"claim-cleanup-{Guid.NewGuid():N}";
        await objContext.CreateObjectStoreAsync(new NatsObjConfig(bucketName)
        {
            Storage = NatsObjStorageType.Memory
        });

        var serializer = new HybridNatsSerializer();
        await using var innerBus = new NatsJetStreamBus(js, NullLogger<NatsJetStreamBus>.Instance, serializer);
        await using var objectStore = new NatsObjectStore(objContext, bucketName, NullLogger<NatsObjectStore>.Instance);

        var offloadOptions = new PayloadOffloadingOptions
        {
            ThresholdBytes = 100, // Very small threshold to force offloading
            ObjectKeyPrefix = "claimcheck"
        };
        var typeAliasRegistry = new MessageTypeAliasRegistry(Options.Create(new MessageTypeAliasOptions()));

        var publisher = new OffloadingJetStreamPublisher(
            innerBus, objectStore, serializer, typeAliasRegistry,
            Options.Create(offloadOptions),
            NullLogger<OffloadingJetStreamPublisher>.Instance);

        // Act: Publish a message that will be offloaded
        var largeData = new byte[500];
        Random.Shared.NextBytes(largeData);
        var payload = new TestPayload("test-1", largeData);

        await publisher.PublishAsync(subject, payload, "msg-cleanup-1", cancellationToken: CancellationToken.None);

        // Verify the object exists in the store
        var objects = await objectStore.ListAsync(cancellationToken: CancellationToken.None);
        var objectList = objects.ToList();
        objectList.Count.ShouldBe(1);
        var objectKey = objectList[0].Key;

        // Consume and resolve the claim check
        var consumer = new OffloadingJetStreamConsumer(
            innerBus, objectStore, serializer,
            Options.Create(offloadOptions),
            NullLogger<OffloadingJetStreamConsumer>.Instance);

        var received = new TaskCompletionSource<IJsMessageContext<TestPayload>>();

        // Start consuming — this uses the decorator's ConsumeAsync
        var consumeTask = consumer.ConsumePullAsync<TestPayload>(
            StreamName.From(streamName),
            ConsumerName.From(consumerName),
            async ctx =>
            {
                received.TrySetResult(ctx);
            },
            cancellationToken: CancellationToken.None);

        // Wait for the message
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var context = await received.Task.WaitAsync(cts.Token);

        // Verify the context carries the claim check key
        context.ShouldBeAssignableTo<OffloadingMessageContext<TestPayload>>();
        var offloadingCtx = (OffloadingMessageContext<TestPayload>)context;
        offloadingCtx.ClaimCheckObjectKey.ShouldNotBeNull();

        // Run the cleanup middleware
        var middleware = new ClaimCheckCleanupMiddleware<TestPayload>(
            objectStore,
            Options.Create(offloadOptions),
            NullLogger<ClaimCheckCleanupMiddleware<TestPayload>>.Instance);

        var acked = false;
        await middleware.InvokeAsync(context, async () =>
        {
            await context.AckAsync();
            acked = true;
        }, CancellationToken.None);

        acked.ShouldBeTrue();

        // Verify the object was deleted
        var objectsAfter = await objectStore.ListAsync(cancellationToken: CancellationToken.None);
        objectsAfter.Count().ShouldBe(0);
    }

    [Test]
    public async Task BatchPublish_ShouldPublishAllMessages()
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
        var streamName = $"BATCH_PUB_{Guid.NewGuid():N}";
        var subject = "batch.pub.test";
        await js.CreateStreamAsync(new StreamConfig(streamName, [subject])
        {
            Storage = StreamConfigStorage.Memory
        });

        var serializer = new HybridNatsSerializer();
        await using var bus = new NatsJetStreamBus(js, NullLogger<NatsJetStreamBus>.Instance, serializer);

        // Act: Batch publish 10 messages
        var messages = Enumerable.Range(0, 10)
            .Select(i => new BatchMessage<string>(subject, $"message-{i}", $"batch-msg-{i}"))
            .ToList();

        await bus.PublishBatchAsync(messages, CancellationToken.None);

        // Assert: All 10 messages should be in the stream
        var consumer = await js.CreateOrUpdateConsumerAsync(streamName, new ConsumerConfig("batch-verify")
        {
            DurableName = "batch-verify",
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            DeliverPolicy = ConsumerConfigDeliverPolicy.All
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var received = new List<string>();
        await foreach (var msg in consumer.FetchAsync<string>(new NatsJSFetchOpts { MaxMsgs = 20 },
                           cancellationToken: cts.Token))
        {
            if (msg.Data != null)
                received.Add(msg.Data);
            await msg.AckAsync(cancellationToken: cts.Token);
        }

        received.Count.ShouldBe(10);
        for (var i = 0; i < 10; i++)
        {
            received.ShouldContain($"message-{i}");
        }
    }

    [Test]
    public async Task BatchPublish_ShouldThrowAggregateException_WhenSomeMessagesFail()
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
        var streamName = $"BATCH_FAIL_{Guid.NewGuid():N}";
        var validSubject = "batch.fail.valid";
        await js.CreateStreamAsync(new StreamConfig(streamName, [validSubject])
        {
            Storage = StreamConfigStorage.Memory
        });

        var serializer = new HybridNatsSerializer();
        await using var bus = new NatsJetStreamBus(js, NullLogger<NatsJetStreamBus>.Instance, serializer);

        // Act: Batch with one valid and one invalid subject (not in stream)
        var messages = new List<BatchMessage<string>>
        {
            new(validSubject, "valid-message", "msg-valid"),
            new("batch.fail.nonexistent", "will-fail", "msg-fail") // Subject not in stream
        };

        // Assert: Should throw AggregateException for the failed message
        var ex = await Should.ThrowAsync<AggregateException>(
            () => bus.PublishBatchAsync(messages, CancellationToken.None));

        ex.InnerExceptions.Count.ShouldBeGreaterThan(0);
    }
}

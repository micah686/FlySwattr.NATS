using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core;
using FlySwattr.NATS.Core.Configuration;
using FlySwattr.NATS.Core.Decorators;
using FlySwattr.NATS.Core.Serializers;
using FlySwattr.NATS.Core.Stores;
using IntegrationTests.Infrastructure;
using MemoryPack;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.ObjectStore;
using Shouldly;
using TUnit.Core;

namespace IntegrationTests.Core;

/// <summary>
/// Integration tests for payload offloading round-trip:
/// Publish large payload → Offload to Object Store → Consume → Hydrate original message
///
/// Tests the Claim Check pattern implementation using real NATS infrastructure.
/// </summary>
[Property("nTag", "Integration")]
public partial class PayloadOffloadingIntegrationTests
{
    /// <summary>
    /// Message type with a large byte array payload for offloading tests.
    /// </summary>
    [MemoryPackable]
    public partial record LargePayloadEvent(string EventId, byte[] Data, string Description);

    #region Test 1: Large Payload Round-Trip (Offloading)

    /// <summary>
    /// Verify that a message significantly larger than the threshold is automatically
    /// offloaded to the Object Store and correctly reassembled by the consumer.
    /// </summary>
    [Test]
    public async Task PayloadOffloading_ShouldOffloadAndHydrate_WhenPayloadExceedsDefaultThreshold()
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

        var streamName = $"OFFLOAD_LARGE_{Guid.NewGuid():N}";
        var subject = "offload.large.test";
        await js.CreateStreamAsync(new StreamConfig(streamName, [subject])
        {
            Storage = StreamConfigStorage.Memory
        });

        var consumerName = "large-payload-processor";
        var consumer = await js.CreateOrUpdateConsumerAsync(streamName, new ConsumerConfig(consumerName)
        {
            DurableName = consumerName,
            AckPolicy = ConsumerConfigAckPolicy.Explicit
        });

        var bucketName = $"payload-offload-{Guid.NewGuid():N}";
        await objContext.CreateObjectStoreAsync(new NatsObjConfig(bucketName)
        {
            Storage = NatsObjStorageType.Memory
        });

        var serializer = new HybridNatsSerializer();
        await using var innerBus = new NatsJetStreamBus(js, new ConsoleLogger<NatsJetStreamBus>(), serializer);

        var offloadOptions = new PayloadOffloadingOptions
        {
            ThresholdBytes = 1024 * 1024, // 1MB threshold
            ObjectKeyPrefix = "claimcheck"
        };

        await using var objectStore = new NatsObjectStore(
            objContext,
            bucketName,
            new ConsoleLogger<NatsObjectStore>());

        var offloadingPublisher = new OffloadingJetStreamPublisher(
            innerBus,
            objectStore,
            serializer,
            Options.Create(offloadOptions),
            new ConsoleLogger<OffloadingJetStreamPublisher>());

        // Create 2MB payload
        var payloadSize = 2 * 1024 * 1024;
        var largeData = new byte[payloadSize];
        Random.Shared.NextBytes(largeData);
        var originalMessage = new LargePayloadEvent("EVT-2MB-001", largeData, "2MB payload");

        // Act - Publish
        await offloadingPublisher.PublishAsync(subject, originalMessage, $"large-{Guid.NewGuid():N}");

        // Assert - Verify object store has the payload
        var storedObjects = (await objectStore.ListAsync()).ToList();
        storedObjects.Count.ShouldBe(1, "Object Store should contain the offloaded payload");
        storedObjects.First().Size.ShouldBeGreaterThan(payloadSize - 1000);

        // Assert - Verify stream has one message (the ClaimCheckMessage)
        var stream = await js.GetStreamAsync(streamName);
        ((long)stream.Info.State.Messages).ShouldBe(1L);

        // Act - Consume and hydrate
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        LargePayloadEvent? receivedMessage = null;

        await foreach (var msg in consumer.FetchAsync<ClaimCheckMessage>(
            new NatsJSFetchOpts { MaxMsgs = 1, Expires = TimeSpan.FromSeconds(5) },
            cancellationToken: cts.Token))
        {
            if (msg.Data != null)
            {
                // Hydrate from object store
                var objectKey = msg.Data.ObjectStoreRef.Replace("objstore://", "");
                using var memoryStream = new MemoryStream();
                await objectStore.GetAsync(objectKey, memoryStream);
                receivedMessage = serializer.Deserialize<LargePayloadEvent>(memoryStream.ToArray());
                await msg.AckAsync();
            }
        }

        // Assert - Verify round-trip
        receivedMessage.ShouldNotBeNull();
        receivedMessage.EventId.ShouldBe(originalMessage.EventId);
        receivedMessage.Data.Length.ShouldBe(payloadSize);
        receivedMessage.Data.ShouldBe(originalMessage.Data);
    }

    #endregion

    #region Test 2: Small Payload (Inline Passthrough)

    /// <summary>
    /// Verify that a message smaller than the threshold is sent directly through
    /// NATS JetStream without interacting with the Object Store.
    /// </summary>
    [Test]
    public async Task PayloadOffloading_ShouldNotOffload_WhenPayloadIsBelowThreshold()
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

        var streamName = $"OFFLOAD_SMALL_{Guid.NewGuid():N}";
        var subject = "offload.small.test";
        await js.CreateStreamAsync(new StreamConfig(streamName, [subject])
        {
            Storage = StreamConfigStorage.Memory
        });

        var bucketName = $"payload-small-{Guid.NewGuid():N}";
        await objContext.CreateObjectStoreAsync(new NatsObjConfig(bucketName)
        {
            Storage = NatsObjStorageType.Memory
        });

        var serializer = new HybridNatsSerializer();
        await using var innerBus = new NatsJetStreamBus(js, new ConsoleLogger<NatsJetStreamBus>(), serializer);

        var offloadOptions = new PayloadOffloadingOptions
        {
            ThresholdBytes = 1024 * 1024, // 1MB threshold
            ObjectKeyPrefix = "claimcheck"
        };

        await using var objectStore = new NatsObjectStore(
            objContext,
            bucketName,
            new ConsoleLogger<NatsObjectStore>());

        var offloadingPublisher = new OffloadingJetStreamPublisher(
            innerBus,
            objectStore,
            serializer,
            Options.Create(offloadOptions),
            new ConsoleLogger<OffloadingJetStreamPublisher>());

        // Create small 1KB payload (well below 1MB threshold)
        var smallData = new byte[1024];
        Random.Shared.NextBytes(smallData);
        var originalMessage = new LargePayloadEvent("EVT-1KB-001", smallData, "1KB payload");

        // Act - Publish
        await offloadingPublisher.PublishAsync(subject, originalMessage, "small-msg-001");

        // Assert - Object Store should be EMPTY (no offloading)
        var storedObjects = await objectStore.ListAsync();
        storedObjects.ShouldBeEmpty("Object Store should be empty for payloads below threshold");

        // Assert - Stream should have the message (published inline)
        var stream = await js.GetStreamAsync(streamName);
        ((long)stream.Info.State.Messages).ShouldBe(1L, "Stream should contain the inline message");

        // The message was stored inline in JetStream (not offloaded to object store)
        // Stream message count of 1 + empty object store confirms inline passthrough
    }

    #endregion

    #region Test 3: Threshold Boundary Verification

    /// <summary>
    /// Verify the exact byte-level decision logic for offloading using a custom low threshold.
    /// </summary>
    [Test]
    public async Task PayloadOffloading_ShouldSwitchMode_ExactlyAtThreshold()
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

        var streamName = $"OFFLOAD_BOUNDARY_{Guid.NewGuid():N}";
        var subject = "offload.boundary.test";
        await js.CreateStreamAsync(new StreamConfig(streamName, [subject])
        {
            Storage = StreamConfigStorage.Memory
        });

        var bucketName = $"payload-boundary-{Guid.NewGuid():N}";
        await objContext.CreateObjectStoreAsync(new NatsObjConfig(bucketName)
        {
            Storage = NatsObjStorageType.Memory
        });

        var serializer = new HybridNatsSerializer();
        await using var innerBus = new NatsJetStreamBus(js, new ConsoleLogger<NatsJetStreamBus>(), serializer);

        // Use 100-byte threshold for boundary testing
        var offloadOptions = new PayloadOffloadingOptions
        {
            ThresholdBytes = 100,
            ObjectKeyPrefix = "claimcheck"
        };

        await using var objectStore = new NatsObjectStore(
            objContext,
            bucketName,
            new ConsoleLogger<NatsObjectStore>());

        var offloadingPublisher = new OffloadingJetStreamPublisher(
            innerBus,
            objectStore,
            serializer,
            Options.Create(offloadOptions),
            new ConsoleLogger<OffloadingJetStreamPublisher>());

        using var listCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        // Case A: Small payload (serializes to < 100 bytes) - should NOT offload
        var smallMessage = new LargePayloadEvent("S", new byte[1], "X");
        await offloadingPublisher.PublishAsync(subject, smallMessage, "boundary-small");

        var objectsAfterSmall = (await objectStore.ListAsync(cancellationToken: listCts.Token)).ToList();
        objectsAfterSmall.Count.ShouldBe(0, "Case A: Object Store should be empty for small payload");

        // Case B: Large payload (serializes to > 100 bytes) - should offload
        var largeMessage = new LargePayloadEvent("L", new byte[200], "Large enough to offload");
        await offloadingPublisher.PublishAsync(subject, largeMessage, "boundary-large");

        var objectsAfterLarge = (await objectStore.ListAsync(cancellationToken: listCts.Token)).ToList();
        objectsAfterLarge.Count.ShouldBe(1, "Case B: Object Store should have one object for large payload");

        // Verify stream has both messages
        var stream = await js.GetStreamAsync(streamName);
        ((long)stream.Info.State.Messages).ShouldBe(2L, "Stream should contain both messages");
    }

    #endregion

    #region Test 4: Metadata Preservation

    /// <summary>
    /// Ensure that Message IDs are preserved when the payload is moved to the Object Store.
    /// This verifies that JetStream deduplication still works on the claim check message.
    /// </summary>
    [Test]
    public async Task PayloadOffloading_ShouldPreserveHeadersAndId_WhenOffloaded()
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

        var streamName = $"OFFLOAD_METADATA_{Guid.NewGuid():N}";
        var subject = "offload.metadata.test";

        // Create stream with deduplication window
        await js.CreateStreamAsync(new StreamConfig(streamName, [subject])
        {
            Storage = StreamConfigStorage.Memory,
            DuplicateWindow = TimeSpan.FromMinutes(1)
        });

        var consumerName = "metadata-processor";
        var consumer = await js.CreateOrUpdateConsumerAsync(streamName, new ConsumerConfig(consumerName)
        {
            DurableName = consumerName,
            AckPolicy = ConsumerConfigAckPolicy.Explicit
        });

        var bucketName = $"payload-metadata-{Guid.NewGuid():N}";
        await objContext.CreateObjectStoreAsync(new NatsObjConfig(bucketName)
        {
            Storage = NatsObjStorageType.Memory
        });

        var serializer = new HybridNatsSerializer();
        await using var innerBus = new NatsJetStreamBus(js, new ConsoleLogger<NatsJetStreamBus>(), serializer);

        var offloadOptions = new PayloadOffloadingOptions
        {
            ThresholdBytes = 1024, // 1KB threshold
            ObjectKeyPrefix = "claimcheck"
        };

        await using var objectStore = new NatsObjectStore(
            objContext,
            bucketName,
            new ConsoleLogger<NatsObjectStore>());

        var offloadingPublisher = new OffloadingJetStreamPublisher(
            innerBus,
            objectStore,
            serializer,
            Options.Create(offloadOptions),
            new ConsoleLogger<OffloadingJetStreamPublisher>());

        // Create 2KB payload (will be offloaded)
        var largeData = new byte[2 * 1024];
        Random.Shared.NextBytes(largeData);
        var originalMessage = new LargePayloadEvent("EVT-META-001", largeData, "Metadata test");
        var businessMessageId = "Order-12345-Created-v1";

        // Act - Publish first time
        await offloadingPublisher.PublishAsync(subject, originalMessage, businessMessageId);

        // Assert - Verify object store has payload
        var storedObjects = (await objectStore.ListAsync()).ToList();
        storedObjects.Count.ShouldBe(1);

        // Assert - Verify stream has message
        var stream = await js.GetStreamAsync(streamName);
        ((long)stream.Info.State.Messages).ShouldBe(1L);

        // Act - Consume and verify content
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        LargePayloadEvent? receivedMessage = null;

        await foreach (var msg in consumer.FetchAsync<ClaimCheckMessage>(
            new NatsJSFetchOpts { MaxMsgs = 1, Expires = TimeSpan.FromSeconds(5) },
            cancellationToken: cts.Token))
        {
            if (msg.Data != null)
            {
                var objectKey = msg.Data.ObjectStoreRef.Replace("objstore://", "");
                using var memoryStream = new MemoryStream();
                await objectStore.GetAsync(objectKey, memoryStream);
                receivedMessage = serializer.Deserialize<LargePayloadEvent>(memoryStream.ToArray());
                await msg.AckAsync();
            }
        }

        receivedMessage.ShouldNotBeNull();
        receivedMessage.EventId.ShouldBe(originalMessage.EventId);
        receivedMessage.Data.ShouldBe(originalMessage.Data);

        // Act - Attempt duplicate publish (same Message ID)
        var duplicateException = await Should.ThrowAsync<NatsJSDuplicateMessageException>(
            async () => await offloadingPublisher.PublishAsync(subject, originalMessage, businessMessageId));

        duplicateException.Message.ShouldContain("Duplicate");

        // Assert - Stream still has only 1 message (duplicate was rejected)
        stream = await js.GetStreamAsync(streamName);
        ((long)stream.Info.State.Messages).ShouldBe(1L);

        // Assert - Object store still has only 1 object (no orphan from failed duplicate)
        storedObjects = (await objectStore.ListAsync()).ToList();
        storedObjects.Count.ShouldBe(1);
    }

    #endregion

    #region Console Logger Helper

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

    #endregion
}

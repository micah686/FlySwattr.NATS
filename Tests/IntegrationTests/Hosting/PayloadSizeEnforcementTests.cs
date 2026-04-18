using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core;
using FlySwattr.NATS.Core.Configuration;
using FlySwattr.NATS.Core.Decorators;
using FlySwattr.NATS.Core.Serializers;
using FlySwattr.NATS.Core.Services;
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

namespace IntegrationTests.Hosting;

/// <summary>
/// Integration tests verifying <see cref="SizeLimitingBufferWriter"/> throws when
/// a serialized payload exceeds <see cref="HybridNatsSerializer"/>'s configured maximum.
/// The enforcement is exercised through <see cref="OffloadingJetStreamPublisher"/>,
/// which calls <c>serializer.Serialize</c> during its size-check pass.
/// </summary>
[Property("nTag", "Integration")]
public partial class PayloadSizeEnforcementTests
{
    [MemoryPackable]
    public partial record SizeTestMessage(string Id, string Data);

    [Test]
    public async Task PublishAsync_PayloadExceedsMaxSize_ThrowsInvalidOperationException()
    {
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var natsOpts = new NatsOpts
        {
            Url = fixture.ConnectionString,
            SerializerRegistry = HybridSerializerRegistry.Default
        };
        await using var conn = new NatsConnection(natsOpts);
        await conn.ConnectAsync();

        var js = new NatsJSContext(conn);
        var objContext = new NatsObjContext(js);

        var streamName = "SIZE_ENFORCEMENT_TEST";
        var subject = "size.enforcement.test";
        await js.CreateStreamAsync(new StreamConfig(streamName, [subject])
        {
            Storage = StreamConfigStorage.Memory
        });

        var bucketName = $"size-enforce-{Guid.NewGuid():N}";
        await objContext.CreateObjectStoreAsync(new NatsObjConfig(bucketName)
        {
            Storage = NatsObjStorageType.Memory
        });

        var serializer = new HybridNatsSerializer(maxPayloadSize: 100);
        await using var bus = new NatsJetStreamBus(js, new ConsoleLogger<NatsJetStreamBus>(), serializer);

        await using var objectStore = new NatsObjectStore(
            objContext,
            bucketName,
            new ConsoleLogger<NatsObjectStore>());

        var typeAliasRegistry = new MessageTypeAliasRegistry(
            Options.Create(new MessageTypeAliasOptions { RequireExplicitAliases = false }));

        var offloadingPublisher = new OffloadingJetStreamPublisher(
            bus,
            objectStore,
            serializer,
            typeAliasRegistry,
            Options.Create(new PayloadOffloadingOptions { ThresholdBytes = 10_000, ObjectKeyPrefix = "claimcheck" }),
            new ConsoleLogger<OffloadingJetStreamPublisher>());

        var oversizedMessage = new SizeTestMessage("oversize", new string('X', 200));

        var ex = await Should.ThrowAsync<InvalidOperationException>(async () =>
            await offloadingPublisher.PublishAsync(subject, oversizedMessage, "size-msg-1"));

        ex.Message.ShouldContain("maximum payload size");
    }

    [Test]
    public async Task PublishAsync_PayloadBelowMaxSize_Succeeds()
    {
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var natsOpts = new NatsOpts
        {
            Url = fixture.ConnectionString,
            SerializerRegistry = HybridSerializerRegistry.Default
        };
        await using var conn = new NatsConnection(natsOpts);
        await conn.ConnectAsync();

        var js = new NatsJSContext(conn);
        var objContext = new NatsObjContext(js);

        var streamName = "SIZE_ENFORCEMENT_OK_TEST";
        var subject = "size.enforcement.ok";
        await js.CreateStreamAsync(new StreamConfig(streamName, [subject])
        {
            Storage = StreamConfigStorage.Memory
        });

        var consumerName = "size-ok-consumer";
        var consumer = await js.CreateOrUpdateConsumerAsync(streamName, new ConsumerConfig(consumerName)
        {
            DurableName = consumerName,
            AckPolicy = ConsumerConfigAckPolicy.Explicit
        });

        var bucketName = $"size-ok-{Guid.NewGuid():N}";
        await objContext.CreateObjectStoreAsync(new NatsObjConfig(bucketName)
        {
            Storage = NatsObjStorageType.Memory
        });

        var serializer = new HybridNatsSerializer(maxPayloadSize: 4096);
        await using var bus = new NatsJetStreamBus(js, new ConsoleLogger<NatsJetStreamBus>(), serializer);

        await using var objectStore = new NatsObjectStore(
            objContext,
            bucketName,
            new ConsoleLogger<NatsObjectStore>());

        var typeAliasRegistry = new MessageTypeAliasRegistry(
            Options.Create(new MessageTypeAliasOptions { RequireExplicitAliases = false }));

        var offloadingPublisher = new OffloadingJetStreamPublisher(
            bus,
            objectStore,
            serializer,
            typeAliasRegistry,
            Options.Create(new PayloadOffloadingOptions { ThresholdBytes = 1024 * 1024, ObjectKeyPrefix = "claimcheck" }),
            new ConsoleLogger<OffloadingJetStreamPublisher>());

        var smallMessage = new SizeTestMessage("ok-1", "within-limits");
        await offloadingPublisher.PublishAsync(subject, smallMessage, "size-ok-1");

        var stream = await js.GetStreamAsync(streamName);
        ((long)stream.Info.State.Messages).ShouldBe(1L);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        SizeTestMessage? received = null;
        await foreach (var msg in consumer.FetchAsync<SizeTestMessage>(
                           new NatsJSFetchOpts { MaxMsgs = 1, Expires = TimeSpan.FromSeconds(2) },
                           cancellationToken: cts.Token))
        {
            received = msg.Data;
            await msg.AckAsync(cancellationToken: cts.Token);
            break;
        }

        received.ShouldNotBeNull();
        received.Id.ShouldBe(smallMessage.Id);
        received.Data.ShouldBe(smallMessage.Data);
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

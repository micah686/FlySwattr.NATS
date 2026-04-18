using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core;
using FlySwattr.NATS.Core.Configuration;
using FlySwattr.NATS.Core.Decorators;
using FlySwattr.NATS.Core.Serializers;
using FlySwattr.NATS.Core.Services;
using FlySwattr.NATS.Core.Stores;
using FlySwattr.NATS.Hosting.Services;
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
/// Verifies that <see cref="NatsConsumerBackgroundService{T}"/> hydrates claim-check
/// payloads when constructed with serializer, object store, and offloading options.
/// Exercises the raw-consumption path used by the hosted topology consumer when
/// payload offloading is enabled.
/// </summary>
[Property("nTag", "Hosting")]
public partial class HostedConsumerWithOffloadingTests
{
    [MemoryPackable]
    public partial record LargeHostedMessage(string Id, string Data);

    [Test]
    public async Task HostedConsumer_WithOffloading_HydratesLargePayload()
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

        var streamName = "OFFLOAD_HOSTED_TEST";
        var subject = "offload.hosted.test";
        await js.CreateStreamAsync(new StreamConfig(streamName, [subject])
        {
            Storage = StreamConfigStorage.Memory
        });

        var consumerName = "offload-processor";
        var consumer = await js.CreateOrUpdateConsumerAsync(streamName, new ConsumerConfig(consumerName)
        {
            DurableName = consumerName,
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            AckWait = TimeSpan.FromSeconds(5)
        });

        var bucketName = $"claim-checks-{Guid.NewGuid():N}";
        await objContext.CreateObjectStoreAsync(new NatsObjConfig(bucketName)
        {
            Storage = NatsObjStorageType.Memory
        });

        var serializer = new HybridNatsSerializer();
        await using var bus = new NatsJetStreamBus(js, new ConsoleLogger<NatsJetStreamBus>(), serializer);

        await using var objectStore = new NatsObjectStore(
            objContext,
            bucketName,
            new ConsoleLogger<NatsObjectStore>());

        var offloadOptions = new PayloadOffloadingOptions
        {
            ThresholdBytes = 512,
            ObjectKeyPrefix = "claimcheck"
        };

        var typeAliasRegistry = new MessageTypeAliasRegistry(
            Options.Create(new MessageTypeAliasOptions { RequireExplicitAliases = false }));
        typeAliasRegistry.Register<LargeHostedMessage>(nameof(LargeHostedMessage));

        var offloadingPublisher = new OffloadingJetStreamPublisher(
            bus,
            objectStore,
            serializer,
            typeAliasRegistry,
            Options.Create(offloadOptions),
            new ConsoleLogger<OffloadingJetStreamPublisher>());

        var expectedData = new string('Z', 1024);
        var originalMessage = new LargeHostedMessage("hosted-offload-1", expectedData);

        await offloadingPublisher.PublishAsync(subject, originalMessage, "hosted-offload-msg-1");

        var storedObjects = (await objectStore.ListAsync()).ToList();
        storedObjects.Count.ShouldBe(1, "payload should be offloaded to the object store");

        var dlqPolicyRegistry = new DlqPolicyRegistry(new ConsoleLogger<DlqPolicyRegistry>());
        var poisonHandler = new DefaultDlqPoisonHandler<LargeHostedMessage>(
            bus,
            serializer,
            typeAliasRegistry,
            objectStore: null,
            notificationService: null,
            dlqPolicyRegistry,
            new ConsoleLogger<DefaultDlqPoisonHandler<LargeHostedMessage>>());

        var received = new TaskCompletionSource<LargeHostedMessage>();
        var hydratedObjectKey = new TaskCompletionSource<string?>();

        Func<IJsMessageContext<LargeHostedMessage>, Task> handler = ctx =>
        {
            received.TrySetResult(ctx.Message);
            var claimCheckKey = (ctx as HydratedMessageContext<LargeHostedMessage>)?.ClaimCheckObjectKey;
            hydratedObjectKey.TrySetResult(claimCheckKey);
            return Task.CompletedTask;
        };

        var service = NatsConsumerBackgroundService<LargeHostedMessage>.Create(
            consumer,
            streamName,
            consumerName,
            handler,
            new NatsJSConsumeOpts { MaxMsgs = 1 },
            new ConsoleLogger<NatsConsumerBackgroundService<LargeHostedMessage>>(),
            poisonHandler,
            serializer: serializer,
            objectStore: objectStore,
            offloadingOptions: offloadOptions);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        await service.StartAsync(cts.Token);

        var message = await received.Task.WaitAsync(cts.Token);

        message.ShouldNotBeNull();
        message.Id.ShouldBe(originalMessage.Id);
        message.Data.Length.ShouldBe(expectedData.Length);
        message.Data.ShouldBe(expectedData);

        var capturedObjectKey = await hydratedObjectKey.Task.WaitAsync(cts.Token);
        capturedObjectKey.ShouldNotBeNullOrEmpty(
            "HydratedMessageContext should carry the object-store key used for hydration");

        await Task.Delay(500);

        var objectsAfterAck = (await objectStore.ListAsync()).ToList();
        objectsAfterAck.Count.ShouldBe(1,
            "without ClaimCheckCleanupMiddleware the object store entry should remain after ack");

        await service.StopAsync(CancellationToken.None);
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

using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core;
using FlySwattr.NATS.Core.Configuration;
using FlySwattr.NATS.Core.Serializers;
using FlySwattr.NATS.Core.Services;
using FlySwattr.NATS.Hosting.Services;
using IntegrationTests.Infrastructure;
using MemoryPack;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.KeyValueStore;
using Shouldly;
using TUnit.Core;

namespace IntegrationTests.Core;

/// <summary>
/// Integration tests verifying that <see cref="DlqStoreFailureOptions.BucketName"/>
/// routes DLQ entries to a non-default KV bucket end-to-end.
/// </summary>
[Property("nTag", "Integration")]
public partial class DlqCustomBucketNameTests
{
    [MemoryPackable]
    public partial record CustomDlqTestEvent(string Id, string Payload);

    [Test]
    public async Task DlqStore_CustomBucketName_PersistsEntriesToCorrectBucket()
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
        var kv = new NatsKVContext(js);

        var streamName = "ORDERS_CUSTOM_DLQ";
        var subject = "orders.custom.dlq";
        await js.CreateStreamAsync(new StreamConfig(streamName, [subject])
        {
            Storage = StreamConfigStorage.Memory
        });

        var dlqStreamName = "DLQ_ORDERS_CUSTOM";
        var dlqSubject = "dlq.orders.custom";
        await js.CreateStreamAsync(new StreamConfig(dlqStreamName, [dlqSubject])
        {
            Storage = StreamConfigStorage.Memory
        });

        var consumerName = "orders-custom-processor";
        var maxDeliver = 2;
        var consumer = await js.CreateOrUpdateConsumerAsync(streamName, new ConsumerConfig(consumerName)
        {
            DurableName = consumerName,
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            AckWait = TimeSpan.FromMilliseconds(500),
            MaxDeliver = maxDeliver
        });

        const string customBucketName = "custom-dlq-bucket";
        const string defaultBucketName = "fs-dlq-entries";

        await kv.CreateStoreAsync(new NatsKVConfig(customBucketName)
        {
            Storage = NatsKVStorageType.Memory
        });
        await kv.CreateStoreAsync(new NatsKVConfig(defaultBucketName)
        {
            Storage = NatsKVStorageType.Memory
        });

        var failureOptions = new DlqStoreFailureOptions { BucketName = customBucketName };
        var serializer = new HybridNatsSerializer();
        await using var bus = new NatsJetStreamBus(js, new ConsoleLogger<NatsJetStreamBus>(), serializer);

        var dlqPolicyRegistry = new DlqPolicyRegistry(new ConsoleLogger<DlqPolicyRegistry>());
        dlqPolicyRegistry.Register(streamName, consumerName, new DeadLetterPolicy
        {
            SourceStream = streamName,
            SourceConsumer = consumerName,
            TargetStream = StreamName.From(dlqStreamName),
            TargetSubject = dlqSubject
        });

        var dlqStore = new NatsDlqStore(
            kv,
            new ConsoleLogger<NatsDlqStore>(),
            Options.Create(failureOptions));

        var typeAliasRegistry = new MessageTypeAliasRegistry(
            Options.Create(new MessageTypeAliasOptions { RequireExplicitAliases = false }));
        typeAliasRegistry.Register<CustomDlqTestEvent>(nameof(CustomDlqTestEvent));

        var poisonHandler = new DefaultDlqPoisonHandler<CustomDlqTestEvent>(
            bus,
            serializer,
            typeAliasRegistry,
            objectStore: null,
            notificationService: null,
            dlqPolicyRegistry,
            new ConsoleLogger<DefaultDlqPoisonHandler<CustomDlqTestEvent>>(),
            failureOptions: Options.Create(failureOptions),
            natsOptions: null,
            dlqStore: dlqStore);

        var failureCount = new CountdownEvent(maxDeliver);
        Func<IJsMessageContext<CustomDlqTestEvent>, Task> handler = _ =>
        {
            failureCount.Signal();
            throw new InvalidOperationException("Always fails");
        };

        var service = new NatsConsumerBackgroundService<CustomDlqTestEvent>(
            consumer,
            streamName,
            consumerName,
            handler,
            new NatsJSConsumeOpts { MaxMsgs = 1 },
            new ConsoleLogger<NatsConsumerBackgroundService<CustomDlqTestEvent>>(),
            poisonHandler);

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        await Task.Delay(500);

        await js.PublishAsync(subject, new CustomDlqTestEvent("evt-1", "poison"));

        failureCount.Wait(TimeSpan.FromSeconds(15)).ShouldBeTrue();
        await Task.Delay(2000);

        var remediationService = new NatsDlqRemediationService(
            dlqStore,
            bus,
            serializer,
            typeAliasRegistry,
            new ConsoleLogger<NatsDlqRemediationService>());

        var entries = await remediationService.ListAsync(filterStream: streamName);
        entries.Count.ShouldBeGreaterThanOrEqualTo(1, "custom bucket should contain the DLQ entry");

        var customStore = await kv.GetStoreAsync(customBucketName);
        var customKeyCount = 0;
        await foreach (var _ in customStore.GetKeysAsync())
        {
            customKeyCount++;
        }
        customKeyCount.ShouldBeGreaterThanOrEqualTo(1, "custom bucket should have at least one entry");

        var defaultStore = await kv.GetStoreAsync(defaultBucketName);
        var defaultKeyCount = 0;
        await foreach (var _ in defaultStore.GetKeysAsync())
        {
            defaultKeyCount++;
        }
        defaultKeyCount.ShouldBe(0, "default bucket should remain empty when a custom bucket is configured");

        await cts.CancelAsync();
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

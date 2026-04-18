using System.Collections.Concurrent;
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
using Shouldly;
using TUnit.Core;

namespace IntegrationTests.Hosting;

/// <summary>
/// Documents the three manual-ack patterns that can be used with
/// <see cref="NatsConsumerBackgroundService{T}"/>. The framework always
/// attempts an implicit ack after the handler returns; calls to
/// Ack/Nack/Term inside the handler set the context's acknowledged flag and
/// make the subsequent implicit ack a no-op.
/// </summary>
[Property("nTag", "Hosting")]
public partial class ManualAckPatternsTests
{
    [MemoryPackable]
    public partial record AckTestMessage(int Id, string Content);

    private static DefaultDlqPoisonHandler<AckTestMessage> CreateDisabledPoisonHandler(
        NatsJetStreamBus bus,
        HybridNatsSerializer serializer)
    {
        var typeAliasRegistry = new MessageTypeAliasRegistry(
            Options.Create(new MessageTypeAliasOptions { RequireExplicitAliases = false }));
        typeAliasRegistry.Register<AckTestMessage>(nameof(AckTestMessage));

        var policyRegistry = new DlqPolicyRegistry(new ConsoleLogger<DlqPolicyRegistry>());

        return new DefaultDlqPoisonHandler<AckTestMessage>(
            bus,
            serializer,
            typeAliasRegistry,
            objectStore: null,
            notificationService: null,
            policyRegistry,
            new ConsoleLogger<DefaultDlqPoisonHandler<AckTestMessage>>());
    }

    [Test]
    public async Task Handler_CallingTermAsync_StopsRedeliveryImmediately()
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
        var streamName = "MANUAL_ACK_TERM_TEST";
        var subject = "manual.ack.term";
        await js.CreateStreamAsync(new StreamConfig(streamName, [subject])
        {
            Storage = StreamConfigStorage.Memory
        });

        var consumerName = "term-consumer";
        var consumer = await js.CreateOrUpdateConsumerAsync(streamName, new ConsumerConfig(consumerName)
        {
            DurableName = consumerName,
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            AckWait = TimeSpan.FromSeconds(1),
            MaxDeliver = 5
        });

        var serializer = new HybridNatsSerializer();
        await using var bus = new NatsJetStreamBus(js, new ConsoleLogger<NatsJetStreamBus>(), serializer);
        var poisonHandler = CreateDisabledPoisonHandler(bus, serializer);

        var handlerCalls = 0;
        Func<IJsMessageContext<AckTestMessage>, Task> handler = async ctx =>
        {
            Interlocked.Increment(ref handlerCalls);
            await ctx.TermAsync();
        };

        var service = new NatsConsumerBackgroundService<AckTestMessage>(
            consumer,
            streamName,
            consumerName,
            handler,
            new NatsJSConsumeOpts { MaxMsgs = 1 },
            new ConsoleLogger<NatsConsumerBackgroundService<AckTestMessage>>(),
            poisonHandler);

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        await Task.Delay(500);

        await js.PublishAsync(subject, new AckTestMessage(1, "term-me"));

        await Task.Delay(TimeSpan.FromSeconds(4));

        handlerCalls.ShouldBe(1, "TermAsync should stop redelivery after the first attempt");

        var consumerInfo = await js.GetConsumerAsync(streamName, consumerName);
        consumerInfo.Info.NumAckPending.ShouldBe(0, "no ack-pending after Term");

        await cts.CancelAsync();
        await service.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task Handler_CallingNackAsync_RequeuesMessageWithDelay()
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
        var streamName = "MANUAL_ACK_NACK_TEST";
        var subject = "manual.ack.nack";
        await js.CreateStreamAsync(new StreamConfig(streamName, [subject])
        {
            Storage = StreamConfigStorage.Memory
        });

        var consumerName = "nack-consumer";
        var consumer = await js.CreateOrUpdateConsumerAsync(streamName, new ConsumerConfig(consumerName)
        {
            DurableName = consumerName,
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            AckWait = TimeSpan.FromSeconds(5),
            MaxDeliver = 3
        });

        var serializer = new HybridNatsSerializer();
        await using var bus = new NatsJetStreamBus(js, new ConsoleLogger<NatsJetStreamBus>(), serializer);
        var poisonHandler = CreateDisabledPoisonHandler(bus, serializer);

        var deliveries = new ConcurrentBag<(uint NumDelivered, DateTimeOffset At)>();
        var allDelivered = new CountdownEvent(3);

        Func<IJsMessageContext<AckTestMessage>, Task> handler = async ctx =>
        {
            deliveries.Add((ctx.NumDelivered, DateTimeOffset.UtcNow));

            if (ctx.NumDelivered < 3)
            {
                await ctx.NackAsync(TimeSpan.FromMilliseconds(200));
            }
            else
            {
                await ctx.AckAsync();
            }

            allDelivered.Signal();
        };

        var service = new NatsConsumerBackgroundService<AckTestMessage>(
            consumer,
            streamName,
            consumerName,
            handler,
            new NatsJSConsumeOpts { MaxMsgs = 1 },
            new ConsoleLogger<NatsConsumerBackgroundService<AckTestMessage>>(),
            poisonHandler);

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        await Task.Delay(500);

        await js.PublishAsync(subject, new AckTestMessage(1, "nack-me"));

        var done = allDelivered.Wait(TimeSpan.FromSeconds(15));
        done.ShouldBeTrue("Expected exactly 3 deliveries (nack, nack, ack)");

        var ordered = deliveries.OrderBy(d => d.NumDelivered).ToList();
        ordered.Count.ShouldBe(3);
        ordered[0].NumDelivered.ShouldBe(1u);
        ordered[1].NumDelivered.ShouldBe(2u);
        ordered[2].NumDelivered.ShouldBe(3u);

        var firstGap = ordered[1].At - ordered[0].At;
        firstGap.ShouldBeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(150),
            "NackAsync delay should delay redelivery by roughly the configured interval");

        await cts.CancelAsync();
        await service.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task Handler_ExplicitAckAsync_IsIdempotentWithFrameworkAck()
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
        var streamName = "MANUAL_ACK_EXPLICIT_TEST";
        var subject = "manual.ack.explicit";
        await js.CreateStreamAsync(new StreamConfig(streamName, [subject])
        {
            Storage = StreamConfigStorage.Memory
        });

        var consumerName = "explicit-ack-consumer";
        var consumer = await js.CreateOrUpdateConsumerAsync(streamName, new ConsumerConfig(consumerName)
        {
            DurableName = consumerName,
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            AckWait = TimeSpan.FromSeconds(2),
            MaxDeliver = 3
        });

        var serializer = new HybridNatsSerializer();
        await using var bus = new NatsJetStreamBus(js, new ConsoleLogger<NatsJetStreamBus>(), serializer);
        var poisonHandler = CreateDisabledPoisonHandler(bus, serializer);

        var handlerCalls = 0;
        Func<IJsMessageContext<AckTestMessage>, Task> handler = async ctx =>
        {
            Interlocked.Increment(ref handlerCalls);
            await ctx.AckAsync();
        };

        var service = new NatsConsumerBackgroundService<AckTestMessage>(
            consumer,
            streamName,
            consumerName,
            handler,
            new NatsJSConsumeOpts { MaxMsgs = 1 },
            new ConsoleLogger<NatsConsumerBackgroundService<AckTestMessage>>(),
            poisonHandler);

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        await Task.Delay(500);

        await js.PublishAsync(subject, new AckTestMessage(1, "ack-me"));

        await Task.Delay(TimeSpan.FromSeconds(5));

        handlerCalls.ShouldBe(1, "Explicit ack + framework implicit ack must only deliver the message once");

        var consumerInfo = await js.GetConsumerAsync(streamName, consumerName);
        consumerInfo.Info.NumAckPending.ShouldBe(0);
        consumerInfo.Info.NumPending.ShouldBe(0UL);

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

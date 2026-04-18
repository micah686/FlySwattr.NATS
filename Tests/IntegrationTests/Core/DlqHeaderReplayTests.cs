using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core;
using FlySwattr.NATS.Core.Configuration;
using FlySwattr.NATS.Core.Serializers;
using FlySwattr.NATS.Core.Services;
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
/// Integration tests that verify custom transport headers survive the DLQ
/// round-trip and that the remediation service strips headers on
/// <see cref="NatsDlqRemediationService"/>'s replay-blocked list.
/// </summary>
[Property("nTag", "Integration")]
public partial class DlqHeaderReplayTests
{
    [MemoryPackable]
    public partial record ReplayHeaderTestEvent(string Id, string Payload);

    [Test]
    public async Task DlqReplay_CustomHeaders_ArePropagatedToReplayedMessage()
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

        var streamName = "REPLAY_HEADERS_TEST";
        var subject = "replay.headers.test";
        await js.CreateStreamAsync(new StreamConfig(streamName, [subject])
        {
            Storage = StreamConfigStorage.Memory
        });

        var consumerName = "replay-consumer";
        var consumer = await js.CreateOrUpdateConsumerAsync(streamName, new ConsumerConfig(consumerName)
        {
            DurableName = consumerName,
            AckPolicy = ConsumerConfigAckPolicy.Explicit
        });

        await kv.CreateStoreAsync(new NatsKVConfig("fs-dlq-entries")
        {
            Storage = NatsKVStorageType.Memory
        });

        var serializer = new HybridNatsSerializer();
        await using var bus = new NatsJetStreamBus(js, new ConsoleLogger<NatsJetStreamBus>(), serializer);

        var dlqStore = new NatsDlqStore(kv, new ConsoleLogger<NatsDlqStore>());
        var typeAliasRegistry = new MessageTypeAliasRegistry(
            Options.Create(new MessageTypeAliasOptions { RequireExplicitAliases = false }));
        typeAliasRegistry.Register<ReplayHeaderTestEvent>(nameof(ReplayHeaderTestEvent));

        var remediationService = new NatsDlqRemediationService(
            dlqStore,
            bus,
            serializer,
            typeAliasRegistry,
            new ConsoleLogger<NatsDlqRemediationService>());

        var originalMessage = new ReplayHeaderTestEvent("evt-1", "hello");
        using var buffer = new MemoryStream();
        var bufferWriter = new System.Buffers.ArrayBufferWriter<byte>();
        serializer.Serialize(bufferWriter, originalMessage);

        var entry = new DlqMessageEntry
        {
            Id = $"{streamName}.{consumerName}.42",
            OriginalStream = streamName,
            OriginalConsumer = consumerName,
            OriginalSubject = subject,
            OriginalSequence = 42,
            DeliveryCount = 3,
            StoredAt = DateTimeOffset.UtcNow,
            ErrorReason = "Simulated failure",
            Payload = bufferWriter.WrittenMemory.ToArray(),
            PayloadEncoding = serializer.GetContentType<ReplayHeaderTestEvent>(),
            OriginalMessageType = nameof(ReplayHeaderTestEvent),
            SerializerType = serializer.GetType().FullName,
            OriginalHeaders = new Dictionary<string, string>
            {
                ["X-Correlation-Id"] = "abc-123",
                ["X-Custom-Tag"] = "important",
                ["Nats-Msg-Id"] = "original-id"
            }
        };
        await dlqStore.StoreAsync(entry);

        using var receivedCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var tcs = new TaskCompletionSource<MessageHeaders>();

        var consumeTask = Task.Run(async () =>
        {
            await foreach (var msg in consumer.ConsumeAsync<ReplayHeaderTestEvent>(
                               opts: new NatsJSConsumeOpts { MaxMsgs = 1 },
                               cancellationToken: receivedCts.Token))
            {
                var headers = msg.Headers != null
                    ? new MessageHeaders(msg.Headers.ToDictionary(k => k.Key, k => k.Value.ToString()))
                    : MessageHeaders.Empty;

                await msg.AckAsync(cancellationToken: receivedCts.Token);
                tcs.TrySetResult(headers);
                break;
            }
        }, receivedCts.Token);

        await Task.Delay(500);

        var replayResult = await remediationService.ReplayAsync(entry.Id);
        replayResult.Success.ShouldBeTrue();

        var receivedHeaders = await tcs.Task.WaitAsync(receivedCts.Token);

        receivedHeaders.Headers.ShouldContainKey("X-Correlation-Id");
        receivedHeaders.Headers["X-Correlation-Id"].ShouldBe("abc-123");
        receivedHeaders.Headers.ShouldContainKey("X-Custom-Tag");
        receivedHeaders.Headers["X-Custom-Tag"].ShouldBe("important");

        if (receivedHeaders.Headers.TryGetValue("Nats-Msg-Id", out var msgId))
        {
            msgId.ShouldNotBe("original-id",
                "Original Nats-Msg-Id must be stripped and replaced by the replay publisher's message id");
        }

        await receivedCts.CancelAsync();
        try { await consumeTask; } catch { /* ignore cancellation */ }
    }

    [Test]
    public async Task DlqReplay_BlockedHeaders_AreStrippedFromReplay()
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

        var streamName = "REPLAY_BLOCKED_HEADERS_TEST";
        var subject = "replay.blocked.test";
        await js.CreateStreamAsync(new StreamConfig(streamName, [subject])
        {
            Storage = StreamConfigStorage.Memory
        });

        var consumerName = "replay-blocked-consumer";
        var consumer = await js.CreateOrUpdateConsumerAsync(streamName, new ConsumerConfig(consumerName)
        {
            DurableName = consumerName,
            AckPolicy = ConsumerConfigAckPolicy.Explicit
        });

        await kv.CreateStoreAsync(new NatsKVConfig("fs-dlq-entries")
        {
            Storage = NatsKVStorageType.Memory
        });

        var serializer = new HybridNatsSerializer();
        await using var bus = new NatsJetStreamBus(js, new ConsoleLogger<NatsJetStreamBus>(), serializer);

        var dlqStore = new NatsDlqStore(kv, new ConsoleLogger<NatsDlqStore>());
        var typeAliasRegistry = new MessageTypeAliasRegistry(
            Options.Create(new MessageTypeAliasOptions { RequireExplicitAliases = false }));
        typeAliasRegistry.Register<ReplayHeaderTestEvent>(nameof(ReplayHeaderTestEvent));

        var remediationService = new NatsDlqRemediationService(
            dlqStore,
            bus,
            serializer,
            typeAliasRegistry,
            new ConsoleLogger<NatsDlqRemediationService>());

        var originalMessage = new ReplayHeaderTestEvent("evt-2", "blocked-headers-test");
        var bufferWriter = new System.Buffers.ArrayBufferWriter<byte>();
        serializer.Serialize(bufferWriter, originalMessage);

        var entry = new DlqMessageEntry
        {
            Id = $"{streamName}.{consumerName}.7",
            OriginalStream = streamName,
            OriginalConsumer = consumerName,
            OriginalSubject = subject,
            OriginalSequence = 7,
            DeliveryCount = 3,
            StoredAt = DateTimeOffset.UtcNow,
            ErrorReason = "Simulated failure",
            Payload = bufferWriter.WrittenMemory.ToArray(),
            PayloadEncoding = serializer.GetContentType<ReplayHeaderTestEvent>(),
            OriginalMessageType = nameof(ReplayHeaderTestEvent),
            SerializerType = serializer.GetType().FullName,
            OriginalHeaders = new Dictionary<string, string>
            {
                ["traceparent"] = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
                ["tracestate"] = "vendor=stale",
                ["X-ClaimCheck-Ref"] = "objstore://stale-key",
                ["X-ClaimCheck-Type"] = "StaleType",
                ["X-FlySwattr-Version"] = "0",
                ["X-FlySwattr-SchemaHash"] = "deadbeef",
                ["X-Business-Header"] = "preserved"
            }
        };
        await dlqStore.StoreAsync(entry);

        using var receivedCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var tcs = new TaskCompletionSource<MessageHeaders>();

        var consumeTask = Task.Run(async () =>
        {
            await foreach (var msg in consumer.ConsumeAsync<ReplayHeaderTestEvent>(
                               opts: new NatsJSConsumeOpts { MaxMsgs = 1 },
                               cancellationToken: receivedCts.Token))
            {
                var headers = msg.Headers != null
                    ? new MessageHeaders(msg.Headers.ToDictionary(k => k.Key, k => k.Value.ToString()))
                    : MessageHeaders.Empty;

                await msg.AckAsync(cancellationToken: receivedCts.Token);
                tcs.TrySetResult(headers);
                break;
            }
        }, receivedCts.Token);

        await Task.Delay(500);

        var replayResult = await remediationService.ReplayAsync(entry.Id);
        replayResult.Success.ShouldBeTrue();

        var receivedHeaders = await tcs.Task.WaitAsync(receivedCts.Token);

        receivedHeaders.Headers.ShouldNotContainKey("X-ClaimCheck-Ref");
        receivedHeaders.Headers.ShouldNotContainKey("X-ClaimCheck-Type");
        receivedHeaders.Headers.ShouldNotContainKey("X-FlySwattr-SchemaHash");

        if (receivedHeaders.Headers.TryGetValue("traceparent", out var tracePrefix))
        {
            tracePrefix.ShouldNotBe("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
                "The stale traceparent must not be propagated to the replayed message");
        }

        receivedHeaders.Headers.ShouldContainKey("X-Business-Header");
        receivedHeaders.Headers["X-Business-Header"].ShouldBe("preserved");

        await receivedCts.CancelAsync();
        try { await consumeTask; } catch { /* ignore cancellation */ }
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

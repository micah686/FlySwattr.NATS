using System.Buffers;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core;
using FlySwattr.NATS.Core.Serializers;
using FlySwattr.NATS.Hosting.Services;
using IntegrationTests.Infrastructure;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.Serializers.Json;
using Shouldly;
using TUnit.Core;

namespace IntegrationTests.Hosting;

[Property("nTag", "Hosting")]
public class NatsConsumerBackgroundServiceTests
{
    public record TestMessage(int Id, string Content);

    [Test]
    public async Task BackgroundService_ShouldHandlePoisonMessage_AfterMaxRetries()
    {
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts { Url = fixture.ConnectionString, SerializerRegistry = NatsJsonSerializerRegistry.Default };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();
        var js = new NatsJSContext(conn);

        // 1. Setup Streams
        var streamName = "POISON_TEST_STREAM";
        var subject = "poison.test";
        await js.CreateStreamAsync(new StreamConfig(streamName, new[] { subject }));

        var dlqStreamName = "DLQ_STREAM";
        var dlqSubject = "dlq.poison";
        await js.CreateStreamAsync(new StreamConfig(dlqStreamName, new[] { dlqSubject }));

        // 2. Create Consumer with MaxDeliver and Short AckWait
        var consumerName = "PoisonConsumer";
        var maxDeliver = 3;
        var ackWait = TimeSpan.FromMilliseconds(500); // Fast retry

        var consumer = await js.CreateOrUpdateConsumerAsync(streamName, new ConsumerConfig(consumerName)
        {
            DurableName = consumerName,
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            AckWait = ackWait,
            MaxDeliver = maxDeliver
        });

        // 3. Setup Dependencies
        var serializer = new HybridNatsSerializer();
        var dlqPolicyRegistry = new DlqPolicyRegistry(new ConsoleLogger<DlqPolicyRegistry>());
        var dlqPolicy = new DeadLetterPolicy 
        { 
            SourceStream = streamName, 
            SourceConsumer = consumerName, 
            TargetStream = StreamName.From(dlqStreamName), 
            TargetSubject = dlqSubject 
        };
        dlqPolicyRegistry.Register(streamName, consumerName, dlqPolicy);

        var bus = new NatsJetStreamBus(js, new ConsoleLogger<NatsJetStreamBus>(), serializer);
        
        var poisonHandler = new DefaultDlqPoisonHandler<TestMessage>(
            bus, 
            serializer, 
            null, 
            null, 
            dlqPolicyRegistry, 
            new ConsoleLogger<DefaultDlqPoisonHandler<TestMessage>>());

        // 4. Create Service with Failing Handler
        var handlerCountdown = new CountdownEvent(maxDeliver);
        Func<IJsMessageContext<TestMessage>, Task> handler = ctx =>
        {
            Console.WriteLine($"[DEBUG] Handler called. NumDelivered: {ctx.NumDelivered}");
            handlerCountdown.Signal();
            throw new Exception("Handler Failure");
        };

        var service = new NatsConsumerBackgroundService<TestMessage>(
            consumer,
            streamName,
            consumerName,
            handler,
            new NatsJSConsumeOpts { MaxMsgs = 1 },
            new ConsoleLogger<NatsConsumerBackgroundService<TestMessage>>(),
            poisonHandler
        );

        // 5. Start Service
        var cts = new CancellationTokenSource();
        var serviceTask = service.StartAsync(cts.Token);

        // Give it a moment to start the consume loop
        await Task.Delay(1000);

        // 6. Publish Poison Message
        await js.PublishAsync(subject, new TestMessage(1, "Bad Payload"));

        // 7. Wait for handler attempts
        bool handlerDone = handlerCountdown.Wait(TimeSpan.FromSeconds(20));
        handlerDone.ShouldBeTrue($"Handler should be called {maxDeliver} times, but was called {maxDeliver - handlerCountdown.CurrentCount} times");

        // 8. Wait/Poll for DLQ Message
        var dlqStreamInfo = await js.GetStreamAsync(dlqStreamName);
        Console.WriteLine($"[DEBUG] DLQ Stream Messages: {dlqStreamInfo.Info.State.Messages}");

        var dlqConsumer = await js.CreateOrUpdateConsumerAsync(dlqStreamName, new ConsumerConfig("DlqWatcher") 
        { 
            AckPolicy = ConsumerConfigAckPolicy.Explicit 
        });

        // Try raw fetch
        var start = DateTime.UtcNow;
        DlqMessage? dlqMsg = null;
        while (DateTime.UtcNow - start < TimeSpan.FromSeconds(10))
        {
            await foreach (var msg in dlqConsumer.FetchAsync<byte[]>(new NatsJSFetchOpts { MaxMsgs = 1, Expires = TimeSpan.FromSeconds(1) }))
            {
                if (msg.Data != null)
                {
                    var json = System.Text.Encoding.UTF8.GetString(msg.Data);
                    Console.WriteLine($"[DEBUG] Raw DLQ Message: {json}");
                    dlqMsg = System.Text.Json.JsonSerializer.Deserialize<DlqMessage>(msg.Data, new System.Text.Json.JsonSerializerOptions { PropertyNamingPolicy = System.Text.Json.JsonNamingPolicy.CamelCase });
                    await msg.AckAsync();
                    break;
                }
            }
            if (dlqMsg != null) break;
            await Task.Delay(500);
        }

        // 9. Assertions
        if (dlqMsg == null)
        {
            dlqStreamInfo = await js.GetStreamAsync(dlqStreamName);
            Console.WriteLine($"[DEBUG] DLQ Stream Messages after poll failure: {dlqStreamInfo.Info.State.Messages}");
        }
        dlqMsg.ShouldNotBeNull();
        dlqMsg.OriginalStream.ShouldBe(streamName);
        dlqMsg.DeliveryCount.ShouldBe(maxDeliver);
        dlqMsg.ErrorReason.ShouldNotBeNull();
        dlqMsg.ErrorReason.ShouldContain("Handler Failure");

        await service.StopAsync(CancellationToken.None);
    }

    private async Task<DlqMessage?> PollForDlqMessageAsync(INatsJSConsumer consumer, TimeSpan timeout)
    {
        var start = DateTime.UtcNow;
        var fetchOpts = new NatsJSFetchOpts { MaxMsgs = 1, Expires = TimeSpan.FromSeconds(1) };

        while (DateTime.UtcNow - start < timeout)
        {
            try 
            {
                await foreach (var msg in consumer.FetchAsync<DlqMessage>(fetchOpts))
                {
                    if (msg.Data != null)
                    {
                        await msg.AckAsync();
                        return msg.Data;
                    }
                }
            }
            catch (Exception ex) 
            { 
                Console.WriteLine($"[DEBUG] Fetch error: {ex.Message}");
            }
            
            await Task.Delay(500);
        }
        return null;
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
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Serializers;
using FlySwattr.NATS.Hosting.Services;
using FlySwattr.NATS.Resilience.Builders;
using IntegrationTests.Infrastructure;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.Serializers.Json;
using Polly;
using Polly.CircuitBreaker;
using Shouldly;
using TUnit.Core;

namespace IntegrationTests.Resilience;

[Property("nTag", "Resilience")]
public class CircuitBreakerIntegrationTests
{
    public record TestMessage(int Id);

    [Test]
    public async Task CircuitBreaker_ShouldOpen_AfterFailures()
    {
        // 1. Setup
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts { Url = fixture.ConnectionString, SerializerRegistry = NatsJsonSerializerRegistry.Default };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();
        var js = new NatsJSContext(conn);

        var streamName = "CB_STREAM";
        var subject = "cb.test";
        await js.CreateStreamAsync(new StreamConfig(streamName, new[] { subject }));

        var consumerName = "CBConsumer";
        var consumer = await js.CreateOrUpdateConsumerAsync(streamName, new ConsumerConfig(consumerName)
        {
            DurableName = consumerName,
            AckPolicy = ConsumerConfigAckPolicy.Explicit
        });

        // 2. Setup Resilience
        // Use very sensitive settings for test speed
        var cbOptions = new ConsumerCircuitBreakerOptions
        {
            FailureRatio = 0.1, // 10% failure trips
            MinimumThroughput = 3, // Trip after 3 calls (if all fail)
            SamplingDuration = TimeSpan.FromSeconds(10),
            BreakDuration = TimeSpan.FromSeconds(5) // 5s break
        };

        var loggerFactory = LoggerFactory.Create(b => b.AddConsole());
        var builderLogger = loggerFactory.CreateLogger<HierarchicalResilienceBuilder>();
        
        await using var builder = new HierarchicalResilienceBuilder(builderLogger);
        var globalPipeline = new ResiliencePipelineBuilder().Build(); // Empty global
        var pipeline = builder.GetPipeline(consumerName, globalPipeline, cbOptions);

        // 3. Setup Consumer Service
        // We track attempts. First 3 fail. 4th should be blocked by CB.
        int attempts = 0;
        var tcsCircuitOpen = new TaskCompletionSource<bool>();

        Func<IJsMessageContext<TestMessage>, Task> handler = async ctx =>
        {
            var count = Interlocked.Increment(ref attempts);
            Console.WriteLine($"[Test] Handler attempt {count}");
            
            if (count <= 3)
            {
                throw new Exception("Boom");
            }
            
            await ctx.AckAsync();
        };

        // We can't easily hook into "OnOpened" of the *inner* strategy created by builder from here,
        // unless we modify builder or inspect pipeline.
        // But we can infer it opens if we see BrokenCircuitException?
        // Wait, NatsConsumerBackgroundService catches exceptions.
        // It logs "Worker crashed" or "Handler failed".
        // If CB is open, pipeline throws BrokenCircuitException.
        // BackgroundService catches it.
        // We can verify that we DON'T process more messages for a while.
        
        // Actually, to verify CB State, we can inspect the pipeline if we could, but we can't.
        // We will rely on behavior:
        // Publish 5 messages.
        // Expect 3 handler calls (failures).
        // Then silence (CB Open).
        
        var service = new NatsConsumerBackgroundService<TestMessage>(
            consumer,
            streamName,
            consumerName,
            handler,
            new NatsJSConsumeOpts { MaxMsgs = 1 },
            loggerFactory.CreateLogger<NatsConsumerBackgroundService<TestMessage>>(),
            poisonHandler: null!, // Not testing poison logic here, let it crash/retry
            resiliencePipeline: pipeline
        );

        var cts = new CancellationTokenSource();
        var serviceTask = service.StartAsync(cts.Token);

        // 4. Publish messages
        for (int i = 0; i < 10; i++)
        {
            await js.PublishAsync(subject, new TestMessage(i));
        }

        // 5. Wait for failures to trip
        // We expect 3 failed attempts fairly quickly.
        await AwaitConditionAsync(() => attempts >= 3, TimeSpan.FromSeconds(5));
        
        // Record attempts at this point
        int attemptsAtTrip = attempts;
        
        // 6. Verify Circuit is Open
        // If Circuit is open, subsequent executions should fail fast with BrokenCircuitException
        // The background service loop will catch this and log/retry loop.
        // Effectively, the 'attempts' counter (handler invocations) should PAUSE.
        
        await Task.Delay(2000); // Wait within BreakDuration (5s)
        
        int attemptsDuringBreak = attempts;
        
        // Assert that no new attempts happened (or very few if races)
        // With CB open, handler is NOT called.
        attemptsDuringBreak.ShouldBe(attemptsAtTrip);

        Console.WriteLine($"[Test] Circuit Breaker confirmed open. Attempts halted at {attemptsAtTrip}.");

        await service.StopAsync(CancellationToken.None);
    }

    private async Task AwaitConditionAsync(Func<bool> condition, TimeSpan timeout)
    {
        var start = DateTime.UtcNow;
        while ((DateTime.UtcNow - start) < timeout)
        {
            if (condition()) return;
            await Task.Delay(100);
        }
    }
}

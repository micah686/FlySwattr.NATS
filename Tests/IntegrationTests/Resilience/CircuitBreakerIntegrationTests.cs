using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Hosting.Services;
using IntegrationTests.Infrastructure;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.Serializers.Json;
using NSubstitute;
using Polly;
using Polly.CircuitBreaker;
using Shouldly;
using TUnit.Core;

namespace IntegrationTests.Resilience;

/// <summary>
/// Integration tests for circuit breaker behavior in the consumer service.
/// These tests focus on HOW the consumer responds to an open circuit,
/// NOT on verifying that N failures trip the circuit (which is Polly's responsibility).
/// </summary>
[Property("nTag", "Resilience")]
public class CircuitBreakerIntegrationTests
{
    public record TestMessage(int Id);

    /// <summary>
    /// Verifies that when a message is processed through an OPEN circuit breaker,
    /// the BrokenCircuitException is caught and routed to the poison handler
    /// instead of invoking the handler.
    /// </summary>
    [Test]
    public async Task Consumer_ShouldRouteToPoisonHandler_WhenCircuitIsOpen()
    {
        // Arrange: Setup NATS infrastructure
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts { Url = fixture.ConnectionString, SerializerRegistry = NatsJsonSerializerRegistry.Default };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();
        var js = new NatsJSContext(conn);

        var streamName = "CB_OPEN_TEST";
        var subject = "cb.open.test";
        await js.CreateStreamAsync(new StreamConfig(streamName, new[] { subject }));

        var consumerName = "CBOpenConsumer";
        var consumer = await js.CreateOrUpdateConsumerAsync(streamName, new ConsumerConfig(consumerName)
        {
            DurableName = consumerName,
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            MaxDeliver = 3
        });

        // Create a pre-opened circuit breaker pipeline
        // This is the key difference: we FORCE the circuit open instead of triggering it via failures
        var preOpenedPipeline = new ResiliencePipelineBuilder()
            .AddCircuitBreaker(new CircuitBreakerStrategyOptions
            {
                FailureRatio = 0.5, // Trip after 50% failures
                MinimumThroughput = 2, // Polly requires minimum of 2
                SamplingDuration = TimeSpan.FromMinutes(1),
                BreakDuration = TimeSpan.FromMinutes(5), // Long break to stay open during test
                ShouldHandle = new PredicateBuilder().Handle<Exception>()
            })
            .Build();

        // Trip the circuit by executing 2 failures (MinimumThroughput = 2)
        await TripCircuitAsync(preOpenedPipeline);

        // Verify circuit is now open
        var circuitIsOpen = false;
        try
        {
            await preOpenedPipeline.ExecuteAsync(async _ => await Task.CompletedTask);
        }
        catch (BrokenCircuitException)
        {
            circuitIsOpen = true;
        }
        circuitIsOpen.ShouldBeTrue("Circuit should be open after initial failures");

        // Setup tracking
        var handlerInvoked = false;
        var poisonHandlerCalled = new TaskCompletionSource<(BrokenCircuitException Exception, IJsMessageContext<TestMessage> Context)>();
        
        Func<IJsMessageContext<TestMessage>, Task> handler = _ =>
        {
            handlerInvoked = true;
            return Task.CompletedTask;
        };

        // Create a mock poison handler that captures BrokenCircuitException
        var poisonHandler = Substitute.For<IPoisonMessageHandler<TestMessage>>();
        poisonHandler.HandleAsync(
                Arg.Any<IJsMessageContext<TestMessage>>(),
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<long>(),
                Arg.Any<Exception>(),
                Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var ex = callInfo.ArgAt<Exception>(4);
                if (ex is BrokenCircuitException bce)
                {
                    var ctx = callInfo.ArgAt<IJsMessageContext<TestMessage>>(0);
                    poisonHandlerCalled.TrySetResult((bce, ctx));
                }
                return Task.CompletedTask;
            });

        var loggerFactory = LoggerFactory.Create(b => b.AddConsole().SetMinimumLevel(LogLevel.Debug));

        var service = new NatsConsumerBackgroundService<TestMessage>(
            consumer,
            streamName,
            consumerName,
            handler,
            new NatsJSConsumeOpts { MaxMsgs = 1 },
            loggerFactory.CreateLogger<NatsConsumerBackgroundService<TestMessage>>(),
            poisonHandler: poisonHandler,
            resiliencePipeline: preOpenedPipeline
        );

        using var cts = new CancellationTokenSource();
        _ = service.StartAsync(cts.Token);

        // Act: Publish a message (it should hit the open circuit)
        await js.PublishAsync(subject, new TestMessage(42));

        // Assert: Wait for poison handler to be called with BrokenCircuitException
        var result = await poisonHandlerCalled.Task.WaitAsync(TimeSpan.FromSeconds(10));

        result.Exception.ShouldBeOfType<BrokenCircuitException>();
        result.Context.Message.Id.ShouldBe(42);
        handlerInvoked.ShouldBeFalse("Handler should NOT be invoked when circuit is open");

        await service.StopAsync(CancellationToken.None);
    }

    /// <summary>
    /// Verifies that when the circuit is open, messages are NAKed (via poison handler)
    /// and become available for redelivery after the break duration expires.
    /// </summary>
    [Test]
    public async Task Consumer_ShouldProcessMessage_AfterCircuitCloses()
    {
        // Arrange: Setup NATS infrastructure
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts { Url = fixture.ConnectionString, SerializerRegistry = NatsJsonSerializerRegistry.Default };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();
        var js = new NatsJSContext(conn);

        var streamName = "CB_RECOVERY_TEST";
        var subject = "cb.recovery.test";
        await js.CreateStreamAsync(new StreamConfig(streamName, new[] { subject }));

        var consumerName = "CBRecoveryConsumer";
        var consumer = await js.CreateOrUpdateConsumerAsync(streamName, new ConsumerConfig(consumerName)
        {
            DurableName = consumerName,
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            MaxDeliver = 10 // Allow redeliveries
        });

        // Create a circuit breaker with a SHORT break duration for test speed
        var breakDuration = TimeSpan.FromSeconds(2);
        var pipeline = new ResiliencePipelineBuilder()
            .AddCircuitBreaker(new CircuitBreakerStrategyOptions
            {
                FailureRatio = 0.5,
                MinimumThroughput = 2,
                SamplingDuration = TimeSpan.FromMinutes(1),
                BreakDuration = breakDuration,
                ShouldHandle = new PredicateBuilder().Handle<Exception>()
            })
            .Build();

        // Trip the circuit by executing 2 failures (MinimumThroughput = 2)
        await TripCircuitAsync(pipeline);

        // Track handler invocations
        var successfullyProcessed = new TaskCompletionSource<TestMessage>();
        var brokenCircuitExceptions = 0;

        Func<IJsMessageContext<TestMessage>, Task> handler = ctx =>
        {
            successfullyProcessed.TrySetResult(ctx.Message);
            return Task.CompletedTask;
        };

        // Poison handler that NAKs with short delay to enable redelivery
        var poisonHandler = Substitute.For<IPoisonMessageHandler<TestMessage>>();
        poisonHandler.HandleAsync(
                Arg.Any<IJsMessageContext<TestMessage>>(),
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<long>(),
                Arg.Any<Exception>(),
                Arg.Any<CancellationToken>())
            .Returns(async callInfo =>
            {
                var ex = callInfo.ArgAt<Exception>(4);
                if (ex is BrokenCircuitException)
                {
                    Interlocked.Increment(ref brokenCircuitExceptions);
                    var ctx = callInfo.ArgAt<IJsMessageContext<TestMessage>>(0);
                    // NAK with short delay to allow redelivery after circuit closes
                    await ctx.NackAsync(TimeSpan.FromMilliseconds(500));
                }
            });

        var loggerFactory = LoggerFactory.Create(b => b.AddConsole().SetMinimumLevel(LogLevel.Debug));

        var service = new NatsConsumerBackgroundService<TestMessage>(
            consumer,
            streamName,
            consumerName,
            handler,
            new NatsJSConsumeOpts { MaxMsgs = 1 },
            loggerFactory.CreateLogger<NatsConsumerBackgroundService<TestMessage>>(),
            poisonHandler: poisonHandler,
            resiliencePipeline: pipeline
        );

        using var cts = new CancellationTokenSource();
        _ = service.StartAsync(cts.Token);

        // Act: Publish message while circuit is open
        await js.PublishAsync(subject, new TestMessage(99));

        // Assert: Message should eventually be processed after circuit closes
        var processed = await successfullyProcessed.Task.WaitAsync(breakDuration + TimeSpan.FromSeconds(5));
        
        processed.Id.ShouldBe(99);
        brokenCircuitExceptions.ShouldBeGreaterThan(0, "Should have hit BrokenCircuitException at least once");

        await service.StopAsync(CancellationToken.None);
    }

    /// <summary>
    /// Verifies that when the circuit breaker pipeline throws BrokenCircuitException,
    /// the consumer service does NOT crash and continues its consume loop.
    /// </summary>
    [Test]
    public async Task Consumer_ShouldContinueConsuming_WhenCircuitOpens()
    {
        // Arrange
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts { Url = fixture.ConnectionString, SerializerRegistry = NatsJsonSerializerRegistry.Default };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();
        var js = new NatsJSContext(conn);

        var streamName = "CB_CONTINUE_TEST";
        var subject = "cb.continue.test";
        await js.CreateStreamAsync(new StreamConfig(streamName, new[] { subject }));

        var consumerName = "CBContinueConsumer";
        var consumer = await js.CreateOrUpdateConsumerAsync(streamName, new ConsumerConfig(consumerName)
        {
            DurableName = consumerName,
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            MaxDeliver = 5
        });

        // Create pre-opened circuit
        var pipeline = new ResiliencePipelineBuilder()
            .AddCircuitBreaker(new CircuitBreakerStrategyOptions
            {
                FailureRatio = 0.5,
                MinimumThroughput = 2,
                SamplingDuration = TimeSpan.FromMinutes(1),
                BreakDuration = TimeSpan.FromMinutes(10), // Stay open
                ShouldHandle = new PredicateBuilder().Handle<Exception>()
            })
            .Build();

        // Trip circuit by executing 2 failures (MinimumThroughput = 2)
        await TripCircuitAsync(pipeline);

        var messagesReceived = 0;
        var poisonHandlerCalls = 0;

        Func<IJsMessageContext<TestMessage>, Task> handler = _ =>
        {
            Interlocked.Increment(ref messagesReceived);
            return Task.CompletedTask;
        };

        var poisonHandler = Substitute.For<IPoisonMessageHandler<TestMessage>>();
        poisonHandler.HandleAsync(
                Arg.Any<IJsMessageContext<TestMessage>>(),
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<long>(),
                Arg.Any<Exception>(),
                Arg.Any<CancellationToken>())
            .Returns(async callInfo =>
            {
                Interlocked.Increment(ref poisonHandlerCalls);
                var ctx = callInfo.ArgAt<IJsMessageContext<TestMessage>>(0);
                await ctx.TermAsync(); // Terminate to prevent infinite redelivery
            });

        var loggerFactory = LoggerFactory.Create(b => b.AddConsole().SetMinimumLevel(LogLevel.Debug));

        var service = new NatsConsumerBackgroundService<TestMessage>(
            consumer,
            streamName,
            consumerName,
            handler,
            new NatsJSConsumeOpts { MaxMsgs = 1 },
            loggerFactory.CreateLogger<NatsConsumerBackgroundService<TestMessage>>(),
            poisonHandler: poisonHandler,
            resiliencePipeline: pipeline
        );

        using var cts = new CancellationTokenSource();
        _ = service.StartAsync(cts.Token);

        // Act: Publish multiple messages
        for (int i = 0; i < 3; i++)
        {
            await js.PublishAsync(subject, new TestMessage(i));
        }

        // Wait for all messages to be handled by poison handler
        await AwaitConditionAsync(() => poisonHandlerCalls >= 3, TimeSpan.FromSeconds(10));

        // Assert: Service is still running and processed all messages (via poison handler)
        poisonHandlerCalls.ShouldBeGreaterThanOrEqualTo(3);
        messagesReceived.ShouldBe(0, "Handler should never be invoked when circuit is open");

        await service.StopAsync(CancellationToken.None);
    }

    private static async Task AwaitConditionAsync(Func<bool> condition, TimeSpan timeout)
    {
        var start = DateTime.UtcNow;
        while ((DateTime.UtcNow - start) < timeout)
        {
            if (condition()) return;
            await Task.Delay(100);
        }
        throw new TimeoutException($"Condition not met within {timeout}");
    }

    /// <summary>
    /// Helper to trip a circuit breaker by executing enough failures to meet MinimumThroughput.
    /// </summary>
    private static async Task TripCircuitAsync(ResiliencePipeline pipeline)
    {
        // Execute 2 failures to meet MinimumThroughput = 2
        for (int i = 0; i < 2; i++)
        {
            try
            {
                await pipeline.ExecuteAsync(async _ =>
                {
                    await Task.CompletedTask;
                    throw new InvalidOperationException("Trip the circuit");
                });
            }
            catch (BrokenCircuitException)
            {
                // Circuit already open, we're done
                return;
            }
            catch
            {
                // Expected failure
            }
        }
    }
}

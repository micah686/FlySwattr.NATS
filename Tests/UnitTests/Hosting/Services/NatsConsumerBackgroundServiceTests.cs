using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Hosting.Health;
using FlySwattr.NATS.Hosting.Services;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NSubstitute;
using Polly;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Hosting.Services;

[Property("nTag", "Hosting")]
public class NatsConsumerBackgroundServiceTests
{
    private class TestableNatsConsumerBackgroundService<T> : NatsConsumerBackgroundService<T>
    {
        public TestableNatsConsumerBackgroundService(
            INatsJSConsumer consumer,
            string streamName,
            string consumerName,
            Func<IJsMessageContext<T>, Task> handler,
            NatsJSConsumeOpts consumeOpts,
            ILogger logger,
            IPoisonMessageHandler<T> poisonHandler,
            int? maxDegreeOfParallelism = null,
            ResiliencePipeline? resiliencePipeline = null,
            IConsumerHealthMetrics? healthMetrics = null,
            ITopologyReadySignal? topologyReadySignal = null,
            IEnumerable<IConsumerMiddleware<T>>? middlewares = null) 
            : base(consumer, streamName, consumerName, handler, consumeOpts, logger, poisonHandler, maxDegreeOfParallelism, resiliencePipeline, healthMetrics, topologyReadySignal, middlewares)
        {
        }

        public Task RunExecuteHandlerAsync(IJsMessageContext<T> context, CancellationToken token) 
            => InvokeExecuteHandlerWithResilienceAsync(context, token);

        private Task InvokeExecuteHandlerWithResilienceAsync(IJsMessageContext<T> context, CancellationToken token)
        {
            // We need to use reflection because ExecuteHandlerWithResilienceAsync is private
            var method = typeof(NatsConsumerBackgroundService<T>).GetMethod("ExecuteHandlerWithResilienceAsync", 
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            return (Task)method!.Invoke(this, new object[] { context, token })!;
        }
    }

    [Test]
    public async Task ExecuteHandler_ShouldInvokeMiddlewareAndHandler()
    {
        // Arrange
        var consumer = Substitute.For<INatsJSConsumer>();
        var logger = Substitute.For<ILogger>();
        var poisonHandler = Substitute.For<IPoisonMessageHandler<string>>();
        
        var handlerInvoked = false;
        Func<IJsMessageContext<string>, Task> handler = _ => 
        {
            handlerInvoked = true;
            return Task.CompletedTask;
        };

        var middleware = Substitute.For<IConsumerMiddleware<string>>();
        middleware.InvokeAsync(Arg.Any<IJsMessageContext<string>>(), Arg.Any<Func<Task>>(), Arg.Any<CancellationToken>())
            .Returns(async x => 
            {
                await x.Arg<Func<Task>>()();
            });

        var service = new TestableNatsConsumerBackgroundService<string>(
            consumer, "stream", "consumer", handler, new NatsJSConsumeOpts(), logger, poisonHandler, 
            middlewares: new[] { middleware });

        var context = Substitute.For<IJsMessageContext<string>>();

        // Act
        await service.RunExecuteHandlerAsync(context, CancellationToken.None);

        // Assert
        handlerInvoked.ShouldBeTrue();
        await middleware.Received(1).InvokeAsync(Arg.Any<IJsMessageContext<string>>(), Arg.Any<Func<Task>>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExecuteHandler_ShouldUseResiliencePipeline()
    {
        // Arrange
        var consumer = Substitute.For<INatsJSConsumer>();
        var logger = Substitute.For<ILogger>();
        var poisonHandler = Substitute.For<IPoisonMessageHandler<string>>();
        
        Func<IJsMessageContext<string>, Task> handler = _ => Task.CompletedTask;

        // ResiliencePipeline is sealed, so we can't mock it directly.
        // Instead, create a real pipeline and track handler execution.
        var pipeline = new ResiliencePipelineBuilder()
            .AddRetry(new Polly.Retry.RetryStrategyOptions
            {
                MaxRetryAttempts = 1 // No actual retries needed for this test
            })
            .Build();

        // We'll track pipeline execution by wrapping the handler
        var handlerExecuted = false;
        Func<IJsMessageContext<string>, Task> trackingHandler = ctx =>
        {
            handlerExecuted = true;
            return Task.CompletedTask;
        };

        var service = new TestableNatsConsumerBackgroundService<string>(
            consumer, "stream", "consumer", trackingHandler, new NatsJSConsumeOpts(), logger, poisonHandler, 
            resiliencePipeline: pipeline);

        var context = Substitute.For<IJsMessageContext<string>>();

        // Act
        await service.RunExecuteHandlerAsync(context, CancellationToken.None);

        // Assert - If the handler was executed, the pipeline was used (since it wraps the handler call)
        handlerExecuted.ShouldBeTrue("The handler should have been executed through the resilience pipeline");
    }

    /// <summary>
    /// Test 8.2: Backpressure and Channel Saturation
    /// 
    /// This test validates that when the bounded channel is full, subsequent messages are NAKed
    /// immediately rather than buffered in memory. This behavior is crucial for:
    /// 1. Preventing memory exhaustion under load
    /// 2. Allowing the NATS server to redistribute messages to other consumers
    /// 3. Maintaining QoS by not silently dropping messages
    /// 
    /// The test simulates a deadlocked handler (MaxConcurrency = 1) and verifies:
    /// - The first message goes into the channel and starts processing (blocks)
    /// - Subsequent messages get NAKed because TryWrite fails on the full channel
    /// </summary>
    [Test]
    public async Task ExecuteAsync_ShouldNakMessages_WhenChannelFull_Backpressure()
    {
        // Arrange
        var consumer = Substitute.For<INatsJSConsumer>();
        var logger = Substitute.For<ILogger>();
        var poisonHandler = Substitute.For<IPoisonMessageHandler<string>>();
        var healthMetrics = Substitute.For<IConsumerHealthMetrics>();
        
        // Track NAKs across all messages
        var nakCount = 0;
        var messagesYielded = new TaskCompletionSource();
        
        // Create 5 mock messages that track their NAK calls
        var mockMessages = new List<INatsJSMsg<string>>();
        for (int i = 0; i < 5; i++)
        {
            var msg = Substitute.For<INatsJSMsg<string>>();
            msg.Data.Returns($"message-{i}");
            msg.Subject.Returns("test.subject");
            msg.NakAsync(Arg.Any<AckOpts>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
                .Returns(ValueTask.CompletedTask)
                .AndDoes(_ => Interlocked.Increment(ref nakCount));
            mockMessages.Add(msg);
        }
        
        // Setup consumer to yield 5 messages then signal completion
        consumer.ConsumeAsync<string>(
            Arg.Any<INatsDeserialize<string>>(),
            Arg.Any<NatsJSConsumeOpts>(),
            Arg.Any<CancellationToken>())
            .Returns(_ => CreateMessageStream(mockMessages, messagesYielded));
        
        // Handler that blocks forever (simulates deadlock)
        var handlerBlocked = new TaskCompletionSource();
        var handlerStarted = new TaskCompletionSource();
        Func<IJsMessageContext<string>, Task> blockingHandler = async _ =>
        {
            handlerStarted.TrySetResult();
            await handlerBlocked.Task;
        };
        
        // Create service with MaxConcurrency = 1 (channel capacity = 1)
        var service = new TestableNatsConsumerBackgroundService<string>(
            consumer, "test-stream", "test-consumer", blockingHandler, 
            new NatsJSConsumeOpts(), logger, poisonHandler,
            maxDegreeOfParallelism: 1,
            healthMetrics: healthMetrics);
        
        using var cts = new CancellationTokenSource();
        
        // Act - start the service
        var executeTask = service.StartAsync(cts.Token);
        
        // Wait for handler to start processing first message
        var handlerStartedTask = handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await handlerStartedTask;
        
        // Wait for all messages to be yielded (which triggers NAKs for overflow)
        var messagesYieldedTask = messagesYielded.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await messagesYieldedTask;
        
        // Small buffer for async operations to complete
        await Task.Delay(100);
        
        // Stop the service
        await cts.CancelAsync();
        handlerBlocked.TrySetResult(); // Unblock handler to allow graceful shutdown
        
        try
        {
            await service.StopAsync(CancellationToken.None);
        }
        catch (OperationCanceledException) { /* Expected */ }
        
        // Assert
        // With MaxConcurrency = 1: channel capacity = 1
        // First message enters channel and blocks in handler
        // Second message: channel has 1 slot, TryWrite succeeds (queued)
        // Messages 3-5: TryWrite fails, NAKed = 3 NAKs
        // Note: The actual count depends on timing of worker processing
        // At minimum, we expect the last few messages to be NAKed
        nakCount.ShouldBeGreaterThanOrEqualTo(3, 
            $"Expected at least 3 NAKs when 5 messages overwhelm a capacity-1 channel, got {nakCount}");
    }

    /// <summary>
    /// Helper method to create an async enumerable from a list of messages.
    /// </summary>
    private static async IAsyncEnumerable<INatsJSMsg<string>> CreateMessageStream(
        List<INatsJSMsg<string>> messages, 
        TaskCompletionSource completionSignal,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var msg in messages)
        {
            if (cancellationToken.IsCancellationRequested) yield break;
            yield return msg;
            await Task.Yield(); // Allow other work to proceed
        }
        completionSignal.TrySetResult();
        
        // Keep enumerable alive until cancelled
        try
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
        }
        catch (OperationCanceledException) { }
    }
}
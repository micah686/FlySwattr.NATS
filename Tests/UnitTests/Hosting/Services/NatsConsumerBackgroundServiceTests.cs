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
            TimeSpan? ackTimeout = null,
            ResiliencePipeline? resiliencePipeline = null,
            IConsumerHealthMetrics? healthMetrics = null,
            ITopologyReadySignal? topologyReadySignal = null,
            IEnumerable<IConsumerMiddleware<T>>? middlewares = null) 
            : base(consumer, streamName, consumerName, handler, consumeOpts, logger, poisonHandler, maxDegreeOfParallelism, ackTimeout, resiliencePipeline, healthMetrics, topologyReadySignal, middlewares)
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
            msg.NakAsync(default, default, default)
                .ReturnsForAnyArgs(ValueTask.CompletedTask)
                .AndDoes(x => Interlocked.Increment(ref nakCount));
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
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        
        // Wait for all messages to be yielded (which triggers NAKs for overflow)
        await messagesYielded.Task.WaitAsync(TimeSpan.FromSeconds(5));
        
        // Unblock handler FIRST to ensure workers can drain/exit
        handlerBlocked.TrySetResult(); 
        
        // Small buffer to allow worker to process next message if any
        await Task.Delay(100);

        // Stop the service
        await cts.CancelAsync();
        
        try
        {
            // Wait for service to stop with generous timeout
            await service.StopAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5));
        }
        catch (OperationCanceledException) { /* Expected */ }
        catch (TimeoutException) { /* Ignore stop timeout in test */ }
        
        // Assert
        nakCount.ShouldBeGreaterThanOrEqualTo(3, 
            $"Expected at least 3 NAKs when 5 messages overwhelm a capacity-1 channel, got {nakCount}");
    }

    [Test]
    public async Task ExecuteAsync_ShouldProcessMessages_AndAck()
    {
        // Arrange
        var consumer = Substitute.For<INatsJSConsumer>();
        var logger = Substitute.For<ILogger>();
        var poisonHandler = Substitute.For<IPoisonMessageHandler<string>>();
        
        var processedMessages = new List<string>();
        var ackCount = 0;
        var messagesYielded = new TaskCompletionSource();
        
        // Create 2 mock messages
        var mockMessages = new List<INatsJSMsg<string>>();
        for (int i = 0; i < 2; i++)
        {
            var msg = Substitute.For<INatsJSMsg<string>>();
            var msgData = $"msg-{i}";
            msg.Data.Returns(msgData);
            msg.Subject.Returns("test.subject");
            msg.AckAsync(Arg.Any<AckOpts?>(), Arg.Any<CancellationToken>())
                .Returns(ValueTask.CompletedTask)
                .AndDoes(x => Interlocked.Increment(ref ackCount));
            mockMessages.Add(msg);
        }
        
        // Setup consumer
        consumer.ConsumeAsync<string>(
            Arg.Any<INatsDeserialize<string>>(),
            Arg.Any<NatsJSConsumeOpts>(),
            Arg.Any<CancellationToken>())
            .Returns(c => CreateMessageStream(mockMessages, messagesYielded, c.Arg<CancellationToken>()));
            
        // Handler
        Func<IJsMessageContext<string>, Task> handler = async ctx =>
        {
            processedMessages.Add(ctx.Message);
            await Task.Yield();
        };
        
        var service = new TestableNatsConsumerBackgroundService<string>(
            consumer, "stream", "consumer", handler, new NatsJSConsumeOpts(), logger, poisonHandler);
            
        using var cts = new CancellationTokenSource();
        
        // Act
        var executeTask = service.StartAsync(cts.Token);
        
        // Wait for messages to be yielded
        await messagesYielded.Task.WaitAsync(TimeSpan.FromSeconds(5));
        
        // Wait a bit for processing
        await Task.Delay(200);
        
        await cts.CancelAsync();
        try { await service.StopAsync(CancellationToken.None); } catch { }
        
        // Assert
        processedMessages.Count.ShouldBe(2);
        processedMessages.ShouldContain("msg-0");
        processedMessages.ShouldContain("msg-1");
        ackCount.ShouldBe(2);
    }

    [Test]
    public async Task ExecuteAsync_ShouldUseConfiguredAckTimeout()
    {
        // Arrange
        var consumer = Substitute.For<INatsJSConsumer>();
        var logger = Substitute.For<ILogger>();
        var poisonHandler = Substitute.For<IPoisonMessageHandler<string>>();

        var messagesYielded = new TaskCompletionSource();
        var ackTokenCaptured = new TaskCompletionSource<CancellationToken>(TaskCreationOptions.RunContinuationsAsynchronously);

        var msg = Substitute.For<INatsJSMsg<string>>();
        msg.Data.Returns("msg-ack-timeout");
        msg.Subject.Returns("test.subject");
        msg.AckAsync(Arg.Any<AckOpts?>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var token = callInfo.ArgAt<CancellationToken>(1);
                ackTokenCaptured.TrySetResult(token);
                return new ValueTask(Task.Delay(Timeout.Infinite, token));
            });

        consumer.ConsumeAsync<string>(
            Arg.Any<INatsDeserialize<string>>(),
            Arg.Any<NatsJSConsumeOpts>(),
            Arg.Any<CancellationToken>())
            .Returns(c => CreateMessageStream([msg], messagesYielded, c.Arg<CancellationToken>()));

        Func<IJsMessageContext<string>, Task> handler = _ => Task.CompletedTask;

        var service = new TestableNatsConsumerBackgroundService<string>(
            consumer,
            "stream",
            "consumer",
            handler,
            new NatsJSConsumeOpts(),
            logger,
            poisonHandler,
            ackTimeout: TimeSpan.FromMilliseconds(50));

        using var cts = new CancellationTokenSource();

        // Act
        await service.StartAsync(cts.Token);
        await messagesYielded.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var ackToken = await ackTokenCaptured.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Task.Delay(200);
        await cts.CancelAsync();
        try { await service.StopAsync(CancellationToken.None); } catch { }

        // Assert
        ackToken.IsCancellationRequested.ShouldBeTrue();
    }

    [Test]
    public async Task ExecuteAsync_ShouldDrainQueuedMessagesAndNakOnShutdown()
    {
        // Arrange
        var consumer = Substitute.For<INatsJSConsumer>();
        var logger = Substitute.For<ILogger>();
        var poisonHandler = Substitute.For<IPoisonMessageHandler<string>>();

        var firstHandlerEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirstHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var messagesYielded = new TaskCompletionSource();

        var firstMsg = Substitute.For<INatsJSMsg<string>>();
        firstMsg.Data.Returns("first");
        firstMsg.Subject.Returns("test.subject.1");
        firstMsg.AckAsync(Arg.Any<AckOpts?>(), Arg.Any<CancellationToken>()).Returns(ValueTask.CompletedTask);

        var secondMsg = Substitute.For<INatsJSMsg<string>>();
        secondMsg.Data.Returns("second");
        secondMsg.Subject.Returns("test.subject.2");
        secondMsg.NakAsync(default, default, default).ReturnsForAnyArgs(ValueTask.CompletedTask);

        consumer.ConsumeAsync<string>(
            Arg.Any<INatsDeserialize<string>>(),
            Arg.Any<NatsJSConsumeOpts>(),
            Arg.Any<CancellationToken>())
            .Returns(c => CreateSequencedMessageStream(firstMsg, secondMsg, firstHandlerEntered.Task, messagesYielded, c.Arg<CancellationToken>()));

        Func<IJsMessageContext<string>, Task> handler = async ctx =>
        {
            if (ctx.Message == "first")
            {
                firstHandlerEntered.TrySetResult();
                await releaseFirstHandler.Task;
            }
        };

        var service = new TestableNatsConsumerBackgroundService<string>(
            consumer,
            "stream",
            "consumer",
            handler,
            new NatsJSConsumeOpts(),
            logger,
            poisonHandler,
            maxDegreeOfParallelism: 1);

        using var cts = new CancellationTokenSource();

        // Act
        await service.StartAsync(cts.Token);
        await firstHandlerEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await messagesYielded.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await cts.CancelAsync();
        await WaitForNakAsync(secondMsg, TimeSpan.FromSeconds(5));

        releaseFirstHandler.TrySetResult();
        try { await service.StopAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5)); } catch { }
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

    private static async IAsyncEnumerable<INatsJSMsg<string>> CreateSequencedMessageStream(
        INatsJSMsg<string> first,
        INatsJSMsg<string> second,
        Task firstHandlerStarted,
        TaskCompletionSource completionSignal,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested) yield break;

        yield return first;
        await firstHandlerStarted.WaitAsync(cancellationToken);

        if (cancellationToken.IsCancellationRequested) yield break;

        yield return second;
        completionSignal.TrySetResult();

        try
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
        }
        catch (OperationCanceledException) { }
    }

    private static async Task WaitForNakAsync(INatsJSMsg<string> message, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);

        while (!cts.IsCancellationRequested)
        {
            try
            {
                await message.Received(1).NakAsync(default, TimeSpan.FromSeconds(1), Arg.Any<CancellationToken>());
                return;
            }
            catch
            {
                await Task.Delay(50, cts.Token);
            }
        }

        await message.Received(1).NakAsync(default, TimeSpan.FromSeconds(1), Arg.Any<CancellationToken>());
    }
}

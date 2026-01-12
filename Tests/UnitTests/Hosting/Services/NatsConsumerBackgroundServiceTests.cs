using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Hosting.Health;
using FlySwattr.NATS.Hosting.Services;
using Microsoft.Extensions.Logging;
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

        var pipeline = Substitute.For<ResiliencePipeline>();
        // Mock ExecuteAsync to invoke the callback
        pipeline.ExecuteAsync(Arg.Any<Func<CancellationToken, ValueTask>>(), Arg.Any<CancellationToken>())
            .Returns(x => 
            {
                var callback = x.Arg<Func<CancellationToken, ValueTask>>();
                var ct = x.Arg<CancellationToken>();
                return callback(ct);
            });

        var service = new TestableNatsConsumerBackgroundService<string>(
            consumer, "stream", "consumer", handler, new NatsJSConsumeOpts(), logger, poisonHandler, 
            resiliencePipeline: pipeline);

        var context = Substitute.For<IJsMessageContext<string>>();

        // Act
        await service.RunExecuteHandlerAsync(context, CancellationToken.None);

        // Assert
        await pipeline.Received(1).ExecuteAsync(Arg.Any<Func<CancellationToken, ValueTask>>(), Arg.Any<CancellationToken>());
    }
}
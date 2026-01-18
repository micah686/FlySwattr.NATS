using System.Buffers;
using System.Diagnostics;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NSubstitute;
using NSubstitute.Core;
using NSubstitute.ExceptionExtensions;
using TUnit.Core;

namespace UnitTests.Core.Messaging;

[Property("nTag", "Core")]
public class NatsJetStreamBusTests : IAsyncDisposable
{
    private readonly INatsJSContext _jsContext;
    private readonly ILogger<NatsJetStreamBus> _logger;
    private readonly IMessageSerializer _serializer;
    private readonly NatsJetStreamBus _bus;

    public NatsJetStreamBusTests()
    {
        _jsContext = Substitute.For<INatsJSContext>();
        _logger = Substitute.For<ILogger<NatsJetStreamBus>>();
        _serializer = Substitute.For<IMessageSerializer>();

        _serializer.When(x => x.Serialize(Arg.Any<IBufferWriter<byte>>(), Arg.Any<object>()))
                   .Do(x => x.Arg<IBufferWriter<byte>>().Write(new byte[] { 1 }));

        _bus = new NatsJetStreamBus(
            _jsContext,
            _logger,
            _serializer
        );
    }

    [Test]
    public async Task PublishAsync_WithNullMessageId_ShouldThrowArgumentException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await _bus.PublishAsync("test.subject", new { Data = "test" }, messageId: (string?)null));
    }

    [Test]
    public async Task PublishAsync_WithEmptyMessageId_ShouldThrowArgumentException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await _bus.PublishAsync("test.subject", new { Data = "test" }, messageId: ""));
    }

    [Test]
    public async Task PublishAsync_WithWhitespaceMessageId_ShouldThrowArgumentException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await _bus.PublishAsync("test.subject", new { Data = "test" }, messageId: "   "));
    }

    [Test]
    public async Task PublishAsync_WithoutMessageIdOverload_ShouldThrowArgumentException()
    {
        // Act & Assert - the overload without messageId should throw
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await _bus.PublishAsync("test.subject", new { Data = "test" }));
    }

    [Test]
    public async Task PublishAsync_ShouldThrow_OnFailure()
    {
        // Arrange - mock the generic PublishAsync<T> method that NatsJetStreamBus calls
        // The actual call uses: _jsContext.PublishAsync(subject, message, headers: headers, opts: opts, cancellationToken)
        _jsContext.PublishAsync(
            Arg.Any<string>(),
            Arg.Any<TestMessage>(),
            Arg.Any<INatsSerialize<TestMessage>>(),
            Arg.Any<NatsJSPubOpts>(),
            Arg.Any<NatsHeaders>(),
            Arg.Any<CancellationToken>()
        ).ThrowsAsync(new NatsNoRespondersException());

        // Act & Assert
        await Assert.ThrowsAsync<NatsNoRespondersException>(async () =>
            await _bus.PublishAsync("test.subject", new TestMessage("test"), "msg-1"));
    }

    private record TestMessage(string Data);

    [Test]
    public async Task ConsumeAsync_ShouldHandleConsumerDeletion()
    {
        // Arrange
        var consumer = Substitute.For<INatsJSConsumer>();
        consumer.Info.Returns(new ConsumerInfo { Name = "test-consumer", StreamName = "test-stream", Config = new ConsumerConfig() });
        
        _jsContext.CreateOrUpdateConsumerAsync(
            Arg.Any<string>(), 
            Arg.Any<ConsumerConfig>(), 
            Arg.Any<CancellationToken>())
            .Returns(consumer);

        // Simulate consumer deletion by throwing on ConsumeAsync
        consumer.ConsumeAsync<object>(
            Arg.Any<INatsDeserialize<object>>(), 
            Arg.Any<NatsJSConsumeOpts>(), 
            Arg.Any<CancellationToken>())
            .Throws(new NatsException("Consumer not found"));

        // Act
        // This will log error but not throw, effectively starting the service loop which will fail and retry/exit
        await _bus.ConsumeAsync<object>(
            StreamName.From("test-stream"),
            SubjectName.From("test.subject"),
            async _ => await Task.CompletedTask
        );

        // Assert
        // Verified by no exception thrown
    }

    [Test]
    public async Task ConsumeAsync_ShouldResubscribe_AfterConnectionReconnect()
    {
        // Arrange
        var consumer = Substitute.For<INatsJSConsumer>();
        consumer.Info.Returns(new ConsumerInfo { Name = "test-consumer", StreamName = "test-stream", Config = new ConsumerConfig() });
        
        _jsContext.CreateOrUpdateConsumerAsync(
            Arg.Any<string>(), 
            Arg.Any<ConsumerConfig>(), 
            Arg.Any<CancellationToken>())
            .Returns(consumer);

        // Simulate disconnect then reconnect
        var callCount = 0;
        consumer.ConsumeAsync<object>(
            Arg.Any<INatsDeserialize<object>>(),
            Arg.Any<NatsJSConsumeOpts>(),
            Arg.Any<CancellationToken>())
            .Returns(info =>
            {
                var count = Interlocked.Increment(ref callCount);
                if (count == 1)
                {
                    throw new NatsException("Connection lost");
                }
                return CreateEmptyAsyncEnumerable();
            });

        // Act
        await _bus.ConsumeAsync<object>(
            StreamName.From("test-stream"),
            SubjectName.From("test.subject"),
            async _ => await Task.CompletedTask
        );

        // Assert
        await Task.Delay(1500); // Wait for retry
        await Assert.That(callCount).IsGreaterThanOrEqualTo(2);
    }

    [Test]
    public async Task ConsumePullAsync_ShouldHandleNoMessagesAvailable()
    {
        // Arrange
        var consumer = Substitute.For<INatsJSConsumer>();
        consumer.Info.Returns(new ConsumerInfo { Name = "test-consumer", StreamName = "test-stream", Config = new ConsumerConfig() });

        _jsContext.GetConsumerAsync(
            Arg.Any<string>(), 
            Arg.Any<string>(), 
            Arg.Any<CancellationToken>())
            .Returns(consumer);
            
        consumer.ConsumeAsync<object>(
            Arg.Any<INatsDeserialize<object>>(),
            Arg.Any<NatsJSConsumeOpts>(),
            Arg.Any<CancellationToken>())
            .Returns(info => GetDelayedEmptyEnumerable());

        // Act
        await _bus.ConsumePullAsync<object>(
            StreamName.From("test-stream"),
            ConsumerName.From("test-consumer"),
            async _ => await Task.CompletedTask
        );

        // Assert
        // Verified by no exception
    }

    [Test]
    public async Task DisposeAsync_ShouldStopAndDisposeBackgroundServices()
    {
        // Arrange
        var consumer = Substitute.For<INatsJSConsumer>();
        consumer.Info.Returns(new ConsumerInfo { Name = "test-consumer", StreamName = "test-stream", Config = new ConsumerConfig() });
        
        _jsContext.CreateOrUpdateConsumerAsync(
            Arg.Any<string>(), 
            Arg.Any<ConsumerConfig>(), 
            Arg.Any<CancellationToken>())
            .Returns(consumer);

        consumer.ConsumeAsync<object>(
            Arg.Any<INatsDeserialize<object>>(),
            Arg.Any<NatsJSConsumeOpts>(),
            Arg.Any<CancellationToken>())
            .Returns(CreateEmptyAsyncEnumerable());

        await _bus.ConsumeAsync<object>(
            StreamName.From("test-stream"),
            SubjectName.From("test.subject"),
            async _ => await Task.CompletedTask
        );

        // Act
        await _bus.DisposeAsync();

        // Assert
        // If we reached here without hanging, it means DisposeAsync didn't deadlock.
        // We can't easily verify private fields, but we've exercised the path.
    }

    [Test]
    public async Task ConsumeAsync_ShouldRetry_WhenLoopErrorOccurs()
    {
        // Arrange
        var consumer = Substitute.For<INatsJSConsumer>();
        consumer.Info.Returns(new ConsumerInfo { Name = "test-consumer", StreamName = "test-stream", Config = new ConsumerConfig() });
        
        _jsContext.CreateOrUpdateConsumerAsync(
            Arg.Any<string>(), 
            Arg.Any<ConsumerConfig>(), 
            Arg.Any<CancellationToken>())
            .Returns(consumer);

        // Throw then return empty
        var callCount = 0;
        consumer.ConsumeAsync<object>(
            Arg.Any<INatsDeserialize<object>>(),
            Arg.Any<NatsJSConsumeOpts>(),
            Arg.Any<CancellationToken>())
            .Returns(info =>
            {
                var count = Interlocked.Increment(ref callCount);
                if (count == 1)
                {
                    throw new Exception("Transient loop error");
                }
                return CreateEmptyAsyncEnumerable();
            });

        // Act
        await _bus.ConsumeAsync<object>(
            StreamName.From("test-stream"),
            SubjectName.From("test.subject"),
            async _ => await Task.CompletedTask
        );

        // Assert
        await Task.Delay(1500); // Wait for retry (1s delay in code)
        await Assert.That(callCount).IsGreaterThanOrEqualTo(2);
    }
    
    private static IAsyncEnumerable<INatsJSMsg<object>> GetDelayedEmptyEnumerable()
    {
        return GetDelayedEmptyEnumerableImpl();

        static async IAsyncEnumerable<INatsJSMsg<object>> GetDelayedEmptyEnumerableImpl([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            try
            {
                await Task.Delay(50, ct);
            }
            catch (OperationCanceledException)
            {
            }
            yield break;
        }
    }

    private static IAsyncEnumerable<INatsJSMsg<object>> CreateEmptyAsyncEnumerable()
    {
        return CreateEmptyAsyncEnumerableImpl();

        static async IAsyncEnumerable<INatsJSMsg<object>> CreateEmptyAsyncEnumerableImpl()
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _bus.DisposeAsync();
    }
}

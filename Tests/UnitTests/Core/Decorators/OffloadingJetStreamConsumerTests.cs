using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Configuration;
using FlySwattr.NATS.Core.Decorators;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Core.Decorators;

[Property("nTag", "Core")]
public class OffloadingJetStreamConsumerTests
{
    private readonly IJetStreamConsumer _innerConsumer;
    private readonly IObjectStore _objectStore;
    private readonly IMessageSerializer _serializer;
    private readonly ILogger<OffloadingJetStreamConsumer> _logger;
    private readonly PayloadOffloadingOptions _options;

    public OffloadingJetStreamConsumerTests()
    {
        _innerConsumer = Substitute.For<IJetStreamConsumer>();
        _objectStore = Substitute.For<IObjectStore>();
        _serializer = Substitute.For<IMessageSerializer>();
        _logger = Substitute.For<ILogger<OffloadingJetStreamConsumer>>();
        _options = new PayloadOffloadingOptions
        {
            ClaimCheckHeaderName = "X-ClaimCheck-Ref",
            ThresholdBytes = 1000
        };
    }

    private OffloadingJetStreamConsumer CreateSut()
    {
        return new OffloadingJetStreamConsumer(
            _innerConsumer,
            _objectStore,
            _serializer,
            Options.Create(_options),
            _logger);
    }

    #region ConsumeAsync Tests

    [Test]
    public async Task ConsumeAsync_ShouldDelegateToInner_WhenNoClaimCheck()
    {
        // Arrange
        var sut = CreateSut();
        var stream = StreamName.From("test-stream");
        var subject = SubjectName.From("test.subject");
        Func<IJsMessageContext<TestMessage>, Task> handler = _ => Task.CompletedTask;

        // Act
        await sut.ConsumeAsync(stream, subject, handler);

        // Assert
        await _innerConsumer.Received(1).ConsumeAsync(
            stream,
            subject,
            Arg.Any<Func<IJsMessageContext<TestMessage>, Task>>(),
            Arg.Any<JetStreamConsumeOptions?>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ConsumeAsync_ShouldWrapHandler_ToResolveClaimCheck()
    {
        // Arrange
        var sut = CreateSut();
        var stream = StreamName.From("test-stream");
        var subject = SubjectName.From("test.subject");
        Func<IJsMessageContext<TestMessage>, Task>? capturedHandler = null;

        _innerConsumer.ConsumeAsync(
            stream,
            subject,
            Arg.Do<Func<IJsMessageContext<TestMessage>, Task>>(h => capturedHandler = h),
            Arg.Any<JetStreamConsumeOptions?>(),
            Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var handlerCalled = false;
        Func<IJsMessageContext<TestMessage>, Task> handler = _ =>
        {
            handlerCalled = true;
            return Task.CompletedTask;
        };

        // Act
        await sut.ConsumeAsync(stream, subject, handler);

        // Assert - a wrapped handler should be captured
        capturedHandler.ShouldNotBeNull();

        // Simulate inner consumer calling the wrapped handler with a regular message
        var mockContext = CreateMockMessageContext(new TestMessage { Data = "test" });
        await capturedHandler(mockContext);

        handlerCalled.ShouldBeTrue();
    }

    #endregion

    #region ConsumePullAsync Tests

    [Test]
    public async Task ConsumePullAsync_ShouldDelegateToInner_WhenNoClaimCheck()
    {
        // Arrange
        var sut = CreateSut();
        var stream = StreamName.From("test-stream");
        var consumer = ConsumerName.From("test-consumer");
        Func<IJsMessageContext<TestMessage>, Task> handler = _ => Task.CompletedTask;

        // Act
        await sut.ConsumePullAsync(stream, consumer, handler);

        // Assert
        await _innerConsumer.Received(1).ConsumePullAsync(
            stream,
            consumer,
            Arg.Any<Func<IJsMessageContext<TestMessage>, Task>>(),
            Arg.Any<JetStreamConsumeOptions?>(),
            Arg.Any<CancellationToken>());
    }

    #endregion

    #region ResolveClaimCheck Tests

    [Test]
    public async Task ResolveClaimCheck_ShouldResolveFromHeader_WhenHeaderPresent()
    {
        // Arrange
        var sut = CreateSut();
        var stream = StreamName.From("test-stream");
        var subject = SubjectName.From("test.subject");
        var objectKey = "claimcheck/test.subject/abc123";
        var resolvedMessage = new TestMessage { Data = "resolved" };
        var resolvedPayload = new byte[] { 1, 2, 3, 4 };

        Func<IJsMessageContext<TestMessage>, Task>? capturedHandler = null;
        _innerConsumer.ConsumeAsync(
            stream,
            subject,
            Arg.Do<Func<IJsMessageContext<TestMessage>, Task>>(h => capturedHandler = h),
            Arg.Any<JetStreamConsumeOptions?>(),
            Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        // Setup object store to return payload
        _objectStore.When(x => x.GetAsync(objectKey, Arg.Any<Stream>(), Arg.Any<CancellationToken>()))
            .Do(x =>
            {
                var targetStream = x.Arg<Stream>();
                targetStream.Write(resolvedPayload, 0, resolvedPayload.Length);
            });

        _serializer.Deserialize<TestMessage>(Arg.Any<ReadOnlyMemory<byte>>()).Returns(resolvedMessage);

        TestMessage? receivedMessage = null;
        Func<IJsMessageContext<TestMessage>, Task> handler = ctx =>
        {
            receivedMessage = ctx.Message;
            return Task.CompletedTask;
        };

        // Act
        await sut.ConsumeAsync(stream, subject, handler);

        // Simulate inner consumer calling wrapped handler with claim check header
        var headers = new Dictionary<string, string>
        {
            { _options.ClaimCheckHeaderName, $"objstore://{objectKey}" }
        };
        var mockContext = CreateMockMessageContext(default(TestMessage)!, headers);
        await capturedHandler!(mockContext);

        // Assert
        receivedMessage.ShouldBe(resolvedMessage);
        await _objectStore.Received(1).GetAsync(objectKey, Arg.Any<Stream>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ResolveClaimCheck_ShouldReturnOriginalContext_WhenNoClaimCheck()
    {
        // Arrange
        var sut = CreateSut();
        var stream = StreamName.From("test-stream");
        var subject = SubjectName.From("test.subject");
        var originalMessage = new TestMessage { Data = "original" };

        Func<IJsMessageContext<TestMessage>, Task>? capturedHandler = null;
        _innerConsumer.ConsumeAsync(
            stream,
            subject,
            Arg.Do<Func<IJsMessageContext<TestMessage>, Task>>(h => capturedHandler = h),
            Arg.Any<JetStreamConsumeOptions?>(),
            Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        TestMessage? receivedMessage = null;
        Func<IJsMessageContext<TestMessage>, Task> handler = ctx =>
        {
            receivedMessage = ctx.Message;
            return Task.CompletedTask;
        };

        // Act
        await sut.ConsumeAsync(stream, subject, handler);

        // Simulate inner consumer calling wrapped handler with a regular message (no claim check)
        var mockContext = CreateMockMessageContext(originalMessage);
        await capturedHandler!(mockContext);

        // Assert - should receive original message, object store not called
        receivedMessage.ShouldBe(originalMessage);
        await _objectStore.DidNotReceive().GetAsync(Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>());
    }

    #endregion

    #region ExtractObjectKey Tests

    [Test]
    public async Task ExtractObjectKey_ShouldRemoveObjstorePrefix()
    {
        // Arrange
        var sut = CreateSut();
        var stream = StreamName.From("test-stream");
        var subject = SubjectName.From("test.subject");
        var objectKey = "claimcheck/test.subject/abc123";
        var fullRef = $"objstore://{objectKey}";
        var resolvedMessage = new TestMessage { Data = "resolved" };

        Func<IJsMessageContext<TestMessage>, Task>? capturedHandler = null;
        _innerConsumer.ConsumeAsync(
            stream,
            subject,
            Arg.Do<Func<IJsMessageContext<TestMessage>, Task>>(h => capturedHandler = h),
            Arg.Any<JetStreamConsumeOptions?>(),
            Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        _serializer.Deserialize<TestMessage>(Arg.Any<ReadOnlyMemory<byte>>()).Returns(resolvedMessage);

        Func<IJsMessageContext<TestMessage>, Task> handler = _ => Task.CompletedTask;

        // Act
        await sut.ConsumeAsync(stream, subject, handler);

        var headers = new Dictionary<string, string>
        {
            { _options.ClaimCheckHeaderName, fullRef }
        };
        var mockContext = CreateMockMessageContext(default(TestMessage)!, headers);
        await capturedHandler!(mockContext);

        // Assert - should call GetAsync with object key without prefix
        await _objectStore.Received(1).GetAsync(objectKey, Arg.Any<Stream>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExtractObjectKey_ShouldReturnOriginal_WhenNoPrefix()
    {
        // Arrange
        var sut = CreateSut();
        var stream = StreamName.From("test-stream");
        var subject = SubjectName.From("test.subject");
        var objectKey = "claimcheck/test.subject/abc123"; // No objstore:// prefix
        var resolvedMessage = new TestMessage { Data = "resolved" };

        Func<IJsMessageContext<TestMessage>, Task>? capturedHandler = null;
        _innerConsumer.ConsumeAsync(
            stream,
            subject,
            Arg.Do<Func<IJsMessageContext<TestMessage>, Task>>(h => capturedHandler = h),
            Arg.Any<JetStreamConsumeOptions?>(),
            Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        _serializer.Deserialize<TestMessage>(Arg.Any<ReadOnlyMemory<byte>>()).Returns(resolvedMessage);

        Func<IJsMessageContext<TestMessage>, Task> handler = _ => Task.CompletedTask;

        // Act
        await sut.ConsumeAsync(stream, subject, handler);

        var headers = new Dictionary<string, string>
        {
            { _options.ClaimCheckHeaderName, objectKey } // No prefix
        };
        var mockContext = CreateMockMessageContext(default(TestMessage)!, headers);
        await capturedHandler!(mockContext);

        // Assert - should use key as-is
        await _objectStore.Received(1).GetAsync(objectKey, Arg.Any<Stream>(), Arg.Any<CancellationToken>());
    }

    #endregion

    #region OffloadingMessageContext Tests

    [Test]
    public async Task OffloadingMessageContext_ShouldReturnResolvedMessage()
    {
        // Arrange
        var sut = CreateSut();
        var stream = StreamName.From("test-stream");
        var subject = SubjectName.From("test.subject");
        var objectKey = "claimcheck/test.subject/abc123";
        var resolvedMessage = new TestMessage { Data = "resolved-content" };

        Func<IJsMessageContext<TestMessage>, Task>? capturedHandler = null;
        _innerConsumer.ConsumeAsync(
            stream,
            subject,
            Arg.Do<Func<IJsMessageContext<TestMessage>, Task>>(h => capturedHandler = h),
            Arg.Any<JetStreamConsumeOptions?>(),
            Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        _serializer.Deserialize<TestMessage>(Arg.Any<ReadOnlyMemory<byte>>()).Returns(resolvedMessage);

        IJsMessageContext<TestMessage>? receivedContext = null;
        Func<IJsMessageContext<TestMessage>, Task> handler = ctx =>
        {
            receivedContext = ctx;
            return Task.CompletedTask;
        };

        // Act
        await sut.ConsumeAsync(stream, subject, handler);

        var headers = new Dictionary<string, string>
        {
            { _options.ClaimCheckHeaderName, $"objstore://{objectKey}" }
        };
        var mockContext = CreateMockMessageContext(default(TestMessage)!, headers);
        await capturedHandler!(mockContext);

        // Assert - context should return resolved message
        receivedContext.ShouldNotBeNull();
        receivedContext.Message.ShouldBe(resolvedMessage);
        receivedContext.Message.Data.ShouldBe("resolved-content");
    }

    [Test]
    public async Task OffloadingMessageContext_ShouldDelegateAckToInner()
    {
        // Arrange
        var sut = CreateSut();
        var stream = StreamName.From("test-stream");
        var subject = SubjectName.From("test.subject");
        var objectKey = "claimcheck/test.subject/abc123";
        var resolvedMessage = new TestMessage { Data = "resolved" };

        Func<IJsMessageContext<TestMessage>, Task>? capturedHandler = null;
        _innerConsumer.ConsumeAsync(
            stream,
            subject,
            Arg.Do<Func<IJsMessageContext<TestMessage>, Task>>(h => capturedHandler = h),
            Arg.Any<JetStreamConsumeOptions?>(),
            Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        _serializer.Deserialize<TestMessage>(Arg.Any<ReadOnlyMemory<byte>>()).Returns(resolvedMessage);

        var mockInnerContext = CreateMockMessageContext(default(TestMessage)!, new Dictionary<string, string>
        {
            { _options.ClaimCheckHeaderName, $"objstore://{objectKey}" }
        });

        IJsMessageContext<TestMessage>? receivedContext = null;
        Func<IJsMessageContext<TestMessage>, Task> handler = ctx =>
        {
            receivedContext = ctx;
            return Task.CompletedTask;
        };

        // Act
        await sut.ConsumeAsync(stream, subject, handler);
        await capturedHandler!(mockInnerContext);

        // Call Ack on the wrapped context
        await receivedContext!.AckAsync();

        // Assert - Ack should be delegated to inner context
        await mockInnerContext.Received(1).AckAsync(Arg.Any<CancellationToken>());
    }

    #endregion

    #region Error Handling Tests

    [Test]
    public async Task ResolveClaimCheck_ShouldHandleObjectStoreFailure()
    {
        // Arrange
        var sut = CreateSut();
        var stream = StreamName.From("test-stream");
        var subject = SubjectName.From("test.subject");
        var objectKey = "claimcheck/test.subject/abc123";

        Func<IJsMessageContext<TestMessage>, Task>? capturedHandler = null;
        _innerConsumer.ConsumeAsync(
            stream,
            subject,
            Arg.Do<Func<IJsMessageContext<TestMessage>, Task>>(h => capturedHandler = h),
            Arg.Any<JetStreamConsumeOptions?>(),
            Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        // Object store throws exception
        _objectStore.GetAsync(Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("Object store unavailable"));

        Func<IJsMessageContext<TestMessage>, Task> handler = _ => Task.CompletedTask;

        // Act
        await sut.ConsumeAsync(stream, subject, handler);

        var headers = new Dictionary<string, string>
        {
            { _options.ClaimCheckHeaderName, $"objstore://{objectKey}" }
        };
        var mockContext = CreateMockMessageContext(default(TestMessage)!, headers);

        // Assert - exception should propagate
        await Should.ThrowAsync<InvalidOperationException>(
            () => capturedHandler!(mockContext));
    }

    #endregion

    #region Helper Methods

    private IJsMessageContext<T> CreateMockMessageContext<T>(T message, Dictionary<string, string>? headers = null)
    {
        var mockContext = Substitute.For<IJsMessageContext<T>>();
        mockContext.Message.Returns(message);
        mockContext.Subject.Returns("test.subject");
        mockContext.Headers.Returns(new MessageHeaders(headers ?? new Dictionary<string, string>()));
        mockContext.ReplyTo.Returns((string?)null);
        mockContext.Sequence.Returns(1UL);
        mockContext.Timestamp.Returns(DateTimeOffset.UtcNow);
        mockContext.Redelivered.Returns(false);
        mockContext.NumDelivered.Returns(1U);
        return mockContext;
    }

    #endregion

    #region Test Types

    public class TestMessage
    {
        public string Data { get; set; } = string.Empty;
    }

    #endregion
}

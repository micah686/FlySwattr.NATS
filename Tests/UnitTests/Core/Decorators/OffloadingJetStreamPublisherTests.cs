using System.Buffers;
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
public class OffloadingJetStreamPublisherTests
{
    private readonly IJetStreamPublisher _innerPublisher;
    private readonly IObjectStore _objectStore;
    private readonly IMessageSerializer _serializer;
    private readonly ILogger<OffloadingJetStreamPublisher> _logger;
    private readonly PayloadOffloadingOptions _options;

    public OffloadingJetStreamPublisherTests()
    {
        _innerPublisher = Substitute.For<IJetStreamPublisher>();
        _objectStore = Substitute.For<IObjectStore>();
        _serializer = Substitute.For<IMessageSerializer>();
        _logger = Substitute.For<ILogger<OffloadingJetStreamPublisher>>();
        _options = new PayloadOffloadingOptions
        {
            ThresholdBytes = 1000, // Use a small threshold for testing
            ObjectKeyPrefix = "claimcheck"
        };
    }

    private OffloadingJetStreamPublisher CreateSut()
    {
        return new OffloadingJetStreamPublisher(
            _innerPublisher,
            _objectStore,
            _serializer,
            Options.Create(_options),
            _logger);
    }

    private void SetupSerializerToReturnPayloadOfSize(int size)
    {
        _serializer.When(s => s.Serialize(Arg.Any<IBufferWriter<byte>>(), Arg.Any<TestMessage>()))
            .Do(x =>
            {
                var writer = x.Arg<IBufferWriter<byte>>();
                var span = writer.GetSpan(size);
                // Fill with deterministic data
                for (int i = 0; i < size; i++)
                {
                    span[i] = (byte)(i % 256);
                }
                writer.Advance(size);
            });
    }

    #region Threshold Boundary Tests

    [Test]
    public async Task PublishAsync_MessageBelowThreshold_ShouldSendInline()
    {
        // Arrange
        var payloadSize = _options.ThresholdBytes - 1;
        SetupSerializerToReturnPayloadOfSize(payloadSize);
        var message = new TestMessage { Data = "test" };
        var sut = CreateSut();

        // Act
        await sut.PublishAsync("test.subject", message, "msg-123");

        // Assert: Inner publisher called with original message, NOT ClaimCheckMessage
        await _innerPublisher.Received(1).PublishAsync("test.subject", message, "msg-123", Arg.Any<CancellationToken>());
        
        // Assert: Object store should NOT be called
        await _objectStore.DidNotReceive().PutAsync(Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PublishAsync_MessageExactlyAtThreshold_ShouldSendInline()
    {
        // Arrange: Exactly at threshold means condition (payload.Length > ThresholdBytes) is FALSE
        var payloadSize = _options.ThresholdBytes;
        SetupSerializerToReturnPayloadOfSize(payloadSize);
        var message = new TestMessage { Data = "test" };
        var sut = CreateSut();

        // Act
        await sut.PublishAsync("test.subject", message, "msg-123");

        // Assert: Inner publisher called with original message (not offloaded)
        await _innerPublisher.Received(1).PublishAsync("test.subject", message, "msg-123", Arg.Any<CancellationToken>());
        
        // Assert: Object store should NOT be called
        await _objectStore.DidNotReceive().PutAsync(Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PublishAsync_MessageAboveThreshold_ShouldOffload()
    {
        // Arrange
        var payloadSize = _options.ThresholdBytes + 1;
        SetupSerializerToReturnPayloadOfSize(payloadSize);
        var message = new TestMessage { Data = "test" };
        
        _objectStore.PutAsync(Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns("stored-key");
        
        var sut = CreateSut();

        // Act
        await sut.PublishAsync("test.subject", message, "msg-123");

        // Assert: Object store should be called to upload the payload
        await _objectStore.Received(1).PutAsync(
            Arg.Is<string>(k => k.StartsWith("claimcheck/test.subject/")),
            Arg.Any<Stream>(),
            Arg.Any<CancellationToken>());

        // Assert: Inner publisher called with ClaimCheckMessage wrapper
        await _innerPublisher.Received(1).PublishAsync(
            "test.subject",
            Arg.Is<ClaimCheckMessage>(c => 
                c.ObjectStoreRef.StartsWith("objstore://claimcheck/test.subject/") &&
                c.OriginalSize == payloadSize),
            "msg-123",
            Arg.Any<CancellationToken>());
    }

    #endregion

    #region Metadata Size Limits Tests

    [Test]
    public async Task PublishAsync_LongTypeName_ShouldHandleGracefully()
    {
        // Arrange: Use a message type with a long assembly-qualified name
        var payloadSize = _options.ThresholdBytes + 1;
        
        // Setup serializer for the long-named type
        _serializer.When(s => s.Serialize(Arg.Any<IBufferWriter<byte>>(), Arg.Any<MessageWithVeryLongNamespaceThatExceedsNormalLengthsForTestingPurposes>()))
            .Do(x =>
            {
                var writer = x.Arg<IBufferWriter<byte>>();
                var span = writer.GetSpan(payloadSize);
                for (int i = 0; i < payloadSize; i++)
                {
                    span[i] = (byte)(i % 256);
                }
                writer.Advance(payloadSize);
            });
        
        _objectStore.PutAsync(Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns("stored-key");
        
        var message = new MessageWithVeryLongNamespaceThatExceedsNormalLengthsForTestingPurposes { Data = "test" };
        var sut = CreateSut();

        // Act
        await sut.PublishAsync("test.subject", message, "msg-123");

        // Assert: ClaimCheckMessage should contain the full type name
        await _innerPublisher.Received(1).PublishAsync(
            "test.subject",
            Arg.Is<ClaimCheckMessage>(c => 
                c.OriginalType != null && 
                c.OriginalType.Contains("MessageWithVeryLongNamespaceThatExceedsNormalLengthsForTestingPurposes")),
            "msg-123",
            Arg.Any<CancellationToken>());
    }

    #endregion

    #region Orphaned Payload Tests

    [Test]
    public async Task PublishAsync_WhenPublishFails_ShouldCleanupOrphanedPayload()
    {
        // Arrange
        var payloadSize = _options.ThresholdBytes + 1;
        SetupSerializerToReturnPayloadOfSize(payloadSize);
        var message = new TestMessage { Data = "test" };
        
        string? capturedObjectKey = null;
        
        _objectStore.PutAsync(Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns(x =>
            {
                capturedObjectKey = x.Arg<string>();
                return Task.FromResult(capturedObjectKey);
            });
        
        // Inner publisher throws exception after PutAsync succeeds
        _innerPublisher.PublishAsync(
            Arg.Any<string>(), 
            Arg.Any<ClaimCheckMessage>(), 
            Arg.Any<string?>(), 
            Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("Simulated publish failure"));
        
        var sut = CreateSut();

        // Act & Assert: Exception should propagate
        var exception = await Should.ThrowAsync<InvalidOperationException>(
            async () => await sut.PublishAsync("test.subject", message, "msg-123"));
        
        exception.Message.ShouldBe("Simulated publish failure");

        // Assert: PutAsync was called (payload was uploaded)
        await _objectStore.Received(1).PutAsync(
            Arg.Any<string>(),
            Arg.Any<Stream>(),
            Arg.Any<CancellationToken>());

        // Assert: DeleteAsync should be called to clean up the orphaned payload
        capturedObjectKey.ShouldNotBeNull();
        await _objectStore.Received(1).DeleteAsync(capturedObjectKey, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PublishAsync_WhenPublishFailsAndCleanupFails_ShouldStillPropagateOriginalException()
    {
        // Arrange
        var payloadSize = _options.ThresholdBytes + 1;
        SetupSerializerToReturnPayloadOfSize(payloadSize);
        var message = new TestMessage { Data = "test" };
        
        _objectStore.PutAsync(Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns("stored-key");
        
        // Inner publisher throws
        _innerPublisher.PublishAsync(
            Arg.Any<string>(), 
            Arg.Any<ClaimCheckMessage>(), 
            Arg.Any<string?>(), 
            Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("Simulated publish failure"));
        
        // Cleanup also fails
        _objectStore.DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new IOException("Simulated cleanup failure"));
        
        var sut = CreateSut();

        // Act & Assert: Original exception should propagate (not the cleanup exception)
        var exception = await Should.ThrowAsync<InvalidOperationException>(
            async () => await sut.PublishAsync("test.subject", message, "msg-123"));
        
        exception.Message.ShouldBe("Simulated publish failure");
    }

    #endregion

    #region Test Message Types

    public class TestMessage
    {
        public string Data { get; set; } = string.Empty;
    }

    public class MessageWithVeryLongNamespaceThatExceedsNormalLengthsForTestingPurposes
    {
        public string Data { get; set; } = string.Empty;
    }

    #endregion
}

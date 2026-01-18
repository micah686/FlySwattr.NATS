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

    #region Production Threshold Boundary Tests (1MB)

    /// <summary>
    /// Verifies that a message of exactly 1,048,576 bytes (1MB) does NOT trigger offloading.
    /// The condition is "payload.Length > ThresholdBytes", so exactly-at-threshold should pass through.
    /// Off-by-one errors here cause protocol violations.
    /// </summary>
    [Test]
    public async Task PublishAsync_MessageExactlyAtProductionThreshold_1MB_ShouldSendInline()
    {
        // Arrange: Use production threshold of 1MB (1,048,576 bytes)
        var productionOptions = new PayloadOffloadingOptions
        {
            ThresholdBytes = 1024 * 1024, // 1MB = 1,048,576 bytes
            ObjectKeyPrefix = "claimcheck"
        };

        var sut = new OffloadingJetStreamPublisher(
            _innerPublisher,
            _objectStore,
            _serializer,
            Options.Create(productionOptions),
            _logger);

        var payloadSize = 1024 * 1024; // Exactly 1MB
        _serializer.When(s => s.Serialize(Arg.Any<IBufferWriter<byte>>(), Arg.Any<TestMessage>()))
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

        var message = new TestMessage { Data = "test" };

        // Act
        await sut.PublishAsync("test.subject", message, "msg-1mb-exact");

        // Assert: Message passes through (NOT offloaded) because condition is strictly greater-than
        await _innerPublisher.Received(1).PublishAsync("test.subject", message, "msg-1mb-exact", Arg.Any<CancellationToken>());
        await _objectStore.DidNotReceive().PutAsync(Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// Verifies that a message of 1,048,577 bytes (1MB + 1 byte) DOES trigger offloading.
    /// This is the critical boundary - one byte over the threshold must trigger the offload path.
    /// </summary>
    [Test]
    public async Task PublishAsync_MessageOneByteAboveProductionThreshold_ShouldOffload()
    {
        // Arrange: Use production threshold of 1MB
        var productionOptions = new PayloadOffloadingOptions
        {
            ThresholdBytes = 1024 * 1024, // 1MB
            ObjectKeyPrefix = "claimcheck"
        };

        var sut = new OffloadingJetStreamPublisher(
            _innerPublisher,
            _objectStore,
            _serializer,
            Options.Create(productionOptions),
            _logger);

        var payloadSize = (1024 * 1024) + 1; // 1MB + 1 byte = 1,048,577 bytes
        _serializer.When(s => s.Serialize(Arg.Any<IBufferWriter<byte>>(), Arg.Any<TestMessage>()))
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

        var message = new TestMessage { Data = "test" };

        // Act
        await sut.PublishAsync("test.subject", message, "msg-1mb-plus-one");

        // Assert: Message SHOULD be offloaded
        await _objectStore.Received(1).PutAsync(
            Arg.Is<string>(k => k.StartsWith("claimcheck/test.subject/")),
            Arg.Any<Stream>(),
            Arg.Any<CancellationToken>());

        await _innerPublisher.Received(1).PublishAsync(
            "test.subject",
            Arg.Is<ClaimCheckMessage>(c => c.OriginalSize == payloadSize),
            "msg-1mb-plus-one",
            Arg.Any<CancellationToken>());
    }

    #endregion

    #region MessageId Passthrough Tests

    /// <summary>
    /// Verifies that the messageId is correctly passed through to the inner publisher
    /// when offloading large payloads. This is critical for JetStream deduplication.
    /// </summary>
    [Test]
    public async Task PublishAsync_WhenOffloading_ShouldPassMessageIdToInnerPublisher()
    {
        // Arrange
        var payloadSize = _options.ThresholdBytes + 1;
        SetupSerializerToReturnPayloadOfSize(payloadSize);
        var message = new TestMessage { Data = "test" };
        var businessKeyMessageId = "order-12345-created";

        _objectStore.PutAsync(Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns("stored-key");

        var sut = CreateSut();

        // Act
        await sut.PublishAsync("test.subject", message, businessKeyMessageId);

        // Assert: The exact messageId must be passed to inner publisher for JetStream deduplication
        await _innerPublisher.Received(1).PublishAsync(
            "test.subject",
            Arg.Any<ClaimCheckMessage>(),
            Arg.Is<string?>(id => id == businessKeyMessageId),
            Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// Verifies that null messageId is also correctly passed through.
    /// While the inner publisher may enforce messageId requirement, this decorator should not modify it.
    /// </summary>
    [Test]
    public async Task PublishAsync_WhenOffloading_WithNullMessageId_ShouldPassNullToInnerPublisher()
    {
        // Arrange
        var payloadSize = _options.ThresholdBytes + 1;
        SetupSerializerToReturnPayloadOfSize(payloadSize);
        var message = new TestMessage { Data = "test" };

        _objectStore.PutAsync(Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns("stored-key");

        var sut = CreateSut();

        // Act
        await sut.PublishAsync("test.subject", message, null);

        // Assert: null messageId should be passed through unchanged
        await _innerPublisher.Received(1).PublishAsync(
            "test.subject",
            Arg.Any<ClaimCheckMessage>(),
            Arg.Is<string?>(id => id == null),
            Arg.Any<CancellationToken>());
    }

    #endregion

    #region ClaimCheckMessage Format Validation

    /// <summary>
    /// Verifies the ObjectStoreRef format follows the expected pattern: "objstore://prefix/subject/guid"
    /// Protocol violations here could cause consumer-side deserialization failures.
    /// </summary>
    [Test]
    public async Task PublishAsync_WhenOffloading_ShouldProduceCorrectObjectStoreRefFormat()
    {
        // Arrange
        var payloadSize = _options.ThresholdBytes + 1;
        SetupSerializerToReturnPayloadOfSize(payloadSize);
        var message = new TestMessage { Data = "test" };
        ClaimCheckMessage? capturedClaimCheck = null;

        _objectStore.PutAsync(Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns("stored-key");

        _innerPublisher.PublishAsync(
            Arg.Any<string>(),
            Arg.Any<ClaimCheckMessage>(),
            Arg.Any<string?>(),
            Arg.Any<CancellationToken>())
            .Returns(x =>
            {
                capturedClaimCheck = x.ArgAt<ClaimCheckMessage>(1);
                return Task.CompletedTask;
            });

        var sut = CreateSut();

        // Act
        await sut.PublishAsync("orders.created", message, "msg-123");

        // Assert: ObjectStoreRef format validation
        capturedClaimCheck.ShouldNotBeNull();

        // Should start with objstore:// protocol
        capturedClaimCheck.ObjectStoreRef.ShouldStartWith("objstore://");

        // Should contain the configured prefix
        capturedClaimCheck.ObjectStoreRef.ShouldContain("claimcheck/");

        // Should contain the subject
        capturedClaimCheck.ObjectStoreRef.ShouldContain("orders.created/");

        // Should end with a GUID (32 hex characters without dashes due to :N format)
        var parts = capturedClaimCheck.ObjectStoreRef.Split('/');
        var guidPart = parts[^1]; // Last segment
        guidPart.Length.ShouldBe(32);
        Guid.TryParse(guidPart, out _).ShouldBeTrue();
    }

    /// <summary>
    /// Verifies that OriginalType correctly captures the assembly-qualified type name.
    /// This is used by consumers for type-safe deserialization.
    /// </summary>
    [Test]
    public async Task PublishAsync_WhenOffloading_ShouldCaptureCorrectOriginalType()
    {
        // Arrange
        var payloadSize = _options.ThresholdBytes + 1;
        SetupSerializerToReturnPayloadOfSize(payloadSize);
        var message = new TestMessage { Data = "test" };
        ClaimCheckMessage? capturedClaimCheck = null;

        _objectStore.PutAsync(Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns("stored-key");

        _innerPublisher.PublishAsync(
            Arg.Any<string>(),
            Arg.Any<ClaimCheckMessage>(),
            Arg.Any<string?>(),
            Arg.Any<CancellationToken>())
            .Returns(x =>
            {
                capturedClaimCheck = x.ArgAt<ClaimCheckMessage>(1);
                return Task.CompletedTask;
            });

        var sut = CreateSut();

        // Act
        await sut.PublishAsync("test.subject", message, "msg-123");

        // Assert: OriginalType should identify the message type
        capturedClaimCheck.ShouldNotBeNull();
        capturedClaimCheck.OriginalType.ShouldNotBeNullOrWhiteSpace();
        capturedClaimCheck.OriginalType.ShouldContain(nameof(TestMessage));
    }

    #endregion

    #region Idempotency Without MessageId Tests

    /// <summary>
    /// Verifies that calling PublishAsync without messageId throws ArgumentException.
    /// This enforces application-level idempotency requirements.
    /// </summary>
    [Test]
    public async Task PublishAsync_WithoutMessageId_ShouldThrowArgumentException()
    {
        // Arrange
        var message = new TestMessage { Data = "test" };
        var sut = CreateSut();

        // Act & Assert
        var exception = await Should.ThrowAsync<ArgumentException>(
            async () => await sut.PublishAsync("test.subject", message));

        exception.Message.ShouldContain("messageId must be provided");
    }

    #endregion

    #region Concurrent Offloading Tests

    /// <summary>
    /// Verifies that concurrent publishes generate unique object keys.
    /// Each offloaded payload must have a unique key to prevent collisions.
    /// </summary>
    [Test]
    public async Task PublishAsync_ConcurrentOffloads_ShouldGenerateUniqueObjectKeys()
    {
        // Arrange
        var payloadSize = _options.ThresholdBytes + 1;
        SetupSerializerToReturnPayloadOfSize(payloadSize);

        var capturedKeys = new System.Collections.Concurrent.ConcurrentBag<string>();

        _objectStore.PutAsync(Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns(x =>
            {
                var key = x.ArgAt<string>(0);
                capturedKeys.Add(key);
                return Task.FromResult(key);
            });

        var sut = CreateSut();
        var tasks = new List<Task>();

        // Act: Launch 10 concurrent publish operations
        for (int i = 0; i < 10; i++)
        {
            var message = new TestMessage { Data = $"test-{i}" };
            var messageId = $"msg-{i}";
            tasks.Add(sut.PublishAsync("test.subject", message, messageId));
        }

        await Task.WhenAll(tasks);

        // Assert: All 10 keys should be unique
        capturedKeys.Count.ShouldBe(10);
        capturedKeys.Distinct().Count().ShouldBe(10);
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

using System.Buffers;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Abstractions.Exceptions;
using FlySwattr.NATS.Core;
using FlySwattr.NATS.Hosting.Services;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Hosting.Services;

/// <summary>
/// Unit tests for DefaultDlqPoisonHandler covering:
/// - Resilience when IDlqStore/Publisher throws exceptions
/// - Serialization failure handling (stores empty payload)
/// - Header preservation in DLQ entries
/// </summary>
[Property("nTag", "Hosting")]
public class DefaultDlqPoisonHandlerTests
{
    #region Test Fixtures

    private IJetStreamPublisher _publisher = null!;
    private IMessageSerializer _serializer = null!;
    private IObjectStore _objectStore = null!;
    private IDlqNotificationService _notificationService = null!;
    private IDlqPolicyRegistry _policyRegistry = null!;
    private ILogger<DefaultDlqPoisonHandler<TestMessage>> _logger = null!;
    private IJsMessageContext<TestMessage> _context = null!;

    [Before(Test)]
    public void Setup()
    {
        _publisher = Substitute.For<IJetStreamPublisher>();
        _serializer = Substitute.For<IMessageSerializer>();
        _objectStore = Substitute.For<IObjectStore>();
        _notificationService = Substitute.For<IDlqNotificationService>();
        _policyRegistry = Substitute.For<IDlqPolicyRegistry>();
        _logger = Substitute.For<ILogger<DefaultDlqPoisonHandler<TestMessage>>>();
        _logger.IsEnabled(default).ReturnsForAnyArgs(true);
        _context = CreateMockContext();
    }

    private IJsMessageContext<TestMessage> CreateMockContext(
        Dictionary<string, string>? headers = null,
        uint numDelivered = 5,
        ulong sequence = 42)
    {
        var context = Substitute.For<IJsMessageContext<TestMessage>>();
        context.Message.Returns(new TestMessage { Id = 1, Name = "Test" });
        context.Subject.Returns("orders.created");
        context.Sequence.Returns(sequence);
        context.NumDelivered.Returns(numDelivered);
        context.Headers.Returns(new MessageHeaders(headers ?? new Dictionary<string, string>
        {
            ["TraceID"] = "trace-abc123",
            ["CorrelationID"] = "corr-xyz789"
        }));
        return context;
    }

    private DefaultDlqPoisonHandler<TestMessage> CreateHandler(
        IJetStreamPublisher? publisher = null,
        IMessageSerializer? serializer = null,
        IObjectStore? objectStore = null,
        IDlqNotificationService? notificationService = null)
    {
        return new DefaultDlqPoisonHandler<TestMessage>(
            publisher ?? _publisher,
            serializer ?? _serializer,
            objectStore ?? _objectStore,
            notificationService ?? _notificationService,
            _policyRegistry,
            _logger);
    }

    #endregion

    #region 1. Resilience of the Safety Net

    /// <summary>
    /// When the DLQ publisher throws an exception (e.g., NATS KV is full),
    /// the handler should degrade gracefully by NAKing with a 30-second delay
    /// rather than throwing an unhandled exception.
    /// </summary>
    [Test]
    public async Task HandleAsync_WhenPublisherThrows_ShouldNakWithBackoff()
    {
        // Arrange
        var policy = new DeadLetterPolicy { SourceStream = "orders-stream", SourceConsumer = "orders-consumer", TargetStream = StreamName.From("dlq"), TargetSubject = "dlq.orders" };
        _policyRegistry.Get("orders-stream", "orders-consumer").Returns(policy);
        
        _serializer.Serialize(Arg.Any<IBufferWriter<byte>>(), Arg.Any<TestMessage>());
        _serializer.GetContentType<TestMessage>().Returns("application/json");
        
        // Simulate DLQ store failure
        _publisher.PublishAsync(
            Arg.Any<string>(), 
            Arg.Any<DlqMessage>(), 
            Arg.Any<string>(), 
            Arg.Any<CancellationToken>())
            .ThrowsAsync(new IOException("NATS KV store full"));

        var handler = CreateHandler();
        var exception = new InvalidOperationException("Processing failed");

        // Act - should NOT throw
        await handler.HandleAsync(
            _context,
            "orders-stream",
            "orders-consumer",
            maxDeliveries: 3,
            exception,
            CancellationToken.None);

        // Assert - NAK with 30 second backoff
        await _context.Received(1).NackAsync(TimeSpan.FromSeconds(30), Arg.Any<CancellationToken>());
        await _context.DidNotReceive().TermAsync(Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// When the notification service throws, the DLQ process should still complete.
    /// Notification failure should be logged but not prevent message termination.
    /// </summary>
    [Test]
    public async Task HandleAsync_WhenNotificationThrows_ShouldStillTermMessage()
    {
        // Arrange
        var policy = new DeadLetterPolicy { SourceStream = "orders-stream", SourceConsumer = "orders-consumer", TargetStream = StreamName.From("dlq"), TargetSubject = "dlq.orders" };
        _policyRegistry.Get("orders-stream", "orders-consumer").Returns(policy);
        
        _serializer.Serialize(Arg.Any<IBufferWriter<byte>>(), Arg.Any<TestMessage>());
        _serializer.GetContentType<TestMessage>().Returns("application/json");
        
        _publisher.PublishAsync(
            Arg.Any<string>(), 
            Arg.Any<DlqMessage>(), 
            Arg.Any<string>(), 
            Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        // Notification fails
        _notificationService.NotifyAsync(Arg.Any<DlqNotification>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new Exception("Notification service unavailable"));

        var handler = CreateHandler();
        var exception = new InvalidOperationException("Processing failed");

        // Act
        await handler.HandleAsync(
            _context,
            "orders-stream",
            "orders-consumer",
            maxDeliveries: 3,
            exception,
            CancellationToken.None);

        // Assert - message should still be terminated
        await _context.Received(1).TermAsync(Arg.Any<CancellationToken>());
    }

    #endregion

    #region 2. Serialization Failure Handling

    /// <summary>
    /// When the message cannot be re-serialized for DLQ storage,
    /// the handler should store empty bytes with appropriate metadata
    /// rather than failing entirely.
    /// </summary>
    [Test]
    public async Task HandleAsync_WhenSerializationFails_ShouldStoreEmptyPayloadWithMetadata()
    {
        // Arrange
        var policy = new DeadLetterPolicy { SourceStream = "orders-stream", SourceConsumer = "orders-consumer", TargetStream = StreamName.From("dlq"), TargetSubject = "dlq.orders" };
        _policyRegistry.Get("orders-stream", "orders-consumer").Returns(policy);
        
        // Serialization throws
        _serializer.When(x => x.Serialize(Arg.Any<IBufferWriter<byte>>(), Arg.Any<TestMessage>()))
            .Do(_ => throw new InvalidOperationException("Cannot serialize corrupted object"));

        DlqMessage? capturedDlqMessage = null;
        _publisher.PublishAsync(
            Arg.Any<string>(), 
            Arg.Any<DlqMessage>(), 
            Arg.Any<string>(), 
            Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                capturedDlqMessage = callInfo.Arg<DlqMessage>();
                return Task.CompletedTask;
            });

        var handler = CreateHandler();
        var exception = new InvalidOperationException("Processing failed");

        // Act
        await handler.HandleAsync(
            _context,
            "orders-stream",
            "orders-consumer",
            maxDeliveries: 3,
            exception,
            CancellationToken.None);

        // Assert - DLQ message should have empty payload with fallback metadata
        capturedDlqMessage.ShouldNotBeNull();
        capturedDlqMessage.Payload.ShouldBeEmpty();
        capturedDlqMessage.PayloadEncoding.ShouldBe("application/octet-stream");
        capturedDlqMessage.SerializerType.ShouldBe("Failed");
        
        // But should still preserve other metadata
        capturedDlqMessage.OriginalStream.ShouldBe("orders-stream");
        capturedDlqMessage.OriginalConsumer.ShouldBe("orders-consumer");
        capturedDlqMessage.OriginalSequence.ShouldBe(42ul);
    }

    #endregion

    #region 3. Header Preservation

    /// <summary>
    /// Original message headers (TraceID, CorrelationID, etc.) must be preserved
    /// in the DLQ entry to aid debugging.
    /// </summary>
    [Test]
    public async Task HandleAsync_ShouldPreserveOriginalHeaders_InDlqMessage()
    {
        // Arrange
        var originalHeaders = new Dictionary<string, string>
        {
            ["TraceID"] = "trace-12345",
            ["CorrelationID"] = "corr-67890",
            ["X-Custom-Header"] = "custom-value"
        };
        var contextWithHeaders = CreateMockContext(headers: originalHeaders);
        
        var policy = new DeadLetterPolicy { SourceStream = "orders-stream", SourceConsumer = "orders-consumer", TargetStream = StreamName.From("dlq"), TargetSubject = "dlq.orders" };
        _policyRegistry.Get("orders-stream", "orders-consumer").Returns(policy);
        
        _serializer.Serialize(Arg.Any<IBufferWriter<byte>>(), Arg.Any<TestMessage>());
        _serializer.GetContentType<TestMessage>().Returns("application/json");

        DlqMessage? capturedDlqMessage = null;
        _publisher.PublishAsync(
            Arg.Any<string>(), 
            Arg.Any<DlqMessage>(), 
            Arg.Any<string>(), 
            Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                capturedDlqMessage = callInfo.Arg<DlqMessage>();
                return Task.CompletedTask;
            });

        var handler = CreateHandler();
        var exception = new InvalidOperationException("Processing failed");

        // Act
        await handler.HandleAsync(
            contextWithHeaders,
            "orders-stream",
            "orders-consumer",
            maxDeliveries: 3,
            exception,
            CancellationToken.None);

        // Assert - All headers preserved
        capturedDlqMessage.ShouldNotBeNull();
        capturedDlqMessage.OriginalHeaders.ShouldNotBeNull();
        capturedDlqMessage.OriginalHeaders.Count.ShouldBe(3);
        capturedDlqMessage.OriginalHeaders["TraceID"].ShouldBe("trace-12345");
        capturedDlqMessage.OriginalHeaders["CorrelationID"].ShouldBe("corr-67890");
        capturedDlqMessage.OriginalHeaders["X-Custom-Header"].ShouldBe("custom-value");
    }

    #endregion

    #region Additional Coverage

    /// <summary>
    /// When exception is MessageValidationException, DLQ should be triggered immediately
    /// regardless of delivery count.
    /// </summary>
    [Test]
    public async Task HandleAsync_ValidationFailure_ShouldRouteToDlqImmediately()
    {
        // Arrange - first delivery
        var context = CreateMockContext(numDelivered: 1);
        
        var policy = new DeadLetterPolicy { SourceStream = "orders-stream", SourceConsumer = "orders-consumer", TargetStream = StreamName.From("dlq"), TargetSubject = "dlq.orders" };
        _policyRegistry.Get("orders-stream", "orders-consumer").Returns(policy);
        
        _serializer.Serialize(Arg.Any<IBufferWriter<byte>>(), Arg.Any<TestMessage>());
        _serializer.GetContentType<TestMessage>().Returns("application/json");
        
        _publisher.PublishAsync(
            Arg.Any<string>(), 
            Arg.Any<DlqMessage>(), 
            Arg.Any<string>(), 
            Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var handler = CreateHandler();
        var validationException = new MessageValidationException("orders.created", new[] { "Invalid order format" });

        // Act
        await handler.HandleAsync(
            context,
            "orders-stream",
            "orders-consumer",
            maxDeliveries: 5,  // Not exhausted
            validationException,
            CancellationToken.None);

        // Assert - should go to DLQ immediately, not NAK
        await _publisher.Received(1).PublishAsync(
            "dlq.orders",
            Arg.Any<DlqMessage>(),
            Arg.Is<string>(id => id.StartsWith("dlq-validation-")),
            Arg.Any<CancellationToken>());
        await context.Received(1).TermAsync(Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// When no DLQ policy/publisher/serializer is available,
    /// the message should be terminated (best effort) rather than NAKed forever.
    /// </summary>
    [Test]
    public async Task HandleAsync_NoDlqAvailable_ShouldTerminateMessage()
    {
        // Arrange - no policy registered
        _policyRegistry.Get("orders-stream", "orders-consumer").Returns((DeadLetterPolicy?)null);

        var handler = CreateHandler();
        var exception = new InvalidOperationException("Processing failed");

        // Act
        await handler.HandleAsync(
            _context,
            "orders-stream",
            "orders-consumer",
            maxDeliveries: 3,
            exception,
            CancellationToken.None);

        // Assert - should terminate, not NAK
        await _context.Received(1).TermAsync(Arg.Any<CancellationToken>());
        await _publisher.DidNotReceive().PublishAsync(
            Arg.Any<string>(), 
            Arg.Any<DlqMessage>(), 
            Arg.Any<string>(), 
            Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// When delivery count has NOT been exhausted, the handler should NAK with backoff
    /// for transient failures (not validation errors).
    /// </summary>
    [Test]
    public async Task HandleAsync_TransientFailure_ShouldNakWithExponentialBackoff()
    {
        // Arrange - only 2nd delivery, max is 5
        var context = CreateMockContext(numDelivered: 2);
        
        var handler = CreateHandler();
        var exception = new TimeoutException("Database timeout");

        // Act
        await handler.HandleAsync(
            context,
            "orders-stream",
            "orders-consumer",
            maxDeliveries: 5,
            exception,
            CancellationToken.None);

        // Assert - NAK with exponential backoff (2^(2-1) = 2 seconds)
        await context.Received(1).NackAsync(
            Arg.Is<TimeSpan>(ts => ts.TotalSeconds >= 1 && ts.TotalSeconds <= 3), 
            Arg.Any<CancellationToken>());
        await _publisher.DidNotReceive().PublishAsync(
            Arg.Any<string>(), 
            Arg.Any<DlqMessage>(), 
            Arg.Any<string>(), 
            Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// DLQ message ID should be deterministic based on stream/consumer/sequence
    /// for idempotency.
    /// </summary>
    [Test]
    public async Task HandleAsync_ShouldUseDeterministicMessageId_ForIdempotency()
    {
        // Arrange
        var context = CreateMockContext(sequence: 12345);
        
        var policy = new DeadLetterPolicy { SourceStream = "orders-stream", SourceConsumer = "orders-consumer", TargetStream = StreamName.From("dlq"), TargetSubject = "dlq.orders" };
        _policyRegistry.Get("orders-stream", "orders-consumer").Returns(policy);
        
        _serializer.Serialize(Arg.Any<IBufferWriter<byte>>(), Arg.Any<TestMessage>());
        _serializer.GetContentType<TestMessage>().Returns("application/json");
        
        _publisher.PublishAsync(
            Arg.Any<string>(), 
            Arg.Any<DlqMessage>(), 
            Arg.Any<string>(), 
            Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var handler = CreateHandler();
        var exception = new InvalidOperationException("Processing failed");

        // Act
        await handler.HandleAsync(
            context,
            "orders-stream",
            "orders-consumer",
            maxDeliveries: 3,
            exception,
            CancellationToken.None);

        // Assert - deterministic ID format
        await _publisher.Received(1).PublishAsync(
            "dlq.orders",
            Arg.Any<DlqMessage>(),
            "dlq-orders-stream-orders-consumer-12345",
            Arg.Any<CancellationToken>());
    }

    #endregion

    #region Large Payload Handling

    /// <summary>
    /// When payload exceeds MaxDlqPayloadSize (1MB) and ObjectStore is available,
    /// the handler should offload the payload to ObjectStore and store a reference.
    /// </summary>
    [Test]
    public async Task HandleAsync_LargePayload_ShouldOffloadToObjectStore()
    {
        // Arrange
        var context = CreateMockContext(sequence: 99);
        
        var policy = new DeadLetterPolicy { SourceStream = "orders-stream", SourceConsumer = "orders-consumer", TargetStream = StreamName.From("dlq"), TargetSubject = "dlq.orders" };
        _policyRegistry.Get("orders-stream", "orders-consumer").Returns(policy);
        
        // Simulate serializer writing a payload larger than 1MB (1024 * 1024 bytes)
        var largePayloadSize = 1024 * 1024 + 1000; // Just over 1MB
        _serializer.When(x => x.Serialize(Arg.Any<IBufferWriter<byte>>(), Arg.Any<TestMessage>()))
            .Do(callInfo =>
            {
                var bufferWriter = callInfo.ArgAt<IBufferWriter<byte>>(0);
                var buffer = bufferWriter.GetSpan(largePayloadSize);
                buffer.Slice(0, largePayloadSize).Fill(0xAB);
                bufferWriter.Advance(largePayloadSize);
            });
        _serializer.GetContentType<TestMessage>().Returns("application/json");
        
        string? capturedObjectKey = null;
        _objectStore.PutAsync(Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                capturedObjectKey = callInfo.ArgAt<string>(0);
                return Task.FromResult("rev-1");
            });

        DlqMessage? capturedDlqMessage = null;
        _publisher.PublishAsync(
            Arg.Any<string>(), 
            Arg.Any<DlqMessage>(), 
            Arg.Any<string>(), 
            Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                capturedDlqMessage = callInfo.Arg<DlqMessage>();
                return Task.CompletedTask;
            });

        var handler = CreateHandler();
        var exception = new InvalidOperationException("Processing failed");

        // Act
        await handler.HandleAsync(
            context,
            "orders-stream",
            "orders-consumer",
            maxDeliveries: 3,
            exception,
            CancellationToken.None);

        // Assert - ObjectStore should have been called with correct key pattern
        await _objectStore.Received(1).PutAsync(
            Arg.Is<string>(key => key.StartsWith("dlq-payload/orders-stream/orders-consumer/99-")),
            Arg.Any<Stream>(),
            Arg.Any<CancellationToken>());
        
        capturedObjectKey.ShouldNotBeNull();
        capturedObjectKey.ShouldStartWith("dlq-payload/orders-stream/orders-consumer/99-");
        
        // DLQ message should have empty payload with objstore reference
        capturedDlqMessage.ShouldNotBeNull();
        capturedDlqMessage.Payload.ShouldBeEmpty();
        capturedDlqMessage.PayloadEncoding.ShouldStartWith("objstore://dlq-payload/");
    }

    /// <summary>
    /// When payload exceeds MaxDlqPayloadSize (1MB) but ObjectStore is NOT available,
    /// the handler should truncate the payload and log a warning.
    /// </summary>
    [Test]
    public async Task HandleAsync_LargePayloadWithoutObjectStore_ShouldTruncateAndLogWarning()
    {
        // Arrange
        var context = CreateMockContext(sequence: 100);
        
        var policy = new DeadLetterPolicy { SourceStream = "orders-stream", SourceConsumer = "orders-consumer", TargetStream = StreamName.From("dlq"), TargetSubject = "dlq.orders" };
        _policyRegistry.Get("orders-stream", "orders-consumer").Returns(policy);
        
        // Simulate serializer writing a payload larger than 1MB
        var largePayloadSize = 1024 * 1024 + 5000; // Just over 1MB
        _serializer.When(x => x.Serialize(Arg.Any<IBufferWriter<byte>>(), Arg.Any<TestMessage>()))
            .Do(callInfo =>
            {
                var bufferWriter = callInfo.ArgAt<IBufferWriter<byte>>(0);
                var buffer = bufferWriter.GetSpan(largePayloadSize);
                buffer.Slice(0, largePayloadSize).Fill(0xCD);
                bufferWriter.Advance(largePayloadSize);
            });
        _serializer.GetContentType<TestMessage>().Returns("application/json");

        DlqMessage? capturedDlqMessage = null;
        _publisher.PublishAsync(
            Arg.Any<string>(), 
            Arg.Any<DlqMessage>(), 
            Arg.Any<string>(), 
            Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                capturedDlqMessage = callInfo.Arg<DlqMessage>();
                return Task.CompletedTask;
            });

        // Create handler WITHOUT ObjectStore explicitly
        // Note: We cannot use CreateHandler(objectStore: null) because it coalesces with the mock field
        var handler = new DefaultDlqPoisonHandler<TestMessage>(
            _publisher,
            _serializer,
            null, // Explicitly null
            _notificationService,
            _policyRegistry,
            _logger);
            
        var exception = new InvalidOperationException("Processing failed");

        // Act
        await handler.HandleAsync(
            context,
            "orders-stream",
            "orders-consumer",
            maxDeliveries: 3,
            exception,
            CancellationToken.None);

        // Assert - DLQ message should have empty payload with truncation indicator
        capturedDlqMessage.ShouldNotBeNull();
        capturedDlqMessage.Payload.ShouldBeEmpty();
        capturedDlqMessage.PayloadEncoding.ShouldStartWith("truncated:size=");
        capturedDlqMessage.PayloadEncoding.ShouldContain(largePayloadSize.ToString());
        
        // Message should still be terminated
        await context.Received(1).TermAsync(Arg.Any<CancellationToken>());
    }

    #endregion
}

/// <summary>
/// Test message type for DLQ handler tests.
/// </summary>
public class TestMessage
{
    public int Id { get; set; }
    public string? Name { get; set; }
}

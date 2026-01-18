using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Hosting.Services;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Hosting.Services;

/// <summary>
/// Unit tests for DlqStoreAdvisoryHandler covering:
/// - Property mapping from ConsumerMaxDeliveriesAdvisory to DlqMessageEntry
/// - IDlqStore interaction
/// - Error handling when store throws
/// </summary>
[Property("nTag", "Hosting")]
public class DlqStoreAdvisoryHandlerTests
{
    private IDlqStore _dlqStore = null!;
    private ILogger<DlqStoreAdvisoryHandler> _logger = null!;

    [Before(Test)]
    public void Setup()
    {
        _dlqStore = Substitute.For<IDlqStore>();
        _logger = Substitute.For<ILogger<DlqStoreAdvisoryHandler>>();
    }

    private DlqStoreAdvisoryHandler CreateHandler() => new(_dlqStore, _logger);

    #region Property Mapping Tests

    /// <summary>
    /// Verifies that all properties from ConsumerMaxDeliveriesAdvisory are correctly
    /// mapped to DlqMessageEntry fields.
    /// </summary>
    [Test]
    public async Task HandleMaxDeliveriesExceededAsync_ShouldMapAdvisoryPropertiesToEntry()
    {
        // Arrange
        var timestamp = DateTimeOffset.UtcNow;
        var advisory = new ConsumerMaxDeliveriesAdvisory(
            Type: "io.nats.jetstream.advisory.v1.max_deliver",
            Id: "advisory-123",
            Timestamp: timestamp,
            Stream: "orders-stream",
            Consumer: "orders-consumer",
            StreamSeq: 12345,
            Deliveries: 5,
            Domain: "test-domain"
        );

        DlqMessageEntry? capturedEntry = null;
        _dlqStore.StoreAsync(Arg.Any<DlqMessageEntry>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                capturedEntry = callInfo.Arg<DlqMessageEntry>();
                return Task.CompletedTask;
            });

        var handler = CreateHandler();

        // Act
        await handler.HandleMaxDeliveriesExceededAsync(advisory, CancellationToken.None);

        // Assert - Verify all property mappings
        capturedEntry.ShouldNotBeNull();
        
        // Id format: {stream}.{consumer}.{streamSeq}
        capturedEntry.Id.ShouldBe("orders-stream.orders-consumer.12345");
        
        capturedEntry.OriginalStream.ShouldBe("orders-stream");
        capturedEntry.OriginalConsumer.ShouldBe("orders-consumer");
        
        // OriginalSubject is approximated from stream.consumer since advisory doesn't include it
        capturedEntry.OriginalSubject.ShouldBe("orders-stream.orders-consumer");
        
        capturedEntry.OriginalSequence.ShouldBe(12345ul);
        capturedEntry.DeliveryCount.ShouldBe(5);
        capturedEntry.StoredAt.ShouldBe(timestamp);
        capturedEntry.ErrorReason.ShouldBe("MaxDeliver limit exceeded (server-side advisory)");
        capturedEntry.Status.ShouldBe(DlqMessageStatus.Pending);
        
        // Advisory-based entries don't have payload (server-side only has metadata)
        capturedEntry.Payload.ShouldBeNull();
    }

    /// <summary>
    /// Verifies that the handler stores the entry in the DLQ store.
    /// </summary>
    [Test]
    public async Task HandleMaxDeliveriesExceededAsync_ShouldStoreEntryInDlqStore()
    {
        // Arrange
        var advisory = new ConsumerMaxDeliveriesAdvisory(
            Type: "io.nats.jetstream.advisory.v1.max_deliver",
            Id: "advisory-456",
            Timestamp: DateTimeOffset.UtcNow,
            Stream: "payments-stream",
            Consumer: "payments-consumer",
            StreamSeq: 999,
            Deliveries: 3
        );

        _dlqStore.StoreAsync(Arg.Any<DlqMessageEntry>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var handler = CreateHandler();

        // Act
        await handler.HandleMaxDeliveriesExceededAsync(advisory, CancellationToken.None);

        // Assert - Store should have been called exactly once
        await _dlqStore.Received(1).StoreAsync(
            Arg.Is<DlqMessageEntry>(e => 
                e.OriginalStream == "payments-stream" && 
                e.OriginalConsumer == "payments-consumer" &&
                e.OriginalSequence == 999ul),
            Arg.Any<CancellationToken>());
    }

    #endregion

    #region Error Handling Tests

    /// <summary>
    /// When the DLQ store throws an exception, the handler should catch it
    /// and log the error rather than propagating the exception.
    /// </summary>
    [Test]
    public async Task HandleMaxDeliveriesExceededAsync_WhenStoreThrows_ShouldLogErrorAndNotRethrow()
    {
        // Arrange
        var advisory = new ConsumerMaxDeliveriesAdvisory(
            Type: "io.nats.jetstream.advisory.v1.max_deliver",
            Id: "advisory-error",
            Timestamp: DateTimeOffset.UtcNow,
            Stream: "error-stream",
            Consumer: "error-consumer",
            StreamSeq: 1,
            Deliveries: 1
        );

        _dlqStore.StoreAsync(Arg.Any<DlqMessageEntry>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new IOException("NATS KV store unavailable"));

        var handler = CreateHandler();

        // Act - Should NOT throw
        await handler.HandleMaxDeliveriesExceededAsync(advisory, CancellationToken.None);

        // Assert - Verify store was attempted
        await _dlqStore.Received(1).StoreAsync(
            Arg.Any<DlqMessageEntry>(),
            Arg.Any<CancellationToken>());
        
        // The handler handles the exception internally (logs it)
        // If we got here without exception, the test passes
    }

    /// <summary>
    /// Verifies that even with null Domain in advisory, the handler works correctly.
    /// </summary>
    [Test]
    public async Task HandleMaxDeliveriesExceededAsync_WithNullDomain_ShouldStoreEntry()
    {
        // Arrange
        var advisory = new ConsumerMaxDeliveriesAdvisory(
            Type: "io.nats.jetstream.advisory.v1.max_deliver",
            Id: "advisory-no-domain",
            Timestamp: DateTimeOffset.UtcNow,
            Stream: "simple-stream",
            Consumer: "simple-consumer",
            StreamSeq: 42,
            Deliveries: 2,
            Domain: null // Explicitly null
        );

        DlqMessageEntry? capturedEntry = null;
        _dlqStore.StoreAsync(Arg.Any<DlqMessageEntry>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                capturedEntry = callInfo.Arg<DlqMessageEntry>();
                return Task.CompletedTask;
            });

        var handler = CreateHandler();

        // Act
        await handler.HandleMaxDeliveriesExceededAsync(advisory, CancellationToken.None);

        // Assert
        capturedEntry.ShouldNotBeNull();
        capturedEntry.Id.ShouldBe("simple-stream.simple-consumer.42");
    }

    #endregion
}

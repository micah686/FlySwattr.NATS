using System.Text;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Services;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Core.Services;

/// <summary>
/// Comprehensive tests for NatsDlqRemediationService.
/// This is the "break glass in case of emergency" tool for operations teams.
/// Tests cover: payload resolution, replay idempotency, transactional integrity, and modification paths.
/// </summary>
[Property("nTag", "Core")]
public class NatsDlqRemediationServiceTests
{
    private readonly IDlqStore _dlqStore;
    private readonly IJetStreamPublisher _publisher;
    private readonly IMessageSerializer _serializer;
    private readonly IObjectStore _objectStore;
    private readonly ILogger<NatsDlqRemediationService> _logger;
    private readonly NatsDlqRemediationService _sut;

    public NatsDlqRemediationServiceTests()
    {
        _dlqStore = Substitute.For<IDlqStore>();
        _publisher = Substitute.For<IJetStreamPublisher>();
        _serializer = Substitute.For<IMessageSerializer>();
        _objectStore = Substitute.For<IObjectStore>();
        _logger = Substitute.For<ILogger<NatsDlqRemediationService>>();

        _sut = new NatsDlqRemediationService(
            _dlqStore,
            _publisher,
            _serializer,
            _logger,
            _objectStore);
    }

    #region Payload Resolution Tests

    [Test]
    public async Task ReplayAsync_ReturnsInlinePayload_WhenPayloadExists()
    {
        // Arrange: Entry has inline payload
        var entryId = "stream.consumer.msg-1";
        var inlinePayload = Encoding.UTF8.GetBytes("{\"orderId\":\"123\"}");
        var entry = CreateTestEntry(entryId, payload: inlinePayload);
        
        _dlqStore.GetAsync(entryId, Arg.Any<CancellationToken>()).Returns(entry);
        _dlqStore.UpdateStatusAsync(Arg.Any<string>(), Arg.Any<DlqMessageStatus>(), Arg.Any<CancellationToken>())
            .Returns(true);

        // Act
        var result = await _sut.ReplayAsync(entryId);

        // Assert: Publisher should receive the inline payload
        result.Success.ShouldBeTrue();
        await _publisher.Received(1).PublishAsync(
            entry.OriginalSubject,
            Arg.Is<byte[]>(p => p.SequenceEqual(inlinePayload)),
            Arg.Any<string>(),
            Arg.Any<CancellationToken>());
        
        // Object store should NOT be called for inline payloads
        await _objectStore.DidNotReceive().GetAsync(
            Arg.Any<string>(), 
            Arg.Any<Stream>(), 
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ReplayAsync_RetrievesFromObjectStore_WhenObjstorePrefixPresent()
    {
        // Arrange: Entry has objstore:// reference in PayloadEncoding
        var entryId = "stream.consumer.msg-2";
        var objectKey = "dlq-payload-abc123";
        var offloadedPayload = Encoding.UTF8.GetBytes("{\"largeData\":\"...\"}");
        var entry = CreateTestEntry(
            entryId, 
            payload: null,  // No inline payload
            payloadEncoding: $"objstore://{objectKey}");

        _dlqStore.GetAsync(entryId, Arg.Any<CancellationToken>()).Returns(entry);
        _dlqStore.UpdateStatusAsync(Arg.Any<string>(), Arg.Any<DlqMessageStatus>(), Arg.Any<CancellationToken>())
            .Returns(true);

        // Mock object store to write payload to the provided stream
        _objectStore.GetAsync(objectKey, Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var stream = callInfo.ArgAt<Stream>(1);
                stream.Write(offloadedPayload, 0, offloadedPayload.Length);
                stream.Position = 0;
                return Task.CompletedTask;
            });

        // Act
        var result = await _sut.ReplayAsync(entryId);

        // Assert: Object store was queried with correct key
        result.Success.ShouldBeTrue();
        await _objectStore.Received(1).GetAsync(
            objectKey, 
            Arg.Any<Stream>(), 
            Arg.Any<CancellationToken>());
        
        // Publisher should receive the offloaded payload
        await _publisher.Received(1).PublishAsync(
            entry.OriginalSubject,
            Arg.Is<byte[]>(p => p.SequenceEqual(offloadedPayload)),
            Arg.Any<string>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ReplayAsync_RetrievesFromObjectStore_WhenHeaderRefPresent()
    {
        // Arrange: Entry has x-dlq-payload-ref header (fallback mechanism)
        var entryId = "stream.consumer.msg-3";
        var objectKey = "header-ref-payload-xyz";
        var offloadedPayload = Encoding.UTF8.GetBytes("{\"headerBased\":true}");
        var headers = new Dictionary<string, string> { { "x-dlq-payload-ref", objectKey } };
        var entry = CreateTestEntry(
            entryId,
            payload: null,
            payloadEncoding: null,  // No objstore:// prefix
            headers: headers);

        _dlqStore.GetAsync(entryId, Arg.Any<CancellationToken>()).Returns(entry);
        _dlqStore.UpdateStatusAsync(Arg.Any<string>(), Arg.Any<DlqMessageStatus>(), Arg.Any<CancellationToken>())
            .Returns(true);

        _objectStore.GetAsync(objectKey, Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var stream = callInfo.ArgAt<Stream>(1);
                stream.Write(offloadedPayload, 0, offloadedPayload.Length);
                stream.Position = 0;
                return Task.CompletedTask;
            });

        // Act
        var result = await _sut.ReplayAsync(entryId);

        // Assert: Object store was queried via header reference
        result.Success.ShouldBeTrue();
        await _objectStore.Received(1).GetAsync(
            objectKey,
            Arg.Any<Stream>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ReplayAsync_ReturnsFailed_WhenNoPayloadSourceAvailable()
    {
        // Arrange: Entry with no inline payload, no objstore reference, no header ref
        var entryId = "stream.consumer.msg-4";
        var entry = CreateTestEntry(
            entryId,
            payload: null,
            payloadEncoding: null,
            headers: null);

        _dlqStore.GetAsync(entryId, Arg.Any<CancellationToken>()).Returns(entry);
        _dlqStore.UpdateStatusAsync(Arg.Any<string>(), Arg.Any<DlqMessageStatus>(), Arg.Any<CancellationToken>())
            .Returns(true);

        // Act
        var result = await _sut.ReplayAsync(entryId);

        // Assert: Should fail gracefully
        result.Success.ShouldBeFalse();
        result.Action.ShouldBe(DlqRemediationAction.Failed);
        result.ErrorMessage!.ShouldContain("Unable to retrieve payload");
        
        // Publisher should NOT be called
        await _publisher.DidNotReceive().PublishAsync<byte[]>(
            Arg.Any<string>(),
            Arg.Any<byte[]>(),
            Arg.Any<string>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ReplayAsync_ReturnsFailed_WhenPayloadIsEmptyArray()
    {
        // Arrange: Entry with empty byte array
        var entryId = "stream.consumer.msg-5";
        var entry = CreateTestEntry(entryId, payload: Array.Empty<byte>());

        _dlqStore.GetAsync(entryId, Arg.Any<CancellationToken>()).Returns(entry);
        _dlqStore.UpdateStatusAsync(Arg.Any<string>(), Arg.Any<DlqMessageStatus>(), Arg.Any<CancellationToken>())
            .Returns(true);

        // Act
        var result = await _sut.ReplayAsync(entryId);

        // Assert: Empty array should be treated as no payload
        result.Success.ShouldBeFalse();
        result.Action.ShouldBe(DlqRemediationAction.Failed);
    }

    #endregion

    #region Replay Idempotency Tests

    [Test]
    public async Task ReplayAsync_GeneratesCorrectMessageIdFormat()
    {
        // Arrange
        var entryId = "orders.processor.msg-abc123";
        var entry = CreateTestEntry(entryId, payload: Encoding.UTF8.GetBytes("test"));
        string? capturedMessageId = null;

        _dlqStore.GetAsync(entryId, Arg.Any<CancellationToken>()).Returns(entry);
        _dlqStore.UpdateStatusAsync(Arg.Any<string>(), Arg.Any<DlqMessageStatus>(), Arg.Any<CancellationToken>())
            .Returns(true);

        _publisher.PublishAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                capturedMessageId = callInfo.ArgAt<string>(2);
                return Task.CompletedTask;
            });

        // Act
        await _sut.ReplayAsync(entryId);

        // Assert: Message ID should follow replay-{id}-{ticks} format
        capturedMessageId.ShouldNotBeNull();
        capturedMessageId.ShouldStartWith($"replay-{entryId}-");
        
        // Extract ticks and verify it's a valid number
        var ticksPart = capturedMessageId.Substring($"replay-{entryId}-".Length);
        long.TryParse(ticksPart, out var ticks).ShouldBeTrue();
        ticks.ShouldBeGreaterThan(0);
    }

    [Test]
    public async Task ReplayWithModificationAsync_GeneratesModifiedMessageIdFormat()
    {
        // Arrange
        var entryId = "orders.processor.msg-xyz789";
        var entry = CreateTestEntry(entryId, payload: Encoding.UTF8.GetBytes("original"));
        var modifiedPayload = new TestPayload { Data = "modified" };
        string? capturedMessageId = null;

        _dlqStore.GetAsync(entryId, Arg.Any<CancellationToken>()).Returns(entry);
        _dlqStore.UpdateStatusAsync(Arg.Any<string>(), Arg.Any<DlqMessageStatus>(), Arg.Any<CancellationToken>())
            .Returns(true);

        _publisher.PublishAsync(Arg.Any<string>(), Arg.Any<TestPayload>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                capturedMessageId = callInfo.ArgAt<string>(2);
                return Task.CompletedTask;
            });

        // Act
        await _sut.ReplayWithModificationAsync(entryId, modifiedPayload);

        // Assert: Message ID should follow replay-modified-{id}-{ticks} format
        capturedMessageId.ShouldNotBeNull();
        capturedMessageId.ShouldStartWith($"replay-modified-{entryId}-");
    }

    [Test]
    public async Task ReplayAsync_UsesDistinctMessageIds_AcrossMultipleReplays()
    {
        // Arrange
        var entryId = "stream.consumer.msg-dedup-test";
        var entry = CreateTestEntry(entryId, payload: Encoding.UTF8.GetBytes("test"));
        var capturedMessageIds = new List<string>();

        _dlqStore.GetAsync(entryId, Arg.Any<CancellationToken>()).Returns(entry);
        _dlqStore.UpdateStatusAsync(Arg.Any<string>(), Arg.Any<DlqMessageStatus>(), Arg.Any<CancellationToken>())
            .Returns(true);

        _publisher.PublishAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                capturedMessageIds.Add(callInfo.ArgAt<string>(2));
                return Task.CompletedTask;
            });

        // Act: Replay multiple times in quick succession
        await _sut.ReplayAsync(entryId);
        await Task.Delay(1); // Ensure tick differences
        await _sut.ReplayAsync(entryId);
        await Task.Delay(1);
        await _sut.ReplayAsync(entryId);

        // Assert: Each replay should have a unique message ID
        capturedMessageIds.Count.ShouldBe(3);
        capturedMessageIds.Distinct().Count().ShouldBe(3, "All message IDs should be unique");
    }

    #endregion

    #region Transactional Integrity Tests

    [Test]
    public async Task ReplayAsync_RevertsToPending_WhenPublishFails()
    {
        // Arrange
        var entryId = "stream.consumer.msg-publish-fail";
        var entry = CreateTestEntry(entryId, payload: Encoding.UTF8.GetBytes("test"));

        _dlqStore.GetAsync(entryId, Arg.Any<CancellationToken>()).Returns(entry);
        _dlqStore.UpdateStatusAsync(Arg.Any<string>(), Arg.Any<DlqMessageStatus>(), Arg.Any<CancellationToken>())
            .Returns(true);

        _publisher.PublishAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new IOException("NATS connection lost"));

        // Act
        var result = await _sut.ReplayAsync(entryId);

        // Assert: Status should be reverted to Pending
        result.Success.ShouldBeFalse();
        result.Action.ShouldBe(DlqRemediationAction.Failed);
        result.ErrorMessage!.ShouldContain("NATS connection lost");

        // Verify status update sequence: Processing -> Pending (revert)
        Received.InOrder(() =>
        {
            _dlqStore.UpdateStatusAsync(entryId, DlqMessageStatus.Processing, Arg.Any<CancellationToken>());
            _dlqStore.UpdateStatusAsync(entryId, DlqMessageStatus.Pending, Arg.Any<CancellationToken>());
        });
    }

    [Test]
    public async Task ReplayAsync_ReturnsFailedResult_OnPublishException()
    {
        // Arrange
        var entryId = "stream.consumer.msg-exception";
        var entry = CreateTestEntry(entryId, payload: Encoding.UTF8.GetBytes("test"));
        var expectedException = new InvalidOperationException("Message too large for JetStream");

        _dlqStore.GetAsync(entryId, Arg.Any<CancellationToken>()).Returns(entry);
        _dlqStore.UpdateStatusAsync(Arg.Any<string>(), Arg.Any<DlqMessageStatus>(), Arg.Any<CancellationToken>())
            .Returns(true);

        _publisher.PublishAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(expectedException);

        // Act
        var result = await _sut.ReplayAsync(entryId);

        // Assert: Result should contain specific error message
        result.Success.ShouldBeFalse();
        result.Action.ShouldBe(DlqRemediationAction.Failed);
        result.ErrorMessage.ShouldBe(expectedException.Message);
    }

    [Test]
    public async Task ReplayAsync_HandlesStatusUpdateFailure_AfterPublishSucceeds()
    {
        // Critical scenario: Publish succeeds but status update fails
        // This tests the "at least once" delivery semantics
        
        // Arrange
        var entryId = "stream.consumer.msg-partial-fail";
        var entry = CreateTestEntry(entryId, payload: Encoding.UTF8.GetBytes("test"));
        var statusUpdateCount = 0;

        _dlqStore.GetAsync(entryId, Arg.Any<CancellationToken>()).Returns(entry);
        
        // First call (Processing) succeeds, second call (Resolved) fails
        _dlqStore.UpdateStatusAsync(Arg.Any<string>(), Arg.Any<DlqMessageStatus>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                statusUpdateCount++;
                if (statusUpdateCount == 2) // The Resolved status update
                {
                    throw new IOException("KV store unavailable");
                }
                return Task.FromResult(true);
            });

        // Act
        var result = await _sut.ReplayAsync(entryId);

        // Assert: The operation should return Failed since post-publish status update threw
        // This is the documented "at-least-once" risk - message was published but status wasn't updated
        result.Success.ShouldBeFalse();
        result.Action.ShouldBe(DlqRemediationAction.Failed);
        
        // Publisher WAS called (the message was delivered)
        await _publisher.Received(1).PublishAsync(
            Arg.Any<string>(),
            Arg.Any<byte[]>(),
            Arg.Any<string>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ReplayWithModificationAsync_RevertsToPending_WhenPublishFails()
    {
        // Arrange
        var entryId = "stream.consumer.msg-mod-fail";
        var entry = CreateTestEntry(entryId, payload: Encoding.UTF8.GetBytes("original"));
        var modifiedPayload = new TestPayload { Data = "modified" };

        _dlqStore.GetAsync(entryId, Arg.Any<CancellationToken>()).Returns(entry);
        _dlqStore.UpdateStatusAsync(Arg.Any<string>(), Arg.Any<DlqMessageStatus>(), Arg.Any<CancellationToken>())
            .Returns(true);

        _publisher.PublishAsync(Arg.Any<string>(), Arg.Any<TestPayload>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new TimeoutException("Publish timeout"));

        // Act
        var result = await _sut.ReplayWithModificationAsync(entryId, modifiedPayload);

        // Assert
        result.Success.ShouldBeFalse();
        result.Action.ShouldBe(DlqRemediationAction.Failed);

        // Verify revert to Pending
        await _dlqStore.Received(1).UpdateStatusAsync(
            entryId, 
            DlqMessageStatus.Pending, 
            Arg.Any<CancellationToken>());
    }

    #endregion

    #region Modification Path Tests

    [Test]
    public async Task ReplayWithModificationAsync_UsesModifiedPayload_NotOriginal()
    {
        // Critical: Modified payload must be published, not the cached/stored original
        
        // Arrange
        var entryId = "stream.consumer.msg-mod-verify";
        var originalPayload = Encoding.UTF8.GetBytes("{\"status\":\"BROKEN\"}");
        var entry = CreateTestEntry(entryId, payload: originalPayload);
        var modifiedPayload = new TestPayload { Data = "FIXED", Value = 42 };
        TestPayload? capturedPayload = null;

        _dlqStore.GetAsync(entryId, Arg.Any<CancellationToken>()).Returns(entry);
        _dlqStore.UpdateStatusAsync(Arg.Any<string>(), Arg.Any<DlqMessageStatus>(), Arg.Any<CancellationToken>())
            .Returns(true);

        _publisher.PublishAsync(Arg.Any<string>(), Arg.Any<TestPayload>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                capturedPayload = callInfo.ArgAt<TestPayload>(1);
                return Task.CompletedTask;
            });

        // Act
        var result = await _sut.ReplayWithModificationAsync(entryId, modifiedPayload);

        // Assert: The MODIFIED payload was published, not the original
        result.Success.ShouldBeTrue();
        capturedPayload.ShouldNotBeNull();
        capturedPayload!.Data.ShouldBe("FIXED");
        capturedPayload.Value.ShouldBe(42);
    }

    [Test]
    public async Task ReplayWithModificationAsync_PublishesToOriginalSubject()
    {
        // Arrange
        var entryId = "orders.processor.msg-subject-test";
        var originalSubject = "orders.v2.created";
        var entry = CreateTestEntry(entryId, payload: Encoding.UTF8.GetBytes("x"), originalSubject: originalSubject);
        var modifiedPayload = new TestPayload { Data = "test" };
        string? capturedSubject = null;

        _dlqStore.GetAsync(entryId, Arg.Any<CancellationToken>()).Returns(entry);
        _dlqStore.UpdateStatusAsync(Arg.Any<string>(), Arg.Any<DlqMessageStatus>(), Arg.Any<CancellationToken>())
            .Returns(true);

        _publisher.PublishAsync(Arg.Any<string>(), Arg.Any<TestPayload>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                capturedSubject = callInfo.ArgAt<string>(0);
                return Task.CompletedTask;
            });

        // Act
        await _sut.ReplayWithModificationAsync(entryId, modifiedPayload);

        // Assert: Published to the ORIGINAL subject from the entry
        capturedSubject.ShouldBe(originalSubject);
    }

    [Test]
    public async Task ReplayWithModificationAsync_UsesGenericPublishOverload()
    {
        // Verify the typed publisher method is called (not raw bytes)
        
        // Arrange
        var entryId = "stream.consumer.msg-typed-publish";
        var entry = CreateTestEntry(entryId, payload: Encoding.UTF8.GetBytes("original"));
        var modifiedPayload = new TestPayload { Data = "typed" };

        _dlqStore.GetAsync(entryId, Arg.Any<CancellationToken>()).Returns(entry);
        _dlqStore.UpdateStatusAsync(Arg.Any<string>(), Arg.Any<DlqMessageStatus>(), Arg.Any<CancellationToken>())
            .Returns(true);

        // Act
        await _sut.ReplayWithModificationAsync(entryId, modifiedPayload);

        // Assert: The generic PublishAsync<TestPayload> was called
        await _publisher.Received(1).PublishAsync(
            Arg.Any<string>(),
            Arg.Is<TestPayload>(p => p != null),
            Arg.Any<string>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ReplayWithModificationAsync_ReturnsNotFound_WhenEntryMissing()
    {
        // Arrange
        var entryId = "nonexistent.consumer.msg-404";
        var modifiedPayload = new TestPayload { Data = "test" };

        _dlqStore.GetAsync(entryId, Arg.Any<CancellationToken>()).Returns((DlqMessageEntry?)null);

        // Act
        var result = await _sut.ReplayWithModificationAsync(entryId, modifiedPayload);

        // Assert
        result.Success.ShouldBeFalse();
        result.Action.ShouldBe(DlqRemediationAction.NotFound);
        result.ErrorMessage!.ShouldContain("not found");
    }

    #endregion

    #region Edge Cases and Validation

    [Test]
    public async Task ReplayAsync_ReturnsNotFound_WhenEntryDoesNotExist()
    {
        // Arrange
        var entryId = "nonexistent.consumer.msg-id";
        _dlqStore.GetAsync(entryId, Arg.Any<CancellationToken>()).Returns((DlqMessageEntry?)null);

        // Act
        var result = await _sut.ReplayAsync(entryId);

        // Assert
        result.Success.ShouldBeFalse();
        result.Action.ShouldBe(DlqRemediationAction.NotFound);
    }

    [Test]
    public async Task ReplayAsync_ThrowsArgumentException_ForEmptyId()
    {
        // Act & Assert
        await Should.ThrowAsync<ArgumentException>(() => _sut.ReplayAsync(""));
        await Should.ThrowAsync<ArgumentException>(() => _sut.ReplayAsync("   "));
    }

    [Test]
    public async Task ReplayWithModificationAsync_ThrowsArgumentNull_ForNullPayload()
    {
        // Act & Assert
        await Should.ThrowAsync<ArgumentNullException>(() => 
            _sut.ReplayWithModificationAsync<TestPayload>("stream.consumer.id", null!));
    }

    [Test]
    public async Task InspectAsync_DelegatesToDlqStore()
    {
        // Arrange
        var entryId = "stream.consumer.msg-inspect";
        var entry = CreateTestEntry(entryId);
        _dlqStore.GetAsync(entryId, Arg.Any<CancellationToken>()).Returns(entry);

        // Act
        var result = await _sut.InspectAsync(entryId);

        // Assert
        result.ShouldNotBeNull();
        result!.Id.ShouldBe("msg-inspect");
        await _dlqStore.Received(1).GetAsync(entryId, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task DeleteAsync_ReturnsSuccess_WhenEntryDeleted()
    {
        // Arrange
        var entryId = "stream.consumer.msg-delete";
        _dlqStore.DeleteAsync(entryId, Arg.Any<CancellationToken>()).Returns(true);

        // Act
        var result = await _sut.DeleteAsync(entryId);

        // Assert
        result.Success.ShouldBeTrue();
        result.Action.ShouldBe(DlqRemediationAction.Deleted);
    }

    [Test]
    public async Task ArchiveAsync_UpdatesStatusToArchived()
    {
        // Arrange
        var entryId = "stream.consumer.msg-archive";
        var entry = CreateTestEntry(entryId);
        _dlqStore.GetAsync(entryId, Arg.Any<CancellationToken>()).Returns(entry);
        _dlqStore.UpdateStatusAsync(entryId, DlqMessageStatus.Archived, Arg.Any<CancellationToken>())
            .Returns(true);

        // Act
        var result = await _sut.ArchiveAsync(entryId, "Obsolete message");

        // Assert
        result.Success.ShouldBeTrue();
        result.Action.ShouldBe(DlqRemediationAction.Archived);
        await _dlqStore.Received(1).UpdateStatusAsync(
            entryId, 
            DlqMessageStatus.Archived, 
            Arg.Any<CancellationToken>());
    }

    #endregion

    #region Helpers

    private DlqMessageEntry CreateTestEntry(
        string key,
        byte[]? payload = null,
        string? payloadEncoding = null,
        Dictionary<string, string>? headers = null,
        string originalSubject = "test.subject")
    {
        // Extract the message ID from the hierarchical key
        var parts = key.Split('.');
        var id = parts.Length >= 3 ? parts[2] : key;
        var stream = parts.Length >= 1 ? parts[0] : "test-stream";
        var consumer = parts.Length >= 2 ? parts[1] : "test-consumer";

        return new DlqMessageEntry
        {
            Id = id,
            OriginalStream = stream,
            OriginalConsumer = consumer,
            OriginalSubject = originalSubject,
            OriginalSequence = 1,
            DeliveryCount = 5,
            StoredAt = DateTimeOffset.UtcNow.AddHours(-1),
            ErrorReason = "Max delivery attempts exceeded",
            Payload = payload,
            PayloadEncoding = payloadEncoding,
            OriginalHeaders = headers
        };
    }

    private record TestPayload
    {
        public string Data { get; init; } = string.Empty;
        public int Value { get; init; }
    }

    #endregion
}

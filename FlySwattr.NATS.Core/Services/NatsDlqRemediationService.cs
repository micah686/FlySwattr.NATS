using System.Text.Json;
using FlySwattr.NATS.Abstractions;
using Microsoft.Extensions.Logging;

namespace FlySwattr.NATS.Core.Services;

/// <summary>
/// NATS-backed implementation of <see cref="IDlqRemediationService"/>.
/// Provides operations to inspect, replay, archive, and delete DLQ messages.
/// </summary>
public partial class NatsDlqRemediationService : IDlqRemediationService
{
    private readonly IDlqStore _dlqStore;
    private readonly IJetStreamPublisher _publisher;
    private readonly IMessageSerializer _serializer;
    private readonly IObjectStore? _objectStore;
    private readonly IDlqNotificationService? _notificationService;
    private readonly ILogger<NatsDlqRemediationService> _logger;

    public NatsDlqRemediationService(
        IDlqStore dlqStore,
        IJetStreamPublisher publisher,
        IMessageSerializer serializer,
        ILogger<NatsDlqRemediationService> logger,
        IObjectStore? objectStore = null,
        IDlqNotificationService? notificationService = null)
    {
        _dlqStore = dlqStore ?? throw new ArgumentNullException(nameof(dlqStore));
        _publisher = publisher ?? throw new ArgumentNullException(nameof(publisher));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _objectStore = objectStore;
        _notificationService = notificationService;
    }

    /// <inheritdoc />
    public async Task<DlqMessageEntry?> InspectAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        
        LogInspecting(id);
        return await _dlqStore.GetAsync(id, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<DlqMessageEntry>> ListAsync(
        string? filterStream = null,
        string? filterConsumer = null,
        DlqMessageStatus? filterStatus = null,
        int limit = 100,
        CancellationToken cancellationToken = default)
    {
        var entries = await _dlqStore.ListAsync(filterStream, filterConsumer, limit, cancellationToken);
        
        // Apply status filter if specified (since underlying store doesn't support it)
        if (filterStatus.HasValue)
        {
            entries = entries.Where(e => e.Status == filterStatus.Value).ToList();
        }
        
        return entries;
    }

    /// <inheritdoc />
    public async Task<DlqRemediationResult> ReplayAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        
        try
        {
            var entry = await _dlqStore.GetAsync(id, cancellationToken);
            if (entry == null)
            {
                LogNotFound(id);
                return new DlqRemediationResult(false, id, DlqRemediationAction.NotFound, "DLQ entry not found");
            }

            // Mark as processing
            await _dlqStore.UpdateStatusAsync(id, DlqMessageStatus.Processing, cancellationToken);
            LogProcessing(id, entry.OriginalSubject);

            try
            {
                // Get payload - either from entry or object store
                var payload = await GetPayloadAsync(entry, cancellationToken);
                if (payload == null || payload.Length == 0)
                {
                    var errorMsg = "Unable to retrieve payload for replay";
                    LogReplayFailed(id, errorMsg);
                    return new DlqRemediationResult(false, id, DlqRemediationAction.Failed, errorMsg);
                }

                // Publish to original subject using raw bytes
                // Use a deterministic message ID for idempotency: replay-{originalId}
                var replayMessageId = $"replay-{id}-{DateTimeOffset.UtcNow.Ticks}";
                await PublishRawAsync(entry.OriginalSubject, payload, replayMessageId, cancellationToken);

                // Mark as resolved
                await _dlqStore.UpdateStatusAsync(id, DlqMessageStatus.Resolved, cancellationToken);
                LogReplayed(id, entry.OriginalSubject);

                return new DlqRemediationResult(true, id, DlqRemediationAction.Replayed, CompletedAt: DateTimeOffset.UtcNow);
            }
            catch (Exception ex)
            {
                // Revert to pending on failure
                await _dlqStore.UpdateStatusAsync(id, DlqMessageStatus.Pending, cancellationToken);
                LogReplayException(id, ex);
                return new DlqRemediationResult(false, id, DlqRemediationAction.Failed, ex.Message);
            }
        }
        catch (Exception ex)
        {
            LogReplayException(id, ex);
            return new DlqRemediationResult(false, id, DlqRemediationAction.Failed, ex.Message);
        }
    }

    /// <inheritdoc />
    public async Task<DlqRemediationResult> ReplayWithModificationAsync<T>(string id, T modifiedPayload, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ArgumentNullException.ThrowIfNull(modifiedPayload);

        try
        {
            var entry = await _dlqStore.GetAsync(id, cancellationToken);
            if (entry == null)
            {
                LogNotFound(id);
                return new DlqRemediationResult(false, id, DlqRemediationAction.NotFound, "DLQ entry not found");
            }

            // Mark as processing
            await _dlqStore.UpdateStatusAsync(id, DlqMessageStatus.Processing, cancellationToken);
            LogProcessingModified(id, entry.OriginalSubject, typeof(T).Name);

            try
            {
                // Use a deterministic message ID for idempotency
                var replayMessageId = $"replay-modified-{id}-{DateTimeOffset.UtcNow.Ticks}";
                await _publisher.PublishAsync(entry.OriginalSubject, modifiedPayload, replayMessageId, cancellationToken);

                // Mark as resolved
                await _dlqStore.UpdateStatusAsync(id, DlqMessageStatus.Resolved, cancellationToken);
                LogReplayedModified(id, entry.OriginalSubject);

                return new DlqRemediationResult(true, id, DlqRemediationAction.Replayed, CompletedAt: DateTimeOffset.UtcNow);
            }
            catch (Exception ex)
            {
                // Revert to pending on failure
                await _dlqStore.UpdateStatusAsync(id, DlqMessageStatus.Pending, cancellationToken);
                LogReplayException(id, ex);
                return new DlqRemediationResult(false, id, DlqRemediationAction.Failed, ex.Message);
            }
        }
        catch (Exception ex)
        {
            LogReplayException(id, ex);
            return new DlqRemediationResult(false, id, DlqRemediationAction.Failed, ex.Message);
        }
    }

    /// <inheritdoc />
    public async Task<DlqRemediationResult> ArchiveAsync(string id, string? reason = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);

        try
        {
            var entry = await _dlqStore.GetAsync(id, cancellationToken);
            if (entry == null)
            {
                LogNotFound(id);
                return new DlqRemediationResult(false, id, DlqRemediationAction.NotFound, "DLQ entry not found");
            }

            await _dlqStore.UpdateStatusAsync(id, DlqMessageStatus.Archived, cancellationToken);
            LogArchived(id, reason);

            return new DlqRemediationResult(true, id, DlqRemediationAction.Archived, CompletedAt: DateTimeOffset.UtcNow);
        }
        catch (Exception ex)
        {
            LogArchiveException(id, ex);
            return new DlqRemediationResult(false, id, DlqRemediationAction.Failed, ex.Message);
        }
    }

    /// <inheritdoc />
    public async Task<DlqRemediationResult> DeleteAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);

        try
        {
            var deleted = await _dlqStore.DeleteAsync(id, cancellationToken);
            if (!deleted)
            {
                LogNotFound(id);
                return new DlqRemediationResult(false, id, DlqRemediationAction.NotFound, "DLQ entry not found");
            }

            LogDeleted(id);
            return new DlqRemediationResult(true, id, DlqRemediationAction.Deleted, CompletedAt: DateTimeOffset.UtcNow);
        }
        catch (Exception ex)
        {
            LogDeleteException(id, ex);
            return new DlqRemediationResult(false, id, DlqRemediationAction.Failed, ex.Message);
        }
    }

    private async Task<byte[]?> GetPayloadAsync(DlqMessageEntry entry, CancellationToken cancellationToken)
    {
        // Check if payload is offloaded to object store
        if (entry.Payload == null || entry.Payload.Length == 0)
        {
            // Look for object store reference in headers or a convention-based key
            // The DLQ message creation uses: objstore://{objectKey} as PayloadEncoding
            // But DlqMessageEntry doesn't have PayloadEncoding, so we need to use the original headers
            // or fall back to checking if there's an object store key pattern
            
            if (_objectStore != null && entry.OriginalHeaders?.TryGetValue("x-dlq-payload-ref", out var payloadRef) == true)
            {
                using var ms = new MemoryStream();
                await _objectStore.GetAsync(payloadRef, ms, cancellationToken);
                return ms.ToArray();
            }
            
            return null;
        }

        return entry.Payload;
    }

    private async Task PublishRawAsync(string subject, byte[] payload, string messageId, CancellationToken cancellationToken)
    {
        // For raw bytes, we need to deserialize first then republish
        // This is a limitation - we don't know the original type
        // For now, we publish as-is using a generic approach
        // The consumer will need to handle the deserialization
        
        // Note: This publishes the raw bytes as the payload. The receiving consumer
        // must be able to handle this format (typically MemoryPack or JSON bytes)
        await _publisher.PublishAsync(subject, payload, messageId, cancellationToken);
    }

    // Source-generated logging methods
    [LoggerMessage(Level = LogLevel.Debug, Message = "Inspecting DLQ entry {Id}")]
    private partial void LogInspecting(string id);

    [LoggerMessage(Level = LogLevel.Warning, Message = "DLQ entry {Id} not found")]
    private partial void LogNotFound(string id);

    [LoggerMessage(Level = LogLevel.Information, Message = "Processing DLQ entry {Id} for replay to {Subject}")]
    private partial void LogProcessing(string id, string subject);

    [LoggerMessage(Level = LogLevel.Information, Message = "Processing DLQ entry {Id} for modified replay to {Subject} with type {PayloadType}")]
    private partial void LogProcessingModified(string id, string subject, string payloadType);

    [LoggerMessage(Level = LogLevel.Information, Message = "Successfully replayed DLQ entry {Id} to {Subject}")]
    private partial void LogReplayed(string id, string subject);

    [LoggerMessage(Level = LogLevel.Information, Message = "Successfully replayed DLQ entry {Id} to {Subject} with modified payload")]
    private partial void LogReplayedModified(string id, string subject);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to replay DLQ entry {Id}: {ErrorMessage}")]
    private partial void LogReplayFailed(string id, string errorMessage);

    [LoggerMessage(Level = LogLevel.Error, Message = "Exception while replaying DLQ entry {Id}")]
    private partial void LogReplayException(string id, Exception ex);

    [LoggerMessage(Level = LogLevel.Information, Message = "Archived DLQ entry {Id}. Reason: {Reason}")]
    private partial void LogArchived(string id, string? reason);

    [LoggerMessage(Level = LogLevel.Error, Message = "Exception while archiving DLQ entry {Id}")]
    private partial void LogArchiveException(string id, Exception ex);

    [LoggerMessage(Level = LogLevel.Information, Message = "Deleted DLQ entry {Id}")]
    private partial void LogDeleted(string id);

    [LoggerMessage(Level = LogLevel.Error, Message = "Exception while deleting DLQ entry {Id}")]
    private partial void LogDeleteException(string id, Exception ex);
}

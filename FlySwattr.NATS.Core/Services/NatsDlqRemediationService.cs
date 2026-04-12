using FlySwattr.NATS.Abstractions;
using Microsoft.Extensions.Logging;

namespace FlySwattr.NATS.Core.Services;

/// <summary>
/// NATS-backed implementation of <see cref="IDlqRemediationService"/>.
/// Provides operations to inspect, replay, archive, and delete DLQ messages.
/// </summary>
internal partial class NatsDlqRemediationService : IDlqRemediationService
{
    private const string ContentTypeHeader = "Content-Type";
    private const string MessageIdHeader = "Nats-Msg-Id";

    private readonly IDlqStore _dlqStore;
    private readonly IJetStreamPublisher _publisher;
    private readonly IMessageSerializer _serializer;
    private readonly IObjectStore? _objectStore;
    private readonly IDlqNotificationService? _notificationService;
    private readonly IRawJetStreamPublisher? _rawPublisher;
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

    internal NatsDlqRemediationService(
        IDlqStore dlqStore,
        IJetStreamPublisher publisher,
        IMessageSerializer serializer,
        ILogger<NatsDlqRemediationService> logger,
        IRawJetStreamPublisher rawPublisher,
        IObjectStore? objectStore,
        IDlqNotificationService? notificationService)
    {
        _dlqStore = dlqStore ?? throw new ArgumentNullException(nameof(dlqStore));
        _publisher = publisher ?? throw new ArgumentNullException(nameof(publisher));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _rawPublisher = rawPublisher;
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
    public async Task<DlqRemediationResult> ReplayAsync(
        string id,
        DlqReplayMode replayMode = DlqReplayMode.Idempotent,
        CancellationToken cancellationToken = default)
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

            var processingResult = await _dlqStore.UpdateStatusAsync(
                id,
                DlqMessageStatus.Processing,
                entry.StoreRevision,
                cancellationToken);

            if (!processingResult.Succeeded)
            {
                return CreateStatusUpdateFailureResult(id, processingResult, "mark entry as processing");
            }

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

                var replayMessageId = CreateReplayMessageId(id, replayMode);
                await PublishReplayAsync(entry, payload, replayMessageId, cancellationToken);

                var resolvedResult = await _dlqStore.UpdateStatusAsync(
                    id,
                    DlqMessageStatus.Resolved,
                    processingResult.Revision,
                    cancellationToken);

                if (!resolvedResult.Succeeded)
                {
                    return CreateStatusUpdateFailureResult(id, resolvedResult, "mark entry as resolved after replay");
                }

                LogReplayed(id, entry.OriginalSubject);

                return new DlqRemediationResult(true, id, DlqRemediationAction.Replayed, CompletedAt: DateTimeOffset.UtcNow);
            }
            catch (Exception ex)
            {
                await TryRevertToPendingAsync(id, processingResult.Revision, cancellationToken);
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
    public async Task<DlqRemediationResult> ReplayWithModificationAsync<T>(
        string id,
        T modifiedPayload,
        DlqReplayMode replayMode = DlqReplayMode.Idempotent,
        CancellationToken cancellationToken = default)
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

            var processingResult = await _dlqStore.UpdateStatusAsync(
                id,
                DlqMessageStatus.Processing,
                entry.StoreRevision,
                cancellationToken);

            if (!processingResult.Succeeded)
            {
                return CreateStatusUpdateFailureResult(id, processingResult, "mark entry as processing");
            }

            LogProcessingModified(id, entry.OriginalSubject, typeof(T).Name);

            try
            {
                var replayMessageId = CreateReplayMessageId(id, replayMode, modified: true);
                await _publisher.PublishAsync(
                    entry.OriginalSubject,
                    modifiedPayload,
                    replayMessageId,
                    CreateReplayHeaders(entry),
                    cancellationToken);

                var resolvedResult = await _dlqStore.UpdateStatusAsync(
                    id,
                    DlqMessageStatus.Resolved,
                    processingResult.Revision,
                    cancellationToken);

                if (!resolvedResult.Succeeded)
                {
                    return CreateStatusUpdateFailureResult(id, resolvedResult, "mark entry as resolved after replay");
                }

                LogReplayedModified(id, entry.OriginalSubject);

                return new DlqRemediationResult(true, id, DlqRemediationAction.Replayed, CompletedAt: DateTimeOffset.UtcNow);
            }
            catch (Exception ex)
            {
                await TryRevertToPendingAsync(id, processingResult.Revision, cancellationToken);
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

            var updateResult = await _dlqStore.UpdateStatusAsync(
                id,
                DlqMessageStatus.Archived,
                entry.StoreRevision,
                cancellationToken);

            if (!updateResult.Succeeded)
            {
                return CreateStatusUpdateFailureResult(id, updateResult, "archive entry");
            }

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
            // Look for object store reference in PayloadEncoding
            if (_objectStore != null && 
                !string.IsNullOrEmpty(entry.PayloadEncoding) && 
                entry.PayloadEncoding.StartsWith("objstore://"))
            {
                var payloadRef = entry.PayloadEncoding.Substring(11); // Remove prefix
                using var ms = new MemoryStream();
                await _objectStore.GetAsync(payloadRef, ms, cancellationToken);
                return ms.ToArray();
            }
            
            // Fallback for backward compatibility or alternative header-based refs
            if (_objectStore != null && entry.OriginalHeaders?.TryGetValue("x-dlq-payload-ref", out var headerRef) == true)
            {
                using var ms = new MemoryStream();
                await _objectStore.GetAsync(headerRef, ms, cancellationToken);
                return ms.ToArray();
            }
            
            return null;
        }

        return entry.Payload;
    }

    private async Task PublishReplayAsync(
        DlqMessageEntry entry,
        byte[] payload,
        string messageId,
        CancellationToken cancellationToken)
    {
        var headers = CreateReplayHeaders(entry);
        if (_rawPublisher != null)
        {
            await _rawPublisher.PublishRawAsync(entry.OriginalSubject, payload, messageId, headers, cancellationToken);
            return;
        }

        var runtimeType = ResolveRuntimeType(entry.OriginalMessageType);
        if (runtimeType == null || runtimeType == typeof(byte[]))
        {
            await _publisher.PublishAsync(entry.OriginalSubject, payload, messageId, headers, cancellationToken);
            return;
        }

        var deserialized = DeserializePayload(payload, runtimeType);
        await PublishTypedAsync(entry.OriginalSubject, deserialized, runtimeType, messageId, headers, cancellationToken);
    }

    private async Task TryRevertToPendingAsync(string id, ulong? expectedRevision, CancellationToken cancellationToken)
    {
        if (expectedRevision == null)
        {
            return;
        }

        var revertResult = await _dlqStore.UpdateStatusAsync(id, DlqMessageStatus.Pending, expectedRevision, cancellationToken);
        if (!revertResult.Succeeded)
        {
            LogReplayFailed(id, $"Failed to revert status to Pending due to {revertResult.Outcome}");
        }
    }

    private DlqRemediationResult CreateStatusUpdateFailureResult(string id, DlqEntryUpdateResult result, string action)
    {
        var message = result.Outcome switch
        {
            DlqEntryUpdateOutcome.NotFound => $"Unable to {action}: DLQ entry not found",
            DlqEntryUpdateOutcome.Conflict => $"Unable to {action}: DLQ entry was modified concurrently",
            _ => $"Unable to {action}"
        };

        LogReplayFailed(id, message);
        return new DlqRemediationResult(false, id, DlqRemediationAction.Failed, message);
    }

    private static string CreateReplayMessageId(string id, DlqReplayMode replayMode, bool modified = false)
    {
        var prefix = modified ? "replay-modified" : "replay";
        return replayMode switch
        {
            DlqReplayMode.Idempotent => $"{prefix}-{id}",
            DlqReplayMode.NewEvent => $"{prefix}-{id}-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-{Guid.NewGuid():N}",
            _ => throw new ArgumentOutOfRangeException(nameof(replayMode), replayMode, "Unknown replay mode")
        };
    }

    private static Type? ResolveRuntimeType(string? originalMessageType)
    {
        if (string.IsNullOrWhiteSpace(originalMessageType))
        {
            return null;
        }

        return Type.GetType(originalMessageType, throwOnError: false)
               ?? AppDomain.CurrentDomain.GetAssemblies()
                   .Select(a => a.GetType(originalMessageType, throwOnError: false, ignoreCase: false))
                   .FirstOrDefault(t => t != null);
    }

    private object DeserializePayload(byte[] payload, Type runtimeType)
    {
        var deserializeMethod = typeof(IMessageSerializer)
            .GetMethod(nameof(IMessageSerializer.Deserialize))
            ?.MakeGenericMethod(runtimeType)
            ?? throw new InvalidOperationException("Unable to resolve serializer deserialize method.");

        return deserializeMethod.Invoke(_serializer, [new ReadOnlyMemory<byte>(payload)])
               ?? throw new InvalidOperationException($"Serializer returned null for type {runtimeType.FullName}.");
    }

    private Task PublishTypedAsync(
        string subject,
        object payload,
        Type payloadType,
        string messageId,
        MessageHeaders? headers,
        CancellationToken cancellationToken)
    {
        var publishMethod = typeof(IJetStreamPublisher)
            .GetMethods()
            .Single(m =>
                m.Name == nameof(IJetStreamPublisher.PublishAsync) &&
                m.IsGenericMethodDefinition &&
                m.GetParameters().Length == 5)
            .MakeGenericMethod(payloadType);

        return (Task)(publishMethod.Invoke(_publisher, [subject, payload, messageId, headers, cancellationToken])
            ?? throw new InvalidOperationException("Unable to invoke typed replay publish."));
    }

    private static MessageHeaders? CreateReplayHeaders(DlqMessageEntry entry)
    {
        if (entry.OriginalHeaders == null || entry.OriginalHeaders.Count == 0)
        {
            return null;
        }

        var headers = entry.OriginalHeaders
            .Where(kvp => !string.Equals(kvp.Key, MessageIdHeader, StringComparison.OrdinalIgnoreCase))
            .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

        return headers.Count == 0 ? null : new MessageHeaders(headers);
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

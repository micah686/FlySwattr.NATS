// ReSharper disable CheckNamespace
// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Abstractions;

/// <summary>
/// Provides remediation operations for Dead Letter Queue entries.
/// Enables operations teams to inspect, replay, archive, and delete failed messages.
/// </summary>
public interface IDlqRemediationService
{
    /// <summary>
    /// Retrieves a DLQ entry for inspection.
    /// </summary>
    /// <param name="id">The unique identifier of the DLQ entry.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The DLQ entry if found, null otherwise.</returns>
    Task<DlqMessageEntry?> InspectAsync(string id, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Lists DLQ entries with optional filtering.
    /// </summary>
    /// <param name="filterStream">Optional stream name to filter by.</param>
    /// <param name="filterConsumer">Optional consumer name to filter by.</param>
    /// <param name="filterStatus">Optional status to filter by.</param>
    /// <param name="limit">Maximum number of entries to return.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A list of matching DLQ entries.</returns>
    Task<IReadOnlyList<DlqMessageEntry>> ListAsync(
        string? filterStream = null,
        string? filterConsumer = null,
        DlqMessageStatus? filterStatus = null,
        int limit = 100,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Replays a DLQ message to its original subject.
    /// </summary>
    /// <remarks>
    /// This operation will:
    /// 1. Mark the entry as Processing
    /// 2. Deserialize the stored payload
    /// 3. Publish to the original subject
    /// 4. Mark the entry as Resolved on success
    /// </remarks>
    /// <param name="id">The unique identifier of the DLQ entry.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the replay operation.</returns>
    Task<DlqRemediationResult> ReplayAsync(string id, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Replays a DLQ message with a modified payload.
    /// </summary>
    /// <remarks>
    /// Use this when the original payload needs correction before replay
    /// (e.g., fixing malformed data that caused the original failure).
    /// </remarks>
    /// <typeparam name="T">The type of the modified payload.</typeparam>
    /// <param name="id">The unique identifier of the DLQ entry.</param>
    /// <param name="modifiedPayload">The corrected payload to publish.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the replay operation.</returns>
    Task<DlqRemediationResult> ReplayWithModificationAsync<T>(string id, T modifiedPayload, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Archives a DLQ entry without replaying it.
    /// </summary>
    /// <remarks>
    /// Use this for messages that should not be replayed but should be 
    /// retained for audit purposes (e.g., known bad data, obsolete messages).
    /// </remarks>
    /// <param name="id">The unique identifier of the DLQ entry.</param>
    /// <param name="reason">Optional reason for archiving.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the archive operation.</returns>
    Task<DlqRemediationResult> ArchiveAsync(string id, string? reason = null, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Permanently deletes a DLQ entry.
    /// </summary>
    /// <remarks>
    /// Use with caution. This permanently removes the entry from the DLQ store.
    /// Consider archiving instead for audit trail purposes.
    /// </remarks>
    /// <param name="id">The unique identifier of the DLQ entry.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the delete operation.</returns>
    Task<DlqRemediationResult> DeleteAsync(string id, CancellationToken cancellationToken = default);
}

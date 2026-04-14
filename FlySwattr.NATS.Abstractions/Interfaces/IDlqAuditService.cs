// ReSharper disable CheckNamespace
// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Abstractions;

/// <summary>
/// Optional hook for auditing and/or authorizing DLQ remediation operations.
/// </summary>
/// <remarks>
/// <para>Register an implementation in the DI container to receive callbacks before and after
/// each remediation action (replay, archive, delete).  The <c>BeforeXxx</c> methods can veto an
/// operation by throwing an exception (e.g., <see cref="UnauthorizedAccessException"/>) —
/// <see cref="NatsDlqRemediationService"/> will propagate the exception to the caller.</para>
/// <para>If no implementation is registered the service falls back to a no-op and all operations
/// proceed without auditing.</para>
/// </remarks>
public interface IDlqAuditService
{
    /// <summary>
    /// Called before a DLQ entry is replayed.  Throw to veto the operation.
    /// </summary>
    /// <param name="id">DLQ entry ID being replayed.</param>
    /// <param name="replayMode">Whether the replay is idempotent or creates a new event.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task BeforeReplayAsync(string id, DlqReplayMode replayMode, CancellationToken cancellationToken = default);

    /// <summary>
    /// Called after a DLQ replay operation completes (success or failure).
    /// </summary>
    /// <param name="id">DLQ entry ID that was replayed.</param>
    /// <param name="result">Result of the operation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task AfterReplayAsync(string id, DlqRemediationResult result, CancellationToken cancellationToken = default);

    /// <summary>
    /// Called before a DLQ entry is replayed with a modified payload.  Throw to veto.
    /// </summary>
    /// <param name="id">DLQ entry ID being replayed.</param>
    /// <param name="payloadType">Runtime type of the modified payload.</param>
    /// <param name="replayMode">Whether the replay is idempotent or creates a new event.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task BeforeReplayWithModificationAsync(string id, Type payloadType, DlqReplayMode replayMode, CancellationToken cancellationToken = default);

    /// <summary>
    /// Called after a replay-with-modification completes (success or failure).
    /// </summary>
    Task AfterReplayWithModificationAsync(string id, Type payloadType, DlqRemediationResult result, CancellationToken cancellationToken = default);

    /// <summary>
    /// Called before a DLQ entry is archived.  Throw to veto the operation.
    /// </summary>
    /// <param name="id">DLQ entry ID being archived.</param>
    /// <param name="reason">Optional operator-provided reason for archiving.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task BeforeArchiveAsync(string id, string? reason, CancellationToken cancellationToken = default);

    /// <summary>
    /// Called after an archive operation completes (success or failure).
    /// </summary>
    Task AfterArchiveAsync(string id, string? reason, DlqRemediationResult result, CancellationToken cancellationToken = default);

    /// <summary>
    /// Called before a DLQ entry is permanently deleted.  Throw to veto the operation.
    /// </summary>
    /// <param name="id">DLQ entry ID being deleted.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task BeforeDeleteAsync(string id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Called after a delete operation completes (success or failure).
    /// </summary>
    Task AfterDeleteAsync(string id, DlqRemediationResult result, CancellationToken cancellationToken = default);
}

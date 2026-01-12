// ReSharper disable CheckNamespace
// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Abstractions;

/// <summary>
/// Represents the result of a DLQ remediation operation.
/// </summary>
/// <param name="Success">Whether the operation completed successfully.</param>
/// <param name="Id">The DLQ entry ID that was processed.</param>
/// <param name="Action">The remediation action that was performed.</param>
/// <param name="ErrorMessage">Error message if the operation failed.</param>
/// <param name="CompletedAt">When the operation completed.</param>
public record DlqRemediationResult(
    bool Success,
    string Id,
    DlqRemediationAction Action,
    string? ErrorMessage = null,
    DateTimeOffset? CompletedAt = null
);

/// <summary>
/// Represents the type of remediation action performed on a DLQ entry.
/// </summary>
public enum DlqRemediationAction
{
    /// <summary>The message was replayed to its original subject.</summary>
    Replayed,
    /// <summary>The message was archived without replay.</summary>
    Archived,
    /// <summary>The message was permanently deleted.</summary>
    Deleted,
    /// <summary>The message was not found in the DLQ store.</summary>
    NotFound,
    /// <summary>The operation failed.</summary>
    Failed
}

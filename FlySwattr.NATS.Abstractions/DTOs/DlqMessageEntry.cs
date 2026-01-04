// ReSharper disable CheckNamespace
// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Abstractions;

/// <summary>
/// Status of a DLQ message entry for tracking and remediation workflows.
/// </summary>
public enum DlqMessageStatus
{
    /// <summary>Message is awaiting review or processing.</summary>
    Pending,
    /// <summary>Message is currently being processed for remediation.</summary>
    Processing,
    /// <summary>Message has been successfully resolved.</summary>
    Resolved,
    /// <summary>Message has been archived without resolution.</summary>
    Archived
}

/// <summary>
/// Represents a stored DLQ message entry with metadata for tracking and remediation.
/// </summary>
public record DlqMessageEntry
{
    /// <summary>Unique identifier for this DLQ entry.</summary>
    public required string Id { get; init; }
    
    /// <summary>The original JetStream stream the message came from.</summary>
    public required string OriginalStream { get; init; }
    
    /// <summary>The original consumer that failed to process the message.</summary>
    public required string OriginalConsumer { get; init; }
    
    /// <summary>The original subject of the message.</summary>
    public required string OriginalSubject { get; init; }
    
    /// <summary>The sequence number in the original stream.</summary>
    public required ulong OriginalSequence { get; init; }
    
    /// <summary>Number of delivery attempts before the message was moved to DLQ.</summary>
    public required int DeliveryCount { get; init; }
    
    /// <summary>When this entry was stored in the DLQ.</summary>
    public required DateTimeOffset StoredAt { get; init; }
    
    /// <summary>Optional error reason from the last processing attempt.</summary>
    public string? ErrorReason { get; init; }
    
    //TODO: Do we need to include the full payload? Or does this need to be a pointer?
    /// <summary>The payload of the failed message.</summary>
    public byte[]? Payload { get; init; }

    /// <summary>Original headers of the failed message.</summary>
    public Dictionary<string, string>? OriginalHeaders { get; init; }

    /// <summary>Current status of this DLQ entry.</summary>
    public DlqMessageStatus Status { get; init; } = DlqMessageStatus.Pending;
}

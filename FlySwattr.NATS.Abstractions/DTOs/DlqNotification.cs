// ReSharper disable CheckNamespace
// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Abstractions;

/// <summary>
/// Represents a notification about a DLQ event for alerting the operations team.
/// </summary>
/// <param name="MessageId">Unique identifier of the DLQ entry.</param>
/// <param name="OriginalStream">The original JetStream stream the message came from.</param>
/// <param name="OriginalConsumer">The original consumer that failed to process the message.</param>
/// <param name="OriginalSubject">The original subject of the message.</param>
/// <param name="OriginalSequence">The sequence number in the original stream.</param>
/// <param name="DeliveryCount">Number of delivery attempts before the message was moved to DLQ.</param>
/// <param name="ErrorReason">Optional error reason from the last processing attempt.</param>
/// <param name="OccurredAt">When this DLQ event occurred.</param>
public record DlqNotification(
    string MessageId,
    string OriginalStream,
    string OriginalConsumer,
    string OriginalSubject,
    ulong OriginalSequence,
    int DeliveryCount,
    string? ErrorReason,
    DateTimeOffset OccurredAt
);

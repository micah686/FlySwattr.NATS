using FlySwattr.NATS.Abstractions;
using Microsoft.Extensions.Logging;

namespace FlySwattr.NATS.Core;

/// <summary>
/// Logging-based implementation of <see cref="IDlqNotificationService"/>.
/// Logs DLQ events with structured logging for observability.
/// Can be replaced with Slack, PagerDuty, or email implementations.
/// </summary>
internal class LoggingDlqNotificationService : IDlqNotificationService
{
    private readonly ILogger<LoggingDlqNotificationService> _logger;

    public LoggingDlqNotificationService(ILogger<LoggingDlqNotificationService> logger)
    {
        _logger = logger;
    }

    /// <inheritdoc />
    public Task NotifyAsync(DlqNotification notification, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(notification);

        // Log at Warning level to ensure visibility in standard log aggregation
        _logger.LogWarning(
            "🚨 DLQ Alert: Message {MessageId} from {Stream}/{Consumer} " +
            "subject {Subject} seq {Sequence} moved to DLQ after {DeliveryCount} attempts. " +
            "Error: {ErrorReason}",
            notification.MessageId,
            notification.OriginalStream,
            notification.OriginalConsumer,
            notification.OriginalSubject,
            notification.OriginalSequence,
            notification.DeliveryCount,
            notification.ErrorReason ?? "Unknown");

        // For extensibility, this could:
        // - Send to Slack webhook
        // - Create PagerDuty incident
        // - Send email notification
        // - Publish to a dedicated alerting stream

        return Task.CompletedTask;
    }
}

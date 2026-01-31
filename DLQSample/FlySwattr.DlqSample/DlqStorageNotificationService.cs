using FlySwattr.NATS.Abstractions;
using Microsoft.Extensions.Logging;

namespace FlySwattr.DlqSample;

public class DlqStorageNotificationService : IDlqNotificationService
{
    private readonly IDlqStore _dlqStore;
    private readonly ILogger<DlqStorageNotificationService> _logger;

    public DlqStorageNotificationService(IDlqStore dlqStore, ILogger<DlqStorageNotificationService> logger)
    {
        _dlqStore = dlqStore;
        _logger = logger;
    }

    public async Task NotifyAsync(DlqNotification notification, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Received DLQ notification for {MessageId}. Storing in DLQ Store...", notification.MessageId);

        var entry = new DlqMessageEntry
        {
            Id = notification.MessageId,
            OriginalStream = notification.OriginalStream,
            OriginalConsumer = notification.OriginalConsumer,
            OriginalSubject = notification.OriginalSubject,
            OriginalSequence = notification.OriginalSequence,
            DeliveryCount = notification.DeliveryCount,
            ErrorReason = notification.ErrorReason,
            StoredAt = notification.OccurredAt,
            Status = DlqMessageStatus.Pending
        };

        await _dlqStore.StoreAsync(entry, cancellationToken);
        _logger.LogInformation("Successfully stored DLQ entry {MessageId} in KV store.", notification.MessageId);
    }
}

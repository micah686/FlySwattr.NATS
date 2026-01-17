using FlySwattr.NATS.Abstractions;
using Microsoft.Extensions.Logging;

namespace FlySwattr.NATS.Hosting.Services;

/// <summary>
/// DLQ Advisory Handler that stores entries in the DLQ KV Store for remediation.
/// This bridges NATS server-side MAX_DELIVERIES advisories to the KV-backed DLQ store.
/// </summary>
internal partial class DlqStoreAdvisoryHandler : IDlqAdvisoryHandler
{
    private readonly IDlqStore _dlqStore;
    private readonly ILogger<DlqStoreAdvisoryHandler> _logger;

    public DlqStoreAdvisoryHandler(
        IDlqStore dlqStore,
        ILogger<DlqStoreAdvisoryHandler> logger)
    {
        _dlqStore = dlqStore ?? throw new ArgumentNullException(nameof(dlqStore));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task HandleMaxDeliveriesExceededAsync(
        ConsumerMaxDeliveriesAdvisory advisory,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var entry = new DlqMessageEntry
            {
                Id = $"{advisory.Stream}.{advisory.Consumer}.{advisory.StreamSeq}",
                OriginalStream = advisory.Stream,
                OriginalConsumer = advisory.Consumer,
                OriginalSubject = $"{advisory.Stream}.{advisory.Consumer}", // Approximate - advisory doesn't include original subject
                OriginalSequence = advisory.StreamSeq,
                DeliveryCount = advisory.Deliveries,
                StoredAt = advisory.Timestamp,
                ErrorReason = "MaxDeliver limit exceeded (server-side advisory)",
                Status = DlqMessageStatus.Pending,
                // Note: We don't have the payload on the advisory - for full payload access,
                // consumers should use the DefaultDlqPoisonHandler which publishes to the DLQ stream
            };

            await _dlqStore.StoreAsync(entry, cancellationToken);
            LogStored(advisory.Stream, advisory.Consumer, advisory.StreamSeq);
        }
        catch (Exception ex)
        {
            LogStoreFailed(advisory.Stream, advisory.Consumer, advisory.StreamSeq, ex);
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Stored DLQ entry for {Stream}/{Consumer} seq {StreamSeq}")]
    private partial void LogStored(string stream, string consumer, ulong streamSeq);

    [LoggerMessage(Level = LogLevel.Error, Message = "Failed to store DLQ entry for {Stream}/{Consumer} seq {StreamSeq}")]
    private partial void LogStoreFailed(string stream, string consumer, ulong streamSeq, Exception exception);
}

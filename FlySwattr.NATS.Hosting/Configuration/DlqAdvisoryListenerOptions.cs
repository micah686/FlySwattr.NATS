namespace FlySwattr.NATS.Hosting.Configuration;

/// <summary>
/// Configuration options for the DLQ Advisory Listener service.
/// </summary>
public class DlqAdvisoryListenerOptions
{
    /// <summary>
    /// Subject pattern for MAX_DELIVERIES advisories. 
    /// Default: "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>"
    /// </summary>
    /// <remarks>
    /// The > wildcard matches all streams and consumers.
    /// Use more specific patterns like "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.ORDERS.>"
    /// to filter to specific streams.
    /// </remarks>
    public string AdvisorySubject { get; set; } = "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>";

    /// <summary>
    /// Optional filter to only handle advisories from specific streams. 
    /// If empty, handles advisories from all streams.
    /// </summary>
    public HashSet<string> StreamFilter { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Optional filter to only handle advisories from specific consumers. 
    /// If empty, handles advisories from all consumers.
    /// </summary>
    public HashSet<string> ConsumerFilter { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Whether to also trigger the existing <see cref="FlySwattr.NATS.Abstractions.IDlqNotificationService"/>
    /// for backward compatibility with existing notification pipelines.
    /// </summary>
    /// <remarks>
    /// When enabled, a <see cref="FlySwattr.NATS.Abstractions.DlqNotification"/> is created from the advisory
    /// and sent through the standard notification service.
    /// </remarks>
    public bool TriggerDlqNotification { get; set; } = true;

    /// <summary>
    /// Delay before reconnecting after a subscription error. Default: 5 seconds.
    /// </summary>
    public TimeSpan ReconnectDelay { get; set; } = TimeSpan.FromSeconds(5);
}

// ReSharper disable CheckNamespace
// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Abstractions;

/// <summary>
/// Handles NATS JetStream advisory events related to consumer delivery failures.
/// Implement this interface to integrate with external notification systems
/// (PagerDuty, Slack, custom alerting, etc.).
/// </summary>
public interface IDlqAdvisoryHandler
{
    /// <summary>
    /// Called when a consumer exceeds its MaxDeliver limit for a message.
    /// This indicates a "poison message" that could not be processed successfully.
    /// </summary>
    /// <param name="advisory">The advisory event details from NATS server.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task HandleMaxDeliveriesExceededAsync(
        ConsumerMaxDeliveriesAdvisory advisory,
        CancellationToken cancellationToken = default);
}

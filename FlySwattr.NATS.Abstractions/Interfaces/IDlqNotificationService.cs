// ReSharper disable CheckNamespace
// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Abstractions;

/// <summary>
/// Provides notification capabilities for DLQ events.
/// </summary>
public interface IDlqNotificationService
{
    /// <summary>
    /// Sends an alert notification about a message entering the DLQ.
    /// </summary>
    /// <param name="notification">The notification details.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task NotifyAsync(DlqNotification notification, CancellationToken cancellationToken = default);
}

// ReSharper disable CheckNamespace
// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Abstractions;

/// <summary>
/// Provides persistent storage for dead letter queue messages.
/// </summary>
public interface IDlqStore
{
    /// <summary>
    /// Stores a DLQ message entry with metadata.
    /// </summary>
    /// <param name="entry">The DLQ entry to store.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task StoreAsync(DlqMessageEntry entry, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Retrieves a DLQ message by its unique identifier.
    /// </summary>
    /// <param name="id">The unique identifier of the DLQ entry.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The DLQ entry if found, null otherwise.</returns>
    Task<DlqMessageEntry?> GetAsync(string id, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Lists DLQ messages with optional filtering by stream/consumer.
    /// </summary>
    /// <param name="filterStream">Optional stream name to filter by.</param>
    /// <param name="filterConsumer">Optional consumer name to filter by.</param>
    /// <param name="limit">Maximum number of entries to return.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A list of matching DLQ entries.</returns>
    Task<IReadOnlyList<DlqMessageEntry>> ListAsync(
        string? filterStream = null,
        string? filterConsumer = null,
        int limit = 100,
        CancellationToken cancellationToken = default);
}

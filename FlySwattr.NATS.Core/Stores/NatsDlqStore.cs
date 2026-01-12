using System.Text.Json;
using FlySwattr.NATS.Abstractions;
using Microsoft.Extensions.Logging;

namespace FlySwattr.NATS.Core;

/// <summary>
/// NATS Key-Value backed implementation of <see cref="IDlqStore"/>.
/// Stores DLQ entries in a dedicated KV bucket for persistence and querying.
/// </summary>
public class NatsDlqStore : IDlqStore
{
    private const string BucketName = "fs-dlq-entries";
    
    private readonly IKeyValueStore _store;
    private readonly ILogger<NatsDlqStore> _logger;

    public NatsDlqStore(
        Func<string, IKeyValueStore> storeFactory,
        ILogger<NatsDlqStore> logger)
    {
        _store = storeFactory(BucketName);
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task StoreAsync(DlqMessageEntry entry, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentException.ThrowIfNullOrWhiteSpace(entry.Id);

        try
        {
            // Store as JSON for human readability and flexibility
            var json = JsonSerializer.Serialize(entry);
            await _store.PutAsync(entry.Id, json, cancellationToken);
            
            _logger.LogDebug("Stored DLQ entry {MessageId} for {Stream}/{Consumer}", 
                entry.Id, entry.OriginalStream, entry.OriginalConsumer);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to store DLQ entry {MessageId}", entry.Id);
            throw;
        }
    }

    /// <inheritdoc />
    public async Task<DlqMessageEntry?> GetAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);

        try
        {
            var json = await _store.GetAsync<string>(id, cancellationToken);
            
            if (string.IsNullOrEmpty(json))
            {
                return null;
            }

            return JsonSerializer.Deserialize<DlqMessageEntry>(json);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to retrieve DLQ entry {MessageId}", id);
            throw;
        }
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<DlqMessageEntry>> ListAsync(
        string? filterStream = null,
        string? filterConsumer = null,
        int limit = 100,
        CancellationToken cancellationToken = default)
    {
        //TODO:
        // Note: NATS KV doesn't support native filtering, so we'd need to:
        // 1. Use a separate index stream, or
        // 2. Watch all keys and filter in memory (expensive for large datasets)
        // 
        // For now, this returns an empty list with a log warning.
        // A production implementation would use a separate indexing strategy.
        
        _logger.LogWarning(
            "ListAsync is not fully implemented. Consider using a dedicated indexing strategy. " +
            "Filters: Stream={Stream}, Consumer={Consumer}, Limit={Limit}",
            filterStream, filterConsumer, limit);

        // Return empty list - full implementation would require indexing strategy
        await Task.CompletedTask; // Satisfy async signature
        return Array.Empty<DlqMessageEntry>();
    }

    /// <inheritdoc />
    public async Task<bool> UpdateStatusAsync(string id, DlqMessageStatus status, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);

        try
        {
            var entry = await GetAsync(id, cancellationToken);
            if (entry == null)
            {
                _logger.LogWarning("Cannot update status for DLQ entry {MessageId}: not found", id);
                return false;
            }

            var updatedEntry = entry with { Status = status };
            await StoreAsync(updatedEntry, cancellationToken);
            
            _logger.LogDebug("Updated DLQ entry {MessageId} status to {Status}", id, status);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to update DLQ entry {MessageId} status", id);
            throw;
        }
    }

    /// <inheritdoc />
    public async Task<bool> DeleteAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);

        try
        {
            // Check if entry exists first
            var entry = await GetAsync(id, cancellationToken);
            if (entry == null)
            {
                _logger.LogWarning("Cannot delete DLQ entry {MessageId}: not found", id);
                return false;
            }

            await _store.DeleteAsync(id, cancellationToken);
            _logger.LogDebug("Deleted DLQ entry {MessageId}", id);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to delete DLQ entry {MessageId}", id);
            throw;
        }
    }
}

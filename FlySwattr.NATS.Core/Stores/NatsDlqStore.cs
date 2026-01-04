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
}

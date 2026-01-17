using System.Text.Json;
using FlySwattr.NATS.Abstractions;
using Microsoft.Extensions.Logging;

namespace FlySwattr.NATS.Core;

/// <summary>
/// NATS Key-Value backed implementation of <see cref="IDlqStore"/>.
/// Stores DLQ entries in a dedicated KV bucket for persistence and querying.
/// Uses hierarchical keys ({stream}.{consumer}.{id}) to enable native NATS KV filtering.
/// </summary>
internal class NatsDlqStore : IDlqStore
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

    /// <summary>
    /// Builds a hierarchical KV key from stream, consumer, and entry ID.
    /// Format: {stream}.{consumer}.{id}
    /// </summary>
    private static string BuildKey(string stream, string consumer, string id) 
        => $"{SanitizeToken(stream)}.{SanitizeToken(consumer)}.{id}";
    
    /// <summary>
    /// Sanitizes a token for use in NATS KV keys by replacing reserved characters.
    /// </summary>
    private static string SanitizeToken(string token) 
        => token.Replace(".", "_").Replace("*", "_").Replace(">", "_");
    
    /// <summary>
    /// Builds the NATS wildcard pattern for filtering keys.
    /// </summary>
    private static string BuildFilterPattern(string? stream, string? consumer)
    {
        return (stream, consumer) switch
        {
            (not null, not null) => $"{SanitizeToken(stream)}.{SanitizeToken(consumer)}.*",
            (not null, null) => $"{SanitizeToken(stream)}.>",
            (null, not null) => $"*.{SanitizeToken(consumer)}.*",
            _ => ">"
        };
    }

    /// <inheritdoc />
    public async Task StoreAsync(DlqMessageEntry entry, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentException.ThrowIfNullOrWhiteSpace(entry.Id);

        try
        {
            var key = BuildKey(entry.OriginalStream, entry.OriginalConsumer, entry.Id);
            var json = JsonSerializer.Serialize(entry);
            await _store.PutAsync(key, json, cancellationToken);
            
            _logger.LogDebug("Stored DLQ entry {MessageId} for {Stream}/{Consumer} with key {Key}", 
                entry.Id, entry.OriginalStream, entry.OriginalConsumer, key);
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
            // The id parameter is the full hierarchical key
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
        try
        {
            var pattern = BuildFilterPattern(filterStream, filterConsumer);
            _logger.LogDebug("Listing DLQ entries with pattern {Pattern}, limit {Limit}", pattern, limit);
            
            var entries = new List<DlqMessageEntry>();
            
            await foreach (var key in _store.GetKeysAsync([pattern], cancellationToken))
            {
                if (entries.Count >= limit) 
                    break;
                
                var entry = await GetAsync(key, cancellationToken);
                if (entry != null)
                {
                    entries.Add(entry);
                }
            }
            
            _logger.LogDebug("Found {Count} DLQ entries matching filters", entries.Count);
            return entries;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to list DLQ entries with filters Stream={Stream}, Consumer={Consumer}", 
                filterStream, filterConsumer);
            throw;
        }
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
            // Re-store with the same key (id is already the full hierarchical key)
            var json = JsonSerializer.Serialize(updatedEntry);
            await _store.PutAsync(id, json, cancellationToken);
            
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
            // Check if entry exists first (id is the full hierarchical key)
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

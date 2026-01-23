using System.Text.Json;
using FlySwattr.NATS.Abstractions;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream.Models;
using NATS.Client.KeyValueStore;

namespace FlySwattr.NATS.Core;

/// <summary>
/// NATS Key-Value backed implementation of <see cref="IDlqStore"/>.
/// Stores DLQ entries in a dedicated KV bucket for persistence and querying.
/// Uses hierarchical keys ({stream}.{consumer}.{id}) to enable native NATS KV filtering.
/// </summary>
internal class NatsDlqStore : IDlqStore
{
    private const string BucketName = "fs-dlq-entries";
    
    private readonly INatsKVContext _kvContext;
    private readonly IKeyValueStore _store;
    private readonly ILogger<NatsDlqStore> _logger;
    
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private bool _isInitialized;

    public NatsDlqStore(
        INatsKVContext kvContext,
        Func<string, IKeyValueStore> storeFactory,
        ILogger<NatsDlqStore> logger)
    {
        _kvContext = kvContext;
        _store = storeFactory(BucketName);
        _logger = logger;
    }

    private async ValueTask EnsureInitializedAsync(CancellationToken cancellationToken)
    {
        if (_isInitialized) return;

        await _initLock.WaitAsync(cancellationToken);
        try
        {
            if (_isInitialized) return;

            try
            {
                // Ensure the DLQ bucket exists with File (Durable) storage.
                // CreateStoreAsync is generally idempotent (Create or Update).
                await _kvContext.CreateStoreAsync(new NatsKVConfig(BucketName) 
                { 
                    Storage = NatsKVStorageType.File,
                    Description = "Dead Letter Queue Storage",
                    History = 1
                }, cancellationToken: cancellationToken);
                
                _logger.LogDebug("Ensured DLQ KV bucket {BucketName} exists", BucketName);
            }
            catch (Exception ex)
            {
                // Log and continue - if it exists with incompatible config, we might get an error,
                // or if we lack permissions. We'll let the actual operations fail if needed.
                _logger.LogWarning(ex, "Attempt to ensure DLQ bucket {BucketName} failed. Proceeding hoping it exists.", BucketName);
            }

            _isInitialized = true;
        }
        finally
        {
            _initLock.Release();
        }
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

        await EnsureInitializedAsync(cancellationToken);

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

        await EnsureInitializedAsync(cancellationToken);

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
        await EnsureInitializedAsync(cancellationToken);

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

        await EnsureInitializedAsync(cancellationToken);

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

        await EnsureInitializedAsync(cancellationToken);

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

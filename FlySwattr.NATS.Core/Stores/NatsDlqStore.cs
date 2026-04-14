using System.Text.Json;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.JetStream.Models;
using NATS.Client.KeyValueStore;

namespace FlySwattr.NATS.Core;

/// <summary>
/// NATS Key-Value backed implementation of <see cref="IDlqStore"/>.
/// Stores DLQ entries in a dedicated KV bucket for persistence and querying.
/// Uses hierarchical keys ({stream}.{consumer}.{id}) to enable native NATS KV filtering.
/// </summary>
/// <remarks>
/// <para><b>Serialization format:</b> DLQ <em>entries</em> (metadata records stored in this KV
/// bucket) are serialized as JSON.  This is intentional: JSON is human-readable, schema-free, and
/// easily inspected or edited by operators in emergency scenarios without needing a MemoryPack
/// toolchain.  The operational cost (larger wire size) is acceptable because DLQ writes are on
/// the slow/failure path, not the hot publish path.</para>
/// <para>DLQ <em>messages</em> (the original failing payloads re-published to a DLQ JetStream
/// subject by <see cref="FlySwattr.NATS.Hosting.Services.DefaultDlqPoisonHandler{T}"/>) are
/// serialized with MemoryPack, consistent with all other JetStream messages on the wire.</para>
/// <para>The two formats coexist deliberately: KV entries are for human/operator consumption;
/// stream messages are for machine consumption by replay consumers.</para>
/// </remarks>
internal class NatsDlqStore : IDlqStore
{
    private const string BucketName = "fs-dlq-entries";
    
    private readonly INatsKVContext _kvContext;
    private readonly ILogger<NatsDlqStore> _logger;
    private readonly DlqStoreFailureOptions _failureOptions;
    
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private bool _isInitialized;
    private bool _fallbackMode;
    private INatsKVStore? _store;

    public NatsDlqStore(
        INatsKVContext kvContext,
        ILogger<NatsDlqStore> logger,
        IOptions<DlqStoreFailureOptions>? failureOptions = null)
    {
        _kvContext = kvContext;
        _logger = logger;
        _failureOptions = failureOptions?.Value ?? new DlqStoreFailureOptions();
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
                // First check if the bucket already exists to avoid conflict errors
                try
                {
                    _store = await _kvContext.GetStoreAsync(BucketName, cancellationToken: cancellationToken);
                    _logger.LogDebug("DLQ KV bucket {BucketName} already exists", BucketName);
                }
                catch
                {
                    // If getting the store fails, assume it doesn't exist and try to create it
                    await _kvContext.CreateStoreAsync(new NatsKVConfig(BucketName) 
                    { 
                        Storage = NatsKVStorageType.File,
                        Description = "Dead Letter Queue Storage",
                        History = 1
                    }, cancellationToken: cancellationToken);
                    _store = await _kvContext.GetStoreAsync(BucketName, cancellationToken: cancellationToken);
                    
                    _logger.LogDebug("Created DLQ KV bucket {BucketName}", BucketName);
                }
            }
            catch (Exception ex)
            {
                switch (_failureOptions.InitializationFailurePolicy)
                {
                    case DlqStoreFailurePolicy.Propagate:
                        _logger.LogError(ex, "DLQ bucket {BucketName} initialization failed. Policy: Propagate — re-throwing.", BucketName);
                        throw;

                    case DlqStoreFailurePolicy.FallbackToLogOnly:
                        _logger.LogWarning(ex,
                            "DLQ bucket {BucketName} initialization failed. Policy: FallbackToLogOnly — " +
                            "DLQ entries will be written to logs instead of KV store.", BucketName);
                        _fallbackMode = true;
                        break;

                    case DlqStoreFailurePolicy.LogAndContinue:
                    default:
                        _logger.LogWarning(ex,
                            "Attempt to ensure DLQ bucket {BucketName} failed. Policy: LogAndContinue — " +
                            "proceeding; individual operations may fail.", BucketName);
                        break;
                }
            }

            _isInitialized = true;
        }
        finally
        {
            _initLock.Release();
        }
    }

    private INatsKVStore Store
        => _store ?? throw new InvalidOperationException("DLQ KV bucket is not initialized.");

    /// <summary>
    /// Checks if the store is in fallback mode or has no store reference,
    /// and applies the configured operation failure policy.
    /// Returns true if the caller should short-circuit (fallback handled).
    /// </summary>
    private bool TryHandleUnavailable(string operation, string id)
    {
        if (!_fallbackMode && _store != null)
            return false;

        if (_fallbackMode)
        {
            _logger.LogError(
                "DLQ store is in fallback-to-log mode. Operation {Operation} for entry {Id} " +
                "will not be persisted to KV store.", operation, id);
            return true;
        }

        // _store is null — initialization failed but policy allowed continue
        switch (_failureOptions.OperationFailurePolicy)
        {
            case DlqStoreFailurePolicy.Propagate:
                throw new InvalidOperationException(
                    $"DLQ KV bucket is not initialized. Cannot perform {operation} for entry {id}.");

            case DlqStoreFailurePolicy.FallbackToLogOnly:
                _logger.LogError(
                    "DLQ store unavailable. Operation {Operation} for entry {Id} " +
                    "will be logged only.", operation, id);
                return true;

            case DlqStoreFailurePolicy.LogAndContinue:
            default:
                _logger.LogWarning(
                    "DLQ store unavailable. Operation {Operation} for entry {Id} skipped.", operation, id);
                return true;
        }
    }

    /// <summary>
    /// The persistent KV key is the DLQ entry ID itself.
    /// Entry IDs are expected to be stable and hierarchical (for example: stream.consumer.sequence)
    /// so list filters still map cleanly onto native KV wildcard lookups.
    /// </summary>
    private static string BuildKey(string id)
        => SanitizeToken(id);
    
    /// <summary>
    /// Sanitizes a token for use in NATS KV keys by replacing reserved characters.
    /// </summary>
    private static string SanitizeToken(string token) 
        => token.Replace("*", "_star_").Replace(">", "_gt_");
    
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
        ValidateEntryId(entry);

        await EnsureInitializedAsync(cancellationToken);

        if (TryHandleUnavailable("Store", entry.Id))
        {
            // Log the full entry for manual recovery
            _logger.LogError(
                "DLQ entry logged (not persisted): Id={Id}, Stream={Stream}, Consumer={Consumer}, " +
                "Subject={Subject}, Sequence={Sequence}, Error={Error}",
                entry.Id, entry.OriginalStream, entry.OriginalConsumer,
                entry.OriginalSubject, entry.OriginalSequence, entry.ErrorReason);
            return;
        }

        try
        {
            var key = BuildKey(entry.Id);
            var json = JsonSerializer.Serialize(entry);
            await Store.PutAsync(key, json, cancellationToken: cancellationToken);
            
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

        if (TryHandleUnavailable("Get", id))
            return null;

        try
        {
            var kvEntry = await Store.GetEntryAsync<string>(BuildKey(id), cancellationToken: cancellationToken);
            if (string.IsNullOrEmpty(kvEntry.Value))
            {
                return null;
            }

            var entry = JsonSerializer.Deserialize<DlqMessageEntry>(kvEntry.Value);
            return entry is null ? null : entry with { StoreRevision = kvEntry.Revision };
        }
        catch (NatsKVKeyNotFoundException)
        {
            return null;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to retrieve DLQ entry {MessageId}", id);
            throw;
        }
    }

    // Maximum concurrent KV reads issued by ListAsync to keep network pressure bounded.
    private const int ListMaxParallelism = 16;

    /// <inheritdoc />
    public async Task<IReadOnlyList<DlqMessageEntry>> ListAsync(
        string? filterStream = null,
        string? filterConsumer = null,
        int limit = 100,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        if (TryHandleUnavailable("List", "(all)"))
            return Array.Empty<DlqMessageEntry>();

        try
        {
            var pattern = BuildFilterPattern(filterStream, filterConsumer);
            _logger.LogDebug("Listing DLQ entries with pattern {Pattern}, limit {Limit}", pattern, limit);

            // Phase 1: collect up to `limit` keys from the streaming key scan.
            // Key enumeration is inherently sequential; reading entries in bulk happens below.
            var keys = new List<string>(Math.Min(limit, 64));
            await foreach (var key in Store.GetKeysAsync([pattern], cancellationToken: cancellationToken))
            {
                keys.Add(key);
                if (keys.Count >= limit)
                    break;
            }

            if (keys.Count == 0)
            {
                _logger.LogDebug("No DLQ entries found matching filters");
                return Array.Empty<DlqMessageEntry>();
            }

            // Phase 2: fetch all entries in parallel with bounded concurrency.
            // Sequential fetches for large DLQs caused O(N) round-trip latency; parallel
            // fetches cut wall-clock time to O(N / parallelism).
            using var semaphore = new SemaphoreSlim(Math.Min(ListMaxParallelism, keys.Count));
            var fetchTasks = keys.Select(async key =>
            {
                await semaphore.WaitAsync(cancellationToken);
                try
                {
                    return await GetAsync(key, cancellationToken);
                }
                finally
                {
                    semaphore.Release();
                }
            }).ToArray();

            var fetched = await Task.WhenAll(fetchTasks);
            var entries = fetched.OfType<DlqMessageEntry>().ToList();

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
    public async Task<DlqEntryUpdateResult> UpdateStatusAsync(
        string id,
        DlqMessageStatus status,
        ulong? expectedRevision = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);

        await EnsureInitializedAsync(cancellationToken);

        if (TryHandleUnavailable("UpdateStatus", id))
            return new DlqEntryUpdateResult(DlqEntryUpdateOutcome.NotFound);

        try
        {
            var storageKey = BuildKey(id);
            var kvEntry = await Store.GetEntryAsync<string>(storageKey, cancellationToken: cancellationToken);
            if (string.IsNullOrEmpty(kvEntry.Value))
            {
                _logger.LogWarning("Cannot update status for DLQ entry {MessageId}: not found", id);
                return new DlqEntryUpdateResult(DlqEntryUpdateOutcome.NotFound);
            }

            var entry = JsonSerializer.Deserialize<DlqMessageEntry>(kvEntry.Value);
            if (entry == null)
            {
                throw new JsonException($"DLQ entry {id} could not be deserialized.");
            }

            var updatedEntry = entry with { Status = status, StoreRevision = null };
            var json = JsonSerializer.Serialize(updatedEntry);
            var newRevision = await Store.UpdateAsync(
                storageKey,
                json,
                expectedRevision ?? kvEntry.Revision,
                serializer: null,
                cancellationToken: cancellationToken);

            _logger.LogDebug("Updated DLQ entry {MessageId} status to {Status} at revision {Revision}", id, status, newRevision);
            return new DlqEntryUpdateResult(DlqEntryUpdateOutcome.Updated, newRevision);
        }
        catch (NatsKVKeyNotFoundException)
        {
            _logger.LogWarning("Cannot update status for DLQ entry {MessageId}: not found", id);
            return new DlqEntryUpdateResult(DlqEntryUpdateOutcome.NotFound);
        }
        catch (NatsKVWrongLastRevisionException)
        {
            _logger.LogWarning("Cannot update status for DLQ entry {MessageId}: revision conflict", id);
            return new DlqEntryUpdateResult(DlqEntryUpdateOutcome.Conflict);
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

        if (TryHandleUnavailable("Delete", id))
            return false;

        try
        {
            // Check if entry exists first (id is the full hierarchical key)
            var storageKey = BuildKey(id);
            var entry = await GetAsync(id, cancellationToken);
            if (entry == null)
            {
                _logger.LogWarning("Cannot delete DLQ entry {MessageId}: not found", id);
                return false;
            }

            await Store.DeleteAsync(storageKey, cancellationToken: cancellationToken);
            _logger.LogDebug("Deleted DLQ entry {MessageId}", id);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to delete DLQ entry {MessageId}", id);
            throw;
        }
    }

    private static void ValidateEntryId(DlqMessageEntry entry)
    {
        var expectedPrefix = $"{entry.OriginalStream}.{entry.OriginalConsumer}.";
        if (!entry.Id.StartsWith(expectedPrefix, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"DLQ entry id '{entry.Id}' must start with '{expectedPrefix}' to remain consistent across store and remediation operations.",
                nameof(entry));
        }
    }
}

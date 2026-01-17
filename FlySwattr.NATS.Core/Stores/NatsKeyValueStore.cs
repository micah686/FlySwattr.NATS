using System.Net.Sockets;
using System.Runtime.CompilerServices;
using FlySwattr.NATS.Abstractions;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.KeyValueStore;

namespace FlySwattr.NATS.Core.Stores;

internal class NatsKeyValueStore : IKeyValueStore, IAsyncDisposable
{
    private readonly INatsKVContext _kvContext;
    private readonly string _bucket;
    private readonly ILogger<NatsKeyValueStore> _logger;
    
    // Internal cache of the STORE HANDLE, not the values.
    // // This is safe in Core as it's just avoiding the lookup overhead.
    private readonly SemaphoreSlim _storeLock = new(1, 1);
    private INatsKVStore? _cachedStore;

    public NatsKeyValueStore(
        INatsKVContext kvContext, 
        string bucket, 
        ILogger<NatsKeyValueStore> logger)
    {
        _kvContext = kvContext;
        _bucket = bucket;
        _logger = logger;
    }

    public ValueTask DisposeAsync()
    {
        _storeLock.Dispose();
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    private async Task<INatsKVStore> GetStoreAsync(CancellationToken cancellationToken)
    {
        // Use Volatile.Read for proper memory barrier on first check
        var store = Volatile.Read(ref _cachedStore);
        if (store != null) return store;

        await _storeLock.WaitAsync(cancellationToken);
        try
        {
            // Double-check with Volatile.Read after acquiring lock
            store = Volatile.Read(ref _cachedStore);
            if (store != null) return store;

            store = await _kvContext.GetStoreAsync(_bucket, cancellationToken: cancellationToken);
            // Use Volatile.Write to ensure proper memory barrier before publishing
            Volatile.Write(ref _cachedStore, store);
            return store;
        }
        finally
        {
            _storeLock.Release();
        }
    }

    /// <summary>
    /// Invalidates the cached store handle. Call this on transient failures to force re-fetching on next operation.
    /// </summary>
    public void InvalidateCache()
    {
        // Use Interlocked.Exchange for atomic cache invalidation
        Interlocked.Exchange(ref _cachedStore, null);
        _logger.LogDebug("KV store cache invalidated for bucket {Bucket}", _bucket);
    }

    public async Task PutAsync<T>(string key, T value, CancellationToken cancellationToken = default)
    {
        try
        {
            var store = await GetStoreAsync(cancellationToken);
            await store.PutAsync(key, value, cancellationToken: cancellationToken);
        }
        catch (NatsJSApiException ex) when (ex.Error.Code is 503 or 504)
        {
            InvalidateCache();
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error putting key {Key} to bucket {Bucket}", key, _bucket);
            throw;
        }
    }

    public async Task<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default)
    {
        try
        {
            var store = await GetStoreAsync(cancellationToken);
            var entry = await store.GetEntryAsync<T>(key, cancellationToken: cancellationToken);
            return entry.Value;
        }
        catch (NatsKVKeyNotFoundException)
        {
            return default;
        }
        catch (NatsJSApiException ex) when (ex.Error.Code is 503 or 504)
        {
            InvalidateCache();
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting key {Key} from bucket {Bucket}", key, _bucket);
            throw;
        }
    }

    public async Task DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        try
        {
            var store = await GetStoreAsync(cancellationToken);
            await store.DeleteAsync(key, cancellationToken: cancellationToken);
        }
        catch (NatsJSApiException ex) when (ex.Error.Code is 503 or 504)
        {
            InvalidateCache();
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting key {Key} from bucket {Bucket}", key, _bucket);
            throw;
        }
    }

    public async Task WatchAsync<T>(string key, Func<KvChangeEvent<T>, Task> handler, CancellationToken cancellationToken = default)
    {
        // If key is empty or wildcard, watch all (or pattern)
        var keyToWatch = string.IsNullOrEmpty(key) ? ">" : key;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var store = await GetStoreAsync(cancellationToken);

                await foreach (var entry in store.WatchAsync<T>(keyToWatch, cancellationToken: cancellationToken))
                {
                    var changeType = entry.Operation switch
                    {
                        NatsKVOperation.Del => KvChangeType.Delete,
                        NatsKVOperation.Purge => KvChangeType.Delete,
                        _ => KvChangeType.Put
                    };

                    var changeEvent = new KvChangeEvent<T>(
                        Type: changeType,
                        Value: changeType == KvChangeType.Put ? entry.Value : default,
                        Key: entry.Key,
                        Revision: entry.Revision
                    );

                    await handler(changeEvent);
                }
            }
            catch (OperationCanceledException)
            {
                // Graceful shutdown requested
                break;
            }
            catch (Exception ex) when (IsTransientException(ex))
            {
                InvalidateCache(); // Refresh store handle on reconnection
                _logger.LogWarning(ex, "KV watch for key {Key} in bucket {Bucket} dropped; retrying in 1s", key, _bucket);
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Fatal error watching key {Key} in bucket {Bucket}", key, _bucket);
                throw;
            }
        }
    }

    public async IAsyncEnumerable<string> GetKeysAsync(
        IEnumerable<string> patterns, 
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        INatsKVStore store;
        try
        {
            store = await GetStoreAsync(cancellationToken);
        }
        catch (NatsJSApiException ex) when (ex.Error.Code is 503 or 504)
        {
            InvalidateCache();
            throw;
        }

        await foreach (var key in store.GetKeysAsync(patterns, cancellationToken: cancellationToken))
        {
            yield return key;
        }
    }

    private static bool IsTransientException(Exception ex)
    {
        // Network-related exceptions
        if (ex is IOException or TimeoutException or SocketException)
            return true;

        // NATS-specific JS API transient errors
        // Check this BEFORE generic NatsException to avoid retrying on permanent API errors (like 400 Bad Request)
        if (ex is NatsJSApiException jsEx)
        {
            // 503 = Service Unavailable, 504 = Gateway Timeout, 404 = Bucket not found (during recreation)
            return jsEx.Error.Code is 503 or 504 or 404;
        }

        // Other NATS-specific basic exceptions (connection issues, etc.)
        if (ex is NatsException || ex is NatsNoRespondersException)
            return true;

        return false;
    }
}
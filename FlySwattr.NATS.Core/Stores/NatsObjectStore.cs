using System.Net.Sockets;
using FlySwattr.NATS.Abstractions;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.ObjectStore;
using NATS.Client.ObjectStore.Models;

namespace FlySwattr.NATS.Core.Stores;

internal class NatsObjectStore : IObjectStore, IAsyncDisposable
{
    private readonly INatsObjContext _objContext;
    private readonly string _bucket;
    private readonly ILogger<NatsObjectStore> _logger;
    private readonly SemaphoreSlim _storeLock = new(1, 1);
    private INatsObjStore? _cachedStore;

    public NatsObjectStore(INatsObjContext objContext, string bucket, ILogger<NatsObjectStore> logger)
    {
        _objContext = objContext;
        _bucket = bucket;
        _logger = logger;
    }

    public ValueTask DisposeAsync()
    {
        _storeLock.Dispose();
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    private async Task<INatsObjStore> GetStoreAsync(CancellationToken cancellationToken)
    {
        var store = Volatile.Read(ref _cachedStore);
        if (store != null) return store;

        await _storeLock.WaitAsync(cancellationToken);
        try
        {
            store = Volatile.Read(ref _cachedStore);
            if (store != null) return store;

            store = await _objContext.GetObjectStoreAsync(_bucket, cancellationToken: cancellationToken);
            Volatile.Write(ref _cachedStore, store);
            return store;
        }
        finally
        {
            _storeLock.Release();
        }
    }

    private void InvalidateCache()
    {
        Interlocked.Exchange(ref _cachedStore, null);
        _logger.LogDebug("Object store cache invalidated for bucket {Bucket}", _bucket);
    }

    private async Task<T> ExecuteWithRetryAsync<T>(Func<CancellationToken, Task<T>> action, CancellationToken ct)
    {
        int attempts = 0;
        while (true)
        {
            try
            {
                return await action(ct);
            }
            catch (Exception ex) when (IsTransient(ex) && attempts < 3)
            {
                attempts++;
                await Task.Delay(1000 * attempts, ct); // Simple linear backoff
            }
        }
    }

    private async Task ExecuteWithRetryAsync(Func<CancellationToken, Task> action, CancellationToken ct)
    {
        int attempts = 0;
        while (true)
        {
            try
            {
                await action(ct);
                return;
            }
            catch (Exception ex) when (IsTransient(ex) && attempts < 3)
            {
                attempts++;
                await Task.Delay(1000 * attempts, ct); // Simple linear backoff
            }
        }
    }

    private static bool IsTransient(Exception ex)
    {
        // Network-related exceptions
        if (ex is IOException or TimeoutException or SocketException)
            return true;

        // NATS-specific JS API transient errors
        if (ex is NatsJSApiException jsEx)
        {
            // 503 = Service Unavailable, 504 = Gateway Timeout
            return jsEx.Error.Code is 503 or 504; 
        }

        // Other NATS-specific basic exceptions (connection issues, etc.)
        if (ex is NatsException || ex is NatsNoRespondersException)
            return true;

        return false;
    }

    public async Task<string> PutAsync(string key, Stream data, CancellationToken cancellationToken = default)
    {
        try
        {
            return await ExecuteWithRetryAsync(async token =>
            {
                // Reset stream position for retry (if stream supports seeking)
                if (data.CanSeek)
                {
                    data.Position = 0;
                }
                
                var store = await GetStoreAsync(token);
                var meta = await store.PutAsync(key, data, leaveOpen: true, cancellationToken: token);
                return meta.Name;
            }, cancellationToken);
        }
        catch (NatsJSApiException ex) when (ex.Error.Code is 503 or 504)
        {
            InvalidateCache();
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error uploading object {Key} to bucket {Bucket}", key, _bucket);
            throw;
        }
    }

    public async Task GetAsync(string key, Stream target, CancellationToken cancellationToken = default)
    {
        try
        {
            await ExecuteWithRetryAsync(async token =>
            {
                // Reset target stream position for retry (if stream supports seeking)
                if (target.CanSeek)
                {
                    target.Position = 0;
                    target.SetLength(0); // Clear any partial data from previous attempt
                }
                
                var store = await GetStoreAsync(token);
                await store.GetAsync(key, target, cancellationToken: token);
            }, cancellationToken);
        }
        catch (NatsJSApiException ex) when (ex.Error.Code is 503 or 504)
        {
            InvalidateCache();
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error downloading object {Key} to stream from bucket {Bucket}", key, _bucket);
            throw;
        }
    }

    public async Task DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        try
        {
            await ExecuteWithRetryAsync(async token =>
            {
                var store = await GetStoreAsync(token);
                await store.DeleteAsync(key, cancellationToken: token);
            }, cancellationToken);
        }
        catch (NatsJSApiException ex) when (ex.Error.Code is 503 or 504)
        {
            InvalidateCache();
            throw;
        }
        catch (NatsObjNotFoundException)
        {
            _logger.LogDebug("Object {Key} not found in bucket {Bucket} during delete (idempotent)", key, _bucket);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting object {Key} from bucket {Bucket}", key, _bucket);
            throw;
        }
    }

    public async Task<ObjectInfo?> GetInfoAsync(string key, bool showDeleted = false, CancellationToken cancellationToken = default)
    {
        try
        {
            return await ExecuteWithRetryAsync(async token =>
            {
                var store = await GetStoreAsync(token);
                try 
                {
                    var info = await store.GetInfoAsync(key, cancellationToken: token);
                    if (info.Deleted && !showDeleted) return null;
                    return MapToObjectInfo(info);
                }
                catch (NatsObjNotFoundException)
                {
                    if (!showDeleted) return null;
                    
                    await foreach(var item in store.ListAsync(cancellationToken: token))
                    {
                        if (item.Name == key && item.Deleted)
                        {
                            return MapToObjectInfo(item);
                        }
                    }
                    return null;
                }
            }, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting info for object {Key} from bucket {Bucket}", key, _bucket);
            throw;
        }
    }

    public async Task UpdateMetaAsync(string key, ObjectMetaInfo meta, CancellationToken cancellationToken = default)
    {
        try
        {
            var store = await GetStoreAsync(cancellationToken);
            var current = await store.GetInfoAsync(key, cancellationToken: cancellationToken);
            
            var headers = new Dictionary<string, string[]>();
            if (meta.Headers != null)
            {
                foreach(var kv in meta.Headers)
                {
                    headers[kv.Key] = new[] { kv.Value };
                }
            }

            var newMeta = current with 
            { 
                Name = meta.Name,
                Description = meta.Description,
                Headers = headers
            };
            
            await store.UpdateMetaAsync(key, newMeta, cancellationToken: cancellationToken);
        }
        catch (Exception ex)
        {
             _logger.LogError(ex, "Error updating meta for {Key}", key);
             throw;
        }
    }

    public async Task<IEnumerable<ObjectInfo>> ListAsync(bool showDeleted = false, CancellationToken cancellationToken = default)
    {
        try
        {
            var store = await GetStoreAsync(cancellationToken);
            var result = new List<ObjectInfo>();
            await foreach(var info in store.ListAsync(cancellationToken: cancellationToken))
            {
                if (!showDeleted && info.Deleted) continue;
                result.Add(MapToObjectInfo(info));
            }
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error listing objects in bucket {Bucket}", _bucket);
            throw;
        }
    }

    public async Task WatchAsync(Func<ObjectInfo, Task> handler, CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var store = await GetStoreAsync(cancellationToken);
                await foreach(var change in store.WatchAsync(cancellationToken: cancellationToken))
                {
                    await handler(MapToObjectInfo(change));
                }
                return;
            }
            catch (NatsJSApiException ex) when (ex.Error.Code is 503 or 504)
            {
                InvalidateCache();
                try { await Task.Delay(1000, cancellationToken); } catch { }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error watching bucket {Bucket}", _bucket);
                throw;
            }
        }
    }

    private ObjectInfo MapToObjectInfo(ObjectMetadata info)
    {
         var headers = new Dictionary<string, string>();
         if (info.Headers != null)
         {
             foreach(var kv in info.Headers)
             {
                 var val = kv.Value?.Length > 0 ? kv.Value[0] : string.Empty;
                 headers[kv.Key] = val;
             }
         }
         
         return new ObjectInfo(
            info.Bucket ?? _bucket,
            info.Name ?? string.Empty,
            (long)info.Size,
            info.MTime,
            info.Digest ?? string.Empty,
            info.Deleted,
            info.Description,
            headers
         );
    }
}

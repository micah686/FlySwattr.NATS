using FlySwattr.NATS.Abstractions;
using Microsoft.Extensions.Logging;
using ZiggyCreatures.Caching.Fusion;

namespace FlySwattr.NATS.Caching.Stores;

public class CachingKeyValueStore : IKeyValueStore
{
    private readonly IKeyValueStore _inner;
    private readonly IFusionCache _cache;
    private readonly string _bucketName;
    private readonly ILogger _logger;

    public CachingKeyValueStore(IKeyValueStore inner, IFusionCache cache, string bucketName, ILogger logger)
    {
        _inner = inner;
        _cache = cache;
        _bucketName = bucketName;
        _logger = logger;
    }

    private string GetCacheKey(string key) => $"{_bucketName}:{key}";

    public async Task<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default)
    {
        var cacheKey = GetCacheKey(key);
        
        // FusionCache handles the "GetOrSet" logic, including fail-safe (stale data) if NATS is down
        return await _cache.GetOrSetAsync<T?>(
            cacheKey,
            async (ctx, ct) => await _inner.GetAsync<T>(key, ct),
            tags: new[] { _bucketName }, 
            token: cancellationToken
        );
    }

    public async Task PutAsync<T>(string key, T value, CancellationToken cancellationToken = default)
    {
        // Write-through or Write-Invalidate? 
        // Write-Invalidate is safer for distributed systems to avoid race conditions.
        await _inner.PutAsync(key, value, cancellationToken);
        
        // Invalidate local cache
        await _cache.RemoveAsync(GetCacheKey(key), token: cancellationToken);
    }

    public async Task DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        await _inner.DeleteAsync(key, cancellationToken);
        await _cache.RemoveAsync(GetCacheKey(key), token: cancellationToken);
    }

    public async Task WatchAsync<T>(string key, Func<KvChangeEvent<T>, Task> handler, CancellationToken cancellationToken = default)
    {
        // Wrap the handler to invalidate cache on external changes
        await _inner.WatchAsync<T>(key, async (change) => 
        {
            if (change.Key!= null)
            {
                await _cache.RemoveAsync(GetCacheKey(change.Key), token: cancellationToken);
            }
            await handler(change);
        }, cancellationToken);
    }
}
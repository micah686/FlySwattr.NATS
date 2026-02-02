using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Caching.Configuration;
using Microsoft.Extensions.Logging;
using ZiggyCreatures.Caching.Fusion;

namespace FlySwattr.NATS.Caching.Stores;

/// <summary>
/// A decorator around <see cref="IKeyValueStore"/> that adds L1 caching via FusionCache.
/// Features include fail-safe (stale data on NATS outage), soft/hard timeouts, and stampede protection.
/// </summary>
internal class CachingKeyValueStore : IKeyValueStore
{
    private readonly IKeyValueStore _inner;
    private readonly IFusionCache _cache;
    private readonly FusionCacheConfiguration _cacheConfig;
    private readonly string _bucketName;
    private readonly ILogger _logger;

    public CachingKeyValueStore(
        IKeyValueStore inner, 
        IFusionCache cache, 
        FusionCacheConfiguration cacheConfig,
        string bucketName, 
        ILogger logger)
    {
        _inner = inner;
        _cache = cache;
        _cacheConfig = cacheConfig;
        _bucketName = bucketName;
        _logger = logger;
    }

    private string GetCacheKey(string key) => $"{_bucketName}:{key}";

    public async Task<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default)
    {
        var cacheKey = GetCacheKey(key);
        
        try
        {
            // Use FusionCache with factory pattern
            // - Provides L1 memory caching
            // - Fail-safe: returns stale data if NATS is unavailable
            // - Stampede protection: single factory call for concurrent requests
            return await _cache.GetOrSetAsync<T?>(
                cacheKey,
                async (ctx, ct) =>
                {
                    var result = await _inner.GetAsync<T>(key, ct);
                    
                    // Key not found - cache null with shorter duration
                    if (result is null)
                    {
                        ctx.Options.Duration = _cacheConfig.NotFoundCacheDuration;
                    }
                    
                    return result;
                },
                new FusionCacheEntryOptions
                {
                    Duration = _cacheConfig.MemoryCacheDuration,
                    FailSafeMaxDuration = _cacheConfig.FailSafeMaxDuration,
                    FactorySoftTimeout = _cacheConfig.FactorySoftTimeout,
                    FactoryHardTimeout = _cacheConfig.FactoryHardTimeout,
                    IsFailSafeEnabled = true,
                    AllowTimedOutFactoryBackgroundCompletion = true
                },
                cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting key {Key} from bucket {Bucket} with caching", key, _bucketName);
            throw;
        }
    }

    public async Task PutAsync<T>(string key, T value, CancellationToken cancellationToken = default)
    {
        // Write-Invalidate strategy is safer for distributed systems to avoid race conditions.
        await _inner.PutAsync(key, value, cancellationToken);
        
        // Invalidate local cache so next read gets fresh value
        await _cache.RemoveAsync(GetCacheKey(key), token: cancellationToken);
        _logger.LogDebug("Value cache invalidated for key {Key} in bucket {Bucket}", key, _bucketName);
    }

    public async Task DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        await _inner.DeleteAsync(key, cancellationToken);
        await _cache.RemoveAsync(GetCacheKey(key), token: cancellationToken);
        _logger.LogDebug("Value cache invalidated for key {Key} in bucket {Bucket}", key, _bucketName);
    }

    public async Task WatchAsync<T>(string key, Func<KvChangeEvent<T>, Task> handler, CancellationToken cancellationToken = default)
    {
        // Wrap the handler to invalidate cache on external changes
        await _inner.WatchAsync<T>(key, async (change) => 
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (change.Key != null)
            {
                // Invalidate cache on any change so subsequent reads get fresh data
                await _cache.RemoveAsync(GetCacheKey(change.Key), token: cancellationToken);
            }
            await handler(change);
        }, cancellationToken);
    }

    public IAsyncEnumerable<string> GetKeysAsync(IEnumerable<string> patterns, CancellationToken cancellationToken = default)
    {
        // Direct passthrough - no caching for key enumeration
        return _inner.GetKeysAsync(patterns, cancellationToken);
    }
}
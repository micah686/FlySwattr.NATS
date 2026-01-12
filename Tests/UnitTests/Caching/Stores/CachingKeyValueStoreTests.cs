using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Caching.Configuration;
using FlySwattr.NATS.Caching.Stores;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Shouldly;
using TUnit.Core;
using ZiggyCreatures.Caching.Fusion;

namespace UnitTests.Caching.Stores;

[Property("nTag", "Caching")]
public class CachingKeyValueStoreTests : IAsyncDisposable
{
    private readonly IKeyValueStore _innerStore;
    private readonly IFusionCache _fusionCache;
    private readonly FusionCacheConfiguration _cacheConfig;
    private readonly ILogger _logger;
    private readonly CachingKeyValueStore _sut;
    private readonly string _bucketName = "test-bucket";

    public CachingKeyValueStoreTests()
    {
        _innerStore = Substitute.For<IKeyValueStore>();
        _fusionCache = new FusionCache(new FusionCacheOptions());
        _logger = Substitute.For<ILogger>();
        _cacheConfig = new FusionCacheConfiguration
        {
            MemoryCacheDuration = TimeSpan.FromMinutes(1),
            FailSafeMaxDuration = TimeSpan.FromMinutes(5)
        };

        _sut = new CachingKeyValueStore(_innerStore, _fusionCache, _cacheConfig, _bucketName, _logger);
    }

    public async ValueTask DisposeAsync()
    {
        _fusionCache.Dispose();
        await Task.CompletedTask;
    }

    [Test]
    public async Task GetAsync_ShouldReturnCachedValue_WhenAvailable()
    {
        // Arrange
        var key = "key1";
        var value = "cached-value";
        await _fusionCache.SetAsync($"{_bucketName}:{key}", value);

        // Act
        var result = await _sut.GetAsync<string>(key);

        // Assert
        result.ShouldBe(value);
        await _innerStore.DidNotReceive().GetAsync<string>(key, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetAsync_ShouldFetchFromInnerStore_AndCache_WhenCacheMiss()
    {
        // Arrange
        var key = "key1";
        var value = "inner-value";
        _innerStore.GetAsync<string>(key, Arg.Any<CancellationToken>()).Returns(value);

        // Act
        var result = await _sut.GetAsync<string>(key);

        // Assert
        result.ShouldBe(value);
        await _innerStore.Received(1).GetAsync<string>(key, Arg.Any<CancellationToken>());
        
        var cachedValue = await _fusionCache.GetOrDefaultAsync<string>($"{_bucketName}:{key}");
        cachedValue.ShouldBe(value);
    }

    [Test]
    public async Task PutAsync_ShouldInvalidateCache()
    {
        // Arrange
        var key = "key1";
        await _fusionCache.SetAsync($"{_bucketName}:{key}", "old-value");

        // Act
        await _sut.PutAsync(key, "new-value");

        // Assert
        await _innerStore.Received(1).PutAsync(key, "new-value", Arg.Any<CancellationToken>());
        
        var cachedValue = await _fusionCache.GetOrDefaultAsync<string>($"{_bucketName}:{key}");
        cachedValue.ShouldBeNull();
    }

    [Test]
    public async Task DeleteAsync_ShouldInvalidateCache()
    {
        // Arrange
        var key = "key1";
        await _fusionCache.SetAsync($"{_bucketName}:{key}", "value");

        // Act
        await _sut.DeleteAsync(key);

        // Assert
        await _innerStore.Received(1).DeleteAsync(key, Arg.Any<CancellationToken>());
        
        var cachedValue = await _fusionCache.GetOrDefaultAsync<string>($"{_bucketName}:{key}");
        cachedValue.ShouldBeNull();
    }

    [Test]
    public async Task GetAsync_ShouldUseFailSafe_OnInnerStoreFailure()
    {
        // Arrange
        var key = "fail-safe-key";
        var cachedValue = "stale-value";
        
        // Populate cache with options that allow fail-safe
        await _fusionCache.SetAsync($"{_bucketName}:{key}", cachedValue, new FusionCacheEntryOptions
        {
            Duration = TimeSpan.FromMinutes(1),
            IsFailSafeEnabled = true,
            FailSafeMaxDuration = TimeSpan.FromHours(1)
        });

        // Simulate expiration to trigger factory call
        _fusionCache.Expire($"{_bucketName}:{key}");

        // Inner store fails
        _innerStore.GetAsync<string>(key, Arg.Any<CancellationToken>())
            .ThrowsAsync(new Exception("NATS down"));

        // Act
        var result = await _sut.GetAsync<string>(key);

        // Assert
        // Should return stale value instead of throwing
        result.ShouldBe(cachedValue);
    }

    [Test]
    public async Task WatchAsync_ShouldInvalidateCache_OnChange()
    {
        // Arrange
        var key = "watched-key";
        await _fusionCache.SetAsync($"{_bucketName}:{key}", "initial");

        // Capture the callback passed to inner watch
        Func<KvChangeEvent<string>, Task> capturedCallback = null!;
        await _innerStore.WatchAsync<string>(key, Arg.Do<Func<KvChangeEvent<string>, Task>>(cb => capturedCallback = cb), Arg.Any<CancellationToken>());

        // Act
        // Start watching (this triggers the inner call and captures the callback)
        await _sut.WatchAsync<string>(key, _ => Task.CompletedTask, CancellationToken.None);

        // Simulate a change event via the captured callback
        var changeEvent = new KvChangeEvent<string>(KvChangeType.Put, "new-val", key, 1);
        await capturedCallback(changeEvent);

        // Assert
        var cachedValue = await _fusionCache.GetOrDefaultAsync<string>($"{_bucketName}:{key}");
        cachedValue.ShouldBeNull();
    }
}

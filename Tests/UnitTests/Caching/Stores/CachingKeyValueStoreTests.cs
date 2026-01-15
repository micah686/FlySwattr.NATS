using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Caching.Configuration;
using FlySwattr.NATS.Caching.Stores;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
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

    /// <summary>
    /// Tests the "Stale-While-Revalidate" pattern with a NATS-specific exception (503 Service Unavailable).
    /// This simulates a scenario where:
    /// 1. A key is populated in the L1 cache
    /// 2. The TTL (Duration) expires, triggering a factory call to refresh from L2 (NATS)
    /// 3. The NATS cluster (L2) is unreachable (throws NatsJSApiException 503)
    /// 4. The time is still within FailSafeMaxDuration
    /// Expected: The stale value from L1 should be returned, preventing cascading failures.
    /// </summary>
    [Test]
    public async Task GetAsync_ShouldServeStaleData_WhenL2ThrowsNatsJSApiException503()
    {
        // Arrange
        var key = "l2-outage-key";
        var staleValue = "stale-data-for-resilience";
        
        // Populate cache with fail-safe enabled, simulating an initial successful fetch from L2
        await _fusionCache.SetAsync($"{_bucketName}:{key}", staleValue, new FusionCacheEntryOptions
        {
            Duration = TimeSpan.FromMilliseconds(100), // Short TTL - will expire quickly
            IsFailSafeEnabled = true,
            FailSafeMaxDuration = TimeSpan.FromHours(1) // Long fail-safe window
        });

        // Wait for TTL to expire (data is now "stale" but within FailSafeMaxDuration)
        await Task.Delay(150);

        // Simulate NATS cluster unavailability with 503 Service Unavailable
        var natsError = new ApiError { Code = 503, Description = "JetStream system temporarily unavailable" };
        var natsException = new NatsJSApiException(natsError);
        
        _innerStore.GetAsync<string>(key, Arg.Any<CancellationToken>())
            .ThrowsAsync(natsException);

        // Act - Attempt to get the value when L2 is down
        var result = await _sut.GetAsync<string>(key);

        // Assert
        // Should return stale value from L1 cache instead of throwing NatsJSApiException
        result.ShouldBe(staleValue);
        
        // Verify that the factory (inner store) was actually called (proving cache miss triggered refresh attempt)
        await _innerStore.Received(1).GetAsync<string>(key, Arg.Any<CancellationToken>());
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

    #region Distributed Invalidation Race Tests

    /// <summary>
    /// Tests that when a local PutAsync occurs and NATS echoes the event back to the watcher,
    /// no race condition or "cache thrashing" occurs.
    /// 
    /// Architectural Context:
    /// The implementation uses a Write-Invalidate strategy where PutAsync:
    /// 1. Writes to NATS
    /// 2. Invalidates local cache
    /// 
    /// NATS then echoes the event back to the watcher, which also triggers cache invalidation.
    /// This test verifies that this "double invalidation" doesn't cause issues and the
    /// subsequent read correctly fetches the fresh value.
    /// </summary>
    [Test]
    public async Task PutAsync_WithWatcherEcho_ShouldNotCauseCacheThrashing()
    {
        // Arrange
        var key = "self-invalidation-key";
        var initialValue = "initial-value";
        var newValue = "updated-value";
        
        // Capture the watcher callback so we can simulate the echo
        Func<KvChangeEvent<string>, Task>? capturedWatcherCallback = null;
        await _innerStore.WatchAsync<string>(
            key, 
            Arg.Do<Func<KvChangeEvent<string>, Task>>(cb => capturedWatcherCallback = cb), 
            Arg.Any<CancellationToken>());
        
        // Pre-populate cache and start watching
        await _fusionCache.SetAsync($"{_bucketName}:{key}", initialValue);
        await _sut.WatchAsync<string>(key, _ => Task.CompletedTask, CancellationToken.None);
        
        // Setup inner store to return fresh value on subsequent read
        _innerStore.GetAsync<string>(key, Arg.Any<CancellationToken>()).Returns(newValue);

        // Act - Step 1: Local node performs PutAsync
        await _sut.PutAsync(key, newValue);
        
        // Act - Step 2: Simulate NATS echoing the event back to the watcher
        // This happens asynchronously in real scenarios, but we simulate it immediately
        capturedWatcherCallback.ShouldNotBeNull();
        var echoEvent = new KvChangeEvent<string>(KvChangeType.Put, newValue, key, 2);
        await capturedWatcherCallback!(echoEvent);

        // Act - Step 3: Read the value after the echo
        var result = await _sut.GetAsync<string>(key);

        // Assert
        // The value should be the fresh value from NATS, not stale or null
        result.ShouldBe(newValue);
        
        // Verify inner store was called for the read (cache was invalidated)
        await _innerStore.Received().GetAsync<string>(key, Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// Tests the timing-specific race condition where the watcher echo arrives
    /// very quickly after PutAsync, potentially before the cache invalidation completes.
    /// 
    /// This tests the "eventual consistency" race window:
    /// - PutAsync writes to NATS and invalidates cache
    /// - Echo arrives almost immediately
    /// - Another read occurs during this window
    /// 
    /// The test verifies that regardless of timing, the cache remains consistent.
    /// </summary>
    [Test]
    public async Task PutAsync_WithImmediateWatcherEcho_CacheRemainsConsistent()
    {
        // Arrange
        var key = "race-timing-key";
        var existingValue = "existing";
        var updatedValue = "updated";
        
        // Capture watcher callback
        Func<KvChangeEvent<string>, Task>? capturedCallback = null;
        await _innerStore.WatchAsync<string>(
            key,
            Arg.Do<Func<KvChangeEvent<string>, Task>>(cb => capturedCallback = cb),
            Arg.Any<CancellationToken>());
        
        // Pre-populate cache and start watching
        await _fusionCache.SetAsync($"{_bucketName}:{key}", existingValue);
        await _sut.WatchAsync<string>(key, _ => Task.CompletedTask, CancellationToken.None);
        
        // Configure inner store behavior for reads
        _innerStore.GetAsync<string>(key, Arg.Any<CancellationToken>()).Returns(updatedValue);

        // Act - Perform Put and simulate immediate echo (race condition window)
        var putTask = _sut.PutAsync(key, updatedValue);
        
        // Simulate echo arriving during PlutAsync execution
        capturedCallback.ShouldNotBeNull();
        var echoTask = capturedCallback!(new KvChangeEvent<string>(KvChangeType.Put, updatedValue, key, 2));
        
        await Task.WhenAll(putTask, echoTask);

        // Act - Read after both operations complete
        var result = await _sut.GetAsync<string>(key);

        // Assert
        // Cache should still work correctly - returning the updated value
        result.ShouldBe(updatedValue);
        
        // Verify the read went to inner store (cache was properly invalidated)
        await _innerStore.Received().GetAsync<string>(key, Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// Tests that multiple rapid writes don't cause cascading invalidations
    /// that could leave the cache in an inconsistent state.
    /// 
    /// Scenario:
    /// - Write A triggers invalidation
    /// - Write B triggers invalidation  
    /// - Echo from A arrives
    /// - Echo from B arrives
    /// - Read should return value B, not A or stale data
    /// </summary>
    [Test]
    public async Task RapidWrites_WithDelayedEchoes_ShouldMaintainLastWriteWins()
    {
        // Arrange
        var key = "rapid-writes-key";
        var valueA = "value-A";
        var valueB = "value-B";
        var finalValue = "value-B"; // Last write wins
        
        // Capture watcher callback
        Func<KvChangeEvent<string>, Task>? capturedCallback = null;
        await _innerStore.WatchAsync<string>(
            key,
            Arg.Do<Func<KvChangeEvent<string>, Task>>(cb => capturedCallback = cb),
            Arg.Any<CancellationToken>());
        
        // Start watching
        await _sut.WatchAsync<string>(key, _ => Task.CompletedTask, CancellationToken.None);
        
        // Configure inner store to return the final value
        _innerStore.GetAsync<string>(key, Arg.Any<CancellationToken>()).Returns(finalValue);

        // Act - Rapid writes
        await _sut.PutAsync(key, valueA);
        await _sut.PutAsync(key, valueB);
        
        // Simulate delayed echoes arriving (out of order is also valid)
        capturedCallback.ShouldNotBeNull();
        await capturedCallback!(new KvChangeEvent<string>(KvChangeType.Put, valueA, key, 1));
        await capturedCallback!(new KvChangeEvent<string>(KvChangeType.Put, valueB, key, 2));

        // Act - Read after all echoes
        var result = await _sut.GetAsync<string>(key);

        // Assert
        // Should return the latest value from NATS (last-write-wins)
        result.ShouldBe(finalValue);
    }

    /// <summary>
    /// Tests the delete-put race condition where a delete is quickly followed by a put.
    /// The watcher must handle the echo sequence correctly so the final read returns
    /// the put value, not null (from delete).
    /// </summary>
    [Test]
    public async Task DeleteFollowedByPut_WithEchoes_ShouldReturnPutValue()
    {
        // Arrange
        var key = "delete-put-race-key";
        var initialValue = "exists";
        var newValue = "recreated";
        
        // Capture watcher callback
        Func<KvChangeEvent<string>, Task>? capturedCallback = null;
        await _innerStore.WatchAsync<string>(
            key,
            Arg.Do<Func<KvChangeEvent<string>, Task>>(cb => capturedCallback = cb),
            Arg.Any<CancellationToken>());
        
        // Pre-populate and start watching
        await _fusionCache.SetAsync($"{_bucketName}:{key}", initialValue);
        await _sut.WatchAsync<string>(key, _ => Task.CompletedTask, CancellationToken.None);
        
        // Configure inner store: after delete+put sequence, returns the new value
        _innerStore.GetAsync<string>(key, Arg.Any<CancellationToken>()).Returns(newValue);

        // Act - Delete then Put in quick succession
        await _sut.DeleteAsync(key);
        await _sut.PutAsync(key, newValue);
        
        // Simulate echoes arriving
        capturedCallback.ShouldNotBeNull();
        await capturedCallback!(new KvChangeEvent<string>(KvChangeType.Delete, default, key, 1));
        await capturedCallback!(new KvChangeEvent<string>(KvChangeType.Put, newValue, key, 2));

        // Act - Read after echoes
        var result = await _sut.GetAsync<string>(key);

        // Assert
        // Should return the recreated value, not null
        result.ShouldBe(newValue);
    }

    #endregion

    /// <summary>
    /// Tests the Soft Timeout Responsiveness behavior for FusionCache.
    /// 
    /// Scenario: When the factory (NATS fetch) is slow, FusionCache should return
    /// stale data quickly based on FactorySoftTimeout rather than blocking for the
    /// full factory duration.
    /// 
    /// This tests system responsiveness under load degradation (latency spikes):
    /// - Configure FactorySoftTimeout to 50ms
    /// - Mock inner store with 500ms delay
    /// - Cache has stale value (TTL expired, within FailSafeMaxDuration)
    /// - GetAsync should return in ~50ms with stale value, not block for 500ms
    /// </summary>
    [Test]
    public async Task GetAsync_ShouldReturnStaleValue_WhenFactoryExceedsSoftTimeout()
    {
        // Arrange
        var key = "soft-timeout-key";
        var staleValue = "stale-value-from-cache";
        var freshValue = "fresh-value-from-factory";

        // Create a new CachingKeyValueStore with soft timeout configured
        var softTimeoutConfig = new FusionCacheConfiguration
        {
            MemoryCacheDuration = TimeSpan.FromMilliseconds(100), // Short TTL
            FailSafeMaxDuration = TimeSpan.FromMinutes(5),
            FactorySoftTimeout = TimeSpan.FromMilliseconds(50), // 50ms soft timeout
            FactoryHardTimeout = TimeSpan.FromSeconds(10) // Long hard timeout
        };

        // Need fresh FusionCache with specific options
        using var fusionCache = new FusionCache(new FusionCacheOptions());
        var sut = new CachingKeyValueStore(_innerStore, fusionCache, softTimeoutConfig, _bucketName, _logger);

        // Populate cache with stale value (simulating a previous successful fetch)
        await fusionCache.SetAsync($"{_bucketName}:{key}", staleValue, new FusionCacheEntryOptions
        {
            Duration = TimeSpan.FromMilliseconds(50), // Short TTL
            IsFailSafeEnabled = true,
            FailSafeMaxDuration = TimeSpan.FromMinutes(5)
        });

        // Wait for TTL to expire (data is now "stale" but within FailSafeMaxDuration)
        await Task.Delay(100);

        // Mock inner store to delay for 500ms (much longer than soft timeout)
        _innerStore.GetAsync<string>(key, Arg.Any<CancellationToken>())
            .Returns(async callInfo =>
            {
                await Task.Delay(500);
                return (string?)freshValue;
            });

        // Act - measure actual response time
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var result = await sut.GetAsync<string>(key);
        stopwatch.Stop();

        // Assert
        // Should return stale value quickly (around soft timeout of 50ms)
        result.ShouldBe(staleValue);

        // Response should be fast (soft timeout + buffer), not the full 500ms factory delay
        // Allow generous buffer for test execution overhead (up to 200ms total)
        stopwatch.ElapsedMilliseconds.ShouldBeLessThan(200, 
            $"Expected response in ~50ms (soft timeout), but took {stopwatch.ElapsedMilliseconds}ms. " +
            "This suggests soft timeout is not working - system blocked for full factory duration.");
        
        // Verify the factory was actually called (proving cache miss triggered refresh attempt)
        await _innerStore.Received(1).GetAsync<string>(key, Arg.Any<CancellationToken>());
    }
}

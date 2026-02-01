using FlySwattr.NATS.Caching.Configuration;
using FlySwattr.NATS.Caching.Stores;
using FlySwattr.NATS.Core.Stores;
using IntegrationTests.Infrastructure;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.KeyValueStore;
using NATS.Client.Serializers.Json;
using Shouldly;
using TUnit.Core;
using ZiggyCreatures.Caching.Fusion;

namespace IntegrationTests.Caching;

[Property("nTag", "Caching")]
public class NatsKvStoreBasicCrudTests
{
    public record ConfigData(string Setting, int Value);

    [Test]
    public async Task Put_And_Get_Returns_Correct_Value_With_Cache()
    {
        // Arrange
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts { Url = fixture.ConnectionString, SerializerRegistry = NatsJsonSerializerRegistry.Default };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();

        var js = new NatsJSContext(conn);
        var kv = new NatsKVContext(js);

        var bucket = "APP_CONFIG_CACHE";
        await kv.CreateStoreAsync(new NatsKVConfig(bucket));

        // Create Core Store
        await using var coreStore = new NatsKeyValueStore(kv, bucket, NullLogger<NatsKeyValueStore>.Instance);

        // Create Caching Wrapper
        var fusionCache = new FusionCache(new FusionCacheOptions());
        var cacheConfig = new FusionCacheConfiguration();
        var cachingStore = new CachingKeyValueStore(coreStore, fusionCache, cacheConfig, bucket, NullLogger<CachingKeyValueStore>.Instance);

        // Act
        await cachingStore.PutAsync("theme", new ConfigData("dark", 1));

        // Assert
        var result = await cachingStore.GetAsync<ConfigData>("theme");
        result.ShouldNotBeNull();
        result.Setting.ShouldBe("dark");
        result.Value.ShouldBe(1);
    }

    [Test]
    public async Task Cache_Should_Return_Stale_If_Nats_Unavailable()
    {
        // Arrange
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts { Url = fixture.ConnectionString, SerializerRegistry = NatsJsonSerializerRegistry.Default };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();

        var js = new NatsJSContext(conn);
        var kv = new NatsKVContext(js);

        var bucket = "FAILSAFE_TEST";
        await kv.CreateStoreAsync(new NatsKVConfig(bucket));

        await using var coreStore = new NatsKeyValueStore(kv, bucket, NullLogger<NatsKeyValueStore>.Instance);

        var fusionCache = new FusionCache(new FusionCacheOptions());
        var cacheConfig = new FusionCacheConfiguration 
        { 
            // Enable fail-safe
            FailSafeMaxDuration = TimeSpan.FromMinutes(1),
            FactorySoftTimeout = TimeSpan.FromMilliseconds(100) // Fast fail to stale
        };
        var cachingStore = new CachingKeyValueStore(coreStore, fusionCache, cacheConfig, bucket, NullLogger<CachingKeyValueStore>.Instance);

        // Put initial value (populates cache)
        await cachingStore.PutAsync("config", "initial");
        
        // Ensure it's in cache
        var val = await cachingStore.GetAsync<string>("config");
        val.ShouldBe("initial");

        // Act: Stop NATS
        await fixture.Container.StopAsync();

        // Get again - should come from cache (fail-safe) even though NATS is down
        // Note: CachingKeyValueStore GetAsync calls GetOrSetAsync.
        // If NATS is down, the factory (coreStore.GetAsync) will throw/timeout.
        // FusionCache should return the stale value.
        var cachedVal = await cachingStore.GetAsync<string>("config");

        // Assert
        cachedVal.ShouldBe("initial");
    }

    [Test]
    public async Task Put_Invalidates_Cache()
    {
        // Arrange
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts { Url = fixture.ConnectionString, SerializerRegistry = NatsJsonSerializerRegistry.Default };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();

        var js = new NatsJSContext(conn);
        var kv = new NatsKVContext(js);

        var bucket = "INVALIDATION_TEST";
        await kv.CreateStoreAsync(new NatsKVConfig(bucket));

        await using var coreStore = new NatsKeyValueStore(kv, bucket, NullLogger<NatsKeyValueStore>.Instance);
        var fusionCache = new FusionCache(new FusionCacheOptions());
        var cacheConfig = new FusionCacheConfiguration();
        var cachingStore = new CachingKeyValueStore(coreStore, fusionCache, cacheConfig, bucket, NullLogger<CachingKeyValueStore>.Instance);

        // Act
        await cachingStore.PutAsync("key", "v1");
        var v1 = await cachingStore.GetAsync<string>("key");
        v1.ShouldBe("v1");

        // Update via Wrapper (should invalidate cache)
        await cachingStore.PutAsync("key", "v2");
        
        // Get again
        var v2 = await cachingStore.GetAsync<string>("key");

        // Assert
        v2.ShouldBe("v2");
    }

    [Test]
    public async Task Watcher_Invalidates_Cache_On_External_Change()
    {
        // Arrange
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts { Url = fixture.ConnectionString, SerializerRegistry = NatsJsonSerializerRegistry.Default };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();

        var js = new NatsJSContext(conn);
        var kv = new NatsKVContext(js);

        var bucket = "WATCHER_CACHE_TEST";
        await kv.CreateStoreAsync(new NatsKVConfig(bucket));

        // Client 1: Caching Store
        await using var coreStore1 = new NatsKeyValueStore(kv, bucket, NullLogger<NatsKeyValueStore>.Instance);
        var fusionCache = new FusionCache(new FusionCacheOptions());
        var cacheConfig = new FusionCacheConfiguration();
        var cachingStore = new CachingKeyValueStore(coreStore1, fusionCache, cacheConfig, bucket, NullLogger<CachingKeyValueStore>.Instance);

        // Client 2: External Core Store
        await using var coreStore2 = new NatsKeyValueStore(kv, bucket, NullLogger<NatsKeyValueStore>.Instance);

        // Setup Watcher on Client 1
        var tcs = new TaskCompletionSource<bool>();
        var cts = new CancellationTokenSource();
        
        // We need to watch to trigger invalidation logic
        var watchTask = cachingStore.WatchAsync<string>("key", async _ => 
        {
            tcs.TrySetResult(true);
            await Task.CompletedTask;
        }, cts.Token);

        // Populate initial
        await coreStore2.PutAsync("key", "v1");
        
        // Wait for watcher to see v1 (and invalidation to happen, though cache is empty anyway)
        await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        
        // Get via Caching Store -> Populates Cache with "v1"
        var val1 = await cachingStore.GetAsync<string>("key");
        val1.ShouldBe("v1");

        // Reset TCS for next event
        tcs = new TaskCompletionSource<bool>();

        // Act: Update via External Client 2
        await coreStore2.PutAsync("key", "v2");

        // Wait for watcher to receive event (which triggers invalidation)
        await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        
        // Give a tiny bit of time for async cache removal to complete (Fire-and-forget in WatchAsync wrapper?)
        // In CachingKeyValueStore: await _cache.RemoveAsync(...) inside the handler.
        // The handler provided by test is awaited. The wrapper awaits cache removal BEFORE calling handler.
        // The CachingKeyValueStore wrapper ensures cache invalidation occurs before the handler is invoked.
        // So when tcs is set in handler, cache is already removed.

        // Get via Caching Store -> Should miss cache and fetch "v2"
        var val2 = await cachingStore.GetAsync<string>("key");

        // Assert
        val2.ShouldBe("v2");

        cts.Cancel();
        try { await watchTask; } catch (OperationCanceledException) { }
    }
}

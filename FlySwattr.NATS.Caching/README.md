# FlySwattr.NATS.Caching

**High-performance caching for NATS Key-Value Stores.**

NATS KV is fast, but reading over the network for every configuration check is inefficient. This library integrates `FusionCache` to provide a robust L1+L2 caching strategy.

## 🚀 How It Works

1.  **L1 (Memory):** Reads are served instantly from local memory.
2.  **L2 (NATS KV):** On cache miss, data is fetched from NATS.
3.  **Proactive Invalidation:** When you `PutAsync` or `DeleteAsync`, the local cache is immediately invalidated.
4.  **Distributed Invalidation:** (Optional) Can watch the bucket for changes from *other* services to invalidate the local cache.

## 🛡️ Resilience Features

*   **Fail-Safe:** If NATS is down, the cache can serve "stale" data from memory instead of crashing your app.
*   **Factory Soft Timeouts:** If NATS is slow, return stale data quickly while refreshing in the background.
*   **Stampede Protection:** Prevents thousands of concurrent requests from hitting NATS simultaneously for the same key.

## 📦 Usage

```csharp
services.AddFlySwattrNatsCaching(options =>
{
    options.MemoryCacheDuration = TimeSpan.FromMinutes(5);
    options.FailSafeMaxDuration = TimeSpan.FromHours(1); // Survive 1h NATS outage
});
```

Once registered, any `IKeyValueStore` injected into your services is automatically decorated with caching.

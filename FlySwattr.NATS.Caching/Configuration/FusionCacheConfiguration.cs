namespace FlySwattr.NATS.Caching.Configuration;

/// <summary>
/// Configuration for FusionCache value-level caching in KV stores.
/// </summary>
public class FusionCacheConfiguration
{
    /// <summary>Cache duration for KV values in memory (L1).</summary>
    public TimeSpan MemoryCacheDuration { get; set; } = TimeSpan.FromMinutes(5);
    
    /// <summary>Fail-safe duration - how long stale values can be served if NATS is down.</summary>
    public TimeSpan FailSafeMaxDuration { get; set; } = TimeSpan.FromHours(1);
    
    /// <summary>Soft timeout - if factory doesn't return within this time, serve stale data.</summary>
    public TimeSpan FactorySoftTimeout { get; set; } = TimeSpan.FromSeconds(1);
    
    /// <summary>Hard timeout - max time to wait for factory before throwing.</summary>
    public TimeSpan FactoryHardTimeout { get; set; } = TimeSpan.FromSeconds(10);
    
    /// <summary>Cache duration for not-found keys (shorter to allow quick refresh when key is created).</summary>
    public TimeSpan NotFoundCacheDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Maximum number of cache size units allowed in the underlying in-memory cache.
    /// Entries use <see cref="EntrySizeUnits"/> as their size.
    /// </summary>
    public long MemorySizeLimit { get; set; } = 10_000;

    /// <summary>
    /// Size units charged to each cache entry.
    /// Defaults to 1, making <see cref="MemorySizeLimit"/> behave like a bounded entry count.
    /// </summary>
    public long EntrySizeUnits { get; set; } = 1;

    /// <summary>
    /// Percentage of the cache compacted when the memory cache is under pressure.
    /// </summary>
    public double CompactionPercentage { get; set; } = 0.2d;
}

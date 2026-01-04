// ReSharper disable CheckNamespace
// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Abstractions;

public interface IKeyValueStore
{
    Task PutAsync<T>(string key, T value, CancellationToken cancellationToken = default);
    Task<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default);
    Task DeleteAsync(string key, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Watches a key for changes, invoking the handler on each change event.
    /// The handler receives a <see cref="KvChangeEvent{T}"/> which explicitly indicates
    /// whether the change is a Put or Delete operation.
    /// </summary>
    /// <typeparam name="T">The type of value being watched.</typeparam>
    /// <param name="key">The key to watch.</param>
    /// <param name="handler">Handler invoked on each change event.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task WatchAsync<T>(string key, Func<KvChangeEvent<T>, Task> handler, CancellationToken cancellationToken = default);
}

public interface IObjectStore
{
    Task<string> PutAsync(string key, Stream data, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Downloads an object directly into the provided target stream.
    /// Preferred for large objects to avoid memory pressure.
    /// </summary>
    Task GetAsync(string key, Stream target, CancellationToken cancellationToken = default);
    
    Task DeleteAsync(string key, CancellationToken cancellationToken = default);
    
    Task<ObjectInfo?> GetInfoAsync(string key, bool showDeleted = false, CancellationToken cancellationToken = default);
    
    Task UpdateMetaAsync(string key, ObjectMetaInfo meta, CancellationToken cancellationToken = default);
    
    Task<IEnumerable<ObjectInfo>> ListAsync(bool showDeleted = false, CancellationToken cancellationToken = default);
    
    Task WatchAsync(Func<ObjectInfo, Task> handler, CancellationToken cancellationToken = default);
}

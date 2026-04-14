using System.Collections.Concurrent;
using FlySwattr.NATS.Abstractions;

namespace FlySwattr.NATS.Core.Stores;

internal sealed class ObjectStoreRegistry : IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, IObjectStore> _stores = new(StringComparer.Ordinal);
    private readonly Func<string, IObjectStore> _factory;

    public ObjectStoreRegistry(Func<string, IObjectStore> factory)
    {
        _factory = factory;
    }

    public IObjectStore GetOrCreate(string bucket)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(bucket);
        return _stores.GetOrAdd(bucket, _factory);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var store in _stores.Values)
        {
            await store.DisposeAsync().ConfigureAwait(false);
        }

        _stores.Clear();
        GC.SuppressFinalize(this);
    }
}

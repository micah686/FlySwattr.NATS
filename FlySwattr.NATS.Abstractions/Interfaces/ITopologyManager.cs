// ReSharper disable CheckNamespace
// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Abstractions;

public interface ITopologyManager
{
    Task EnsureStreamAsync(StreamSpec spec, CancellationToken cancellationToken = default);
    Task EnsureConsumerAsync(ConsumerSpec spec, CancellationToken cancellationToken = default);
    Task EnsureBucketAsync(BucketName name, StorageType storageType, CancellationToken cancellationToken = default);
    Task EnsureObjectStoreAsync(BucketName name, StorageType storageType, CancellationToken cancellationToken = default);
}

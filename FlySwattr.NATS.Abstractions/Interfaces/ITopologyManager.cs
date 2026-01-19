// ReSharper disable CheckNamespace
// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Abstractions;

/// <summary>
/// Manages NATS JetStream topology (streams, consumers, buckets, and object stores).
/// </summary>
public interface ITopologyManager
{
    /// <summary>
    /// Ensures a stream exists with the specified configuration.
    /// Creates the stream if it doesn't exist, or updates it if configuration has changed.
    /// </summary>
    Task EnsureStreamAsync(StreamSpec spec, CancellationToken cancellationToken = default);

    /// <summary>
    /// Ensures a consumer exists with the specified configuration.
    /// Creates the consumer if it doesn't exist, or updates it if configuration has changed.
    /// Also auto-provisions DLQ infrastructure if the consumer has a DeadLetterPolicy.
    /// </summary>
    Task EnsureConsumerAsync(ConsumerSpec spec, CancellationToken cancellationToken = default);

    /// <summary>
    /// Ensures a KV bucket exists with the specified name and storage type.
    /// </summary>
    Task EnsureBucketAsync(BucketName name, StorageType storageType, CancellationToken cancellationToken = default);

    /// <summary>
    /// Ensures a KV bucket exists with the specified configuration.
    /// Creates the bucket if it doesn't exist.
    /// </summary>
    Task EnsureBucketAsync(BucketSpec spec, CancellationToken cancellationToken = default);

    /// <summary>
    /// Ensures an object store exists with the specified name and storage type.
    /// </summary>
    Task EnsureObjectStoreAsync(BucketName name, StorageType storageType, CancellationToken cancellationToken = default);

    /// <summary>
    /// Ensures an object store exists with the specified configuration.
    /// Creates the object store if it doesn't exist.
    /// </summary>
    Task EnsureObjectStoreAsync(ObjectStoreSpec spec, CancellationToken cancellationToken = default);
}

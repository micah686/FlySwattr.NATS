// ReSharper disable CheckNamespace
// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Abstractions;

/// <summary>
/// Defines a source of topology specifications (streams, consumers, buckets, and object stores) for declarative provisioning.
/// Implement this interface to define your application's NATS topology.
/// </summary>
/// <remarks>
/// <para>
/// The topology provisioning system automatically handles DLQ infrastructure:
/// </para>
/// <list type="bullet">
///   <item><description>DLQ streams are auto-derived from <see cref="ConsumerSpec.DeadLetterPolicy"/> definitions</description></item>
///   <item><description>The <c>fs-dlq-entries</c> KV bucket is auto-created when any consumer has a DLQ policy</description></item>
/// </list>
/// <para>
/// This means you don't need to manually define DLQ streams in <see cref="GetStreams"/> if you've
/// already specified them in your consumer's <see cref="DeadLetterPolicy.TargetStream"/>.
/// </para>
/// </remarks>
public interface ITopologySource
{
    /// <summary>
    /// Gets the stream specifications to provision.
    /// </summary>
    /// <remarks>
    /// DLQ streams referenced by <see cref="ConsumerSpec.DeadLetterPolicy"/> are automatically
    /// created and do not need to be included here.
    /// </remarks>
    IEnumerable<StreamSpec> GetStreams();

    /// <summary>
    /// Gets the consumer specifications to provision.
    /// </summary>
    IEnumerable<ConsumerSpec> GetConsumers();

    /// <summary>
    /// Gets the KV bucket specifications to provision.
    /// Default implementation returns an empty collection for backwards compatibility.
    /// </summary>
    /// <remarks>
    /// The <c>fs-dlq-entries</c> bucket is automatically created when any consumer has a
    /// <see cref="DeadLetterPolicy"/> and does not need to be included here.
    /// </remarks>
    IEnumerable<BucketSpec> GetBuckets() => Enumerable.Empty<BucketSpec>();

    /// <summary>
    /// Gets the Object Store specifications to provision.
    /// Default implementation returns an empty collection for backwards compatibility.
    /// </summary>
    /// <remarks>
    /// The payload offloading bucket is automatically created when payload offloading is enabled
    /// and does not need to be included here.
    /// </remarks>
    IEnumerable<ObjectStoreSpec> GetObjectStores() => Enumerable.Empty<ObjectStoreSpec>();
}

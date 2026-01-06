// ReSharper disable CheckNamespace
// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Abstractions;

/// <summary>
/// Defines a source of topology specifications (streams and consumers) for declarative provisioning.
/// Implement this interface to define your application's NATS topology.
/// </summary>
public interface ITopologySource
{
    /// <summary>
    /// Gets the stream specifications to provision.
    /// </summary>
    IEnumerable<StreamSpec> GetStreams();

    /// <summary>
    /// Gets the consumer specifications to provision.
    /// </summary>
    IEnumerable<ConsumerSpec> GetConsumers();
}

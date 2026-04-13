// ReSharper disable CheckNamespace
namespace FlySwattr.NATS.Abstractions;

/// <summary>
/// Marker service indicating that FlySwattr NATS core services have been registered.
/// </summary>
public sealed class NatsCoreServicesMarker;

/// <summary>
/// Marker service indicating that payload offloading decorators have been registered.
/// </summary>
public sealed class NatsPayloadOffloadingMarker;

/// <summary>
/// Marker service indicating that resilience decorators have been registered.
/// </summary>
public sealed class NatsResilienceMarker;

/// <summary>
/// Marker service indicating that KV caching decorators have been registered.
/// </summary>
public sealed class NatsCachingMarker;

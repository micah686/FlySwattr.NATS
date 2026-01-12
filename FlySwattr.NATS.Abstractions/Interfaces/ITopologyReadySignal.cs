namespace FlySwattr.NATS.Abstractions;

/// <summary>
/// Coordination primitive for startup orchestration.
/// Allows dependent services to block until topology provisioning is complete,
/// preventing race conditions where consumers attempt to subscribe before streams exist.
/// </summary>
public interface ITopologyReadySignal
{
    /// <summary>
    /// Waits for the topology to be ready. Returns immediately if already signaled.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token for the wait operation.</param>
    /// <returns>A task that completes when topology is ready.</returns>
    /// <exception cref="TopologyProvisioningException">Thrown if topology provisioning failed.</exception>
    Task WaitAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Signals that topology provisioning has completed successfully.
    /// Thread-safe; subsequent calls are ignored.
    /// </summary>
    void SignalReady();

    /// <summary>
    /// Signals that topology provisioning failed with an unrecoverable error.
    /// Waiting tasks will throw the provided exception.
    /// Thread-safe; subsequent calls are ignored.
    /// </summary>
    /// <param name="exception">The exception that caused the failure.</param>
    void SignalFailed(Exception exception);

    /// <summary>
    /// Gets whether the signal has been set (either ready or failed).
    /// </summary>
    bool IsSignaled { get; }
}

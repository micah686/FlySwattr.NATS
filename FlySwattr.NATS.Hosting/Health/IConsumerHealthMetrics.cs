namespace FlySwattr.NATS.Hosting.Health;

/// <summary>
/// Interface for tracking consumer health metrics to detect zombie consumers.
/// </summary>
public interface IConsumerHealthMetrics
{
    /// <summary>
    /// Records a heartbeat when a message is successfully processed.
    /// </summary>
    void RecordHeartbeat(string streamName, string consumerName);

    /// <summary>
    /// Records that the consume loop is still actively iterating.
    /// This should be called on each loop iteration, even if no messages are received.
    /// </summary>
    void RecordLoopIteration(string streamName, string consumerName);

    /// <summary>
    /// Registers a consumer when it starts up.
    /// </summary>
    void RegisterConsumer(string streamName, string consumerName);

    /// <summary>
    /// Unregisters a consumer when it shuts down.
    /// </summary>
    void UnregisterConsumer(string streamName, string consumerName);

    /// <summary>
    /// Gets the health state of all registered consumers.
    /// </summary>
    IReadOnlyDictionary<ConsumerKey, ConsumerHealthState> GetAllConsumerStates();
}

/// <summary>
/// Unique identifier for a consumer.
/// </summary>
public readonly record struct ConsumerKey(string StreamName, string ConsumerName)
{
    public override string ToString() => $"{StreamName}/{ConsumerName}";
}

/// <summary>
/// Health state information for a consumer.
/// </summary>
public record ConsumerHealthState
{
    /// <summary>
    /// The last time a message was successfully processed (ACK'd).
    /// </summary>
    public DateTimeOffset LastHeartbeat { get; init; }

    /// <summary>
    /// The last time the consume loop iterated (even if no messages were received).
    /// </summary>
    public DateTimeOffset LastLoopIteration { get; init; }

    /// <summary>
    /// When the consumer was registered/started.
    /// </summary>
    public DateTimeOffset StartedAt { get; init; }

    /// <summary>
    /// Whether the consumer is still registered (not shut down).
    /// </summary>
    public bool IsActive { get; init; }
}

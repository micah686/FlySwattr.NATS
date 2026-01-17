using System.Collections.Concurrent;

namespace FlySwattr.NATS.Hosting.Health;

/// <summary>
/// Thread-safe singleton service that maintains heartbeat timestamps for all registered consumers.
/// </summary>
internal class NatsConsumerHealthMetrics : IConsumerHealthMetrics
{
    private readonly ConcurrentDictionary<ConsumerKey, MutableConsumerState> _consumers = new();
    private readonly TimeProvider _timeProvider;

    public NatsConsumerHealthMetrics() : this(TimeProvider.System)
    {
    }

    public NatsConsumerHealthMetrics(TimeProvider timeProvider)
    {
        _timeProvider = timeProvider;
    }

    public void RegisterConsumer(string streamName, string consumerName)
    {
        var key = new ConsumerKey(streamName, consumerName);
        var now = _timeProvider.GetUtcNow();

        _consumers.AddOrUpdate(
            key,
            _ => new MutableConsumerState
            {
                LastHeartbeat = now,
                LastLoopIteration = now,
                StartedAt = now,
                IsActive = true
            },
            (_, existing) =>
            {
                existing.IsActive = true;
                existing.StartedAt = now;
                existing.LastLoopIteration = now;
                return existing;
            });
    }

    public void UnregisterConsumer(string streamName, string consumerName)
    {
        var key = new ConsumerKey(streamName, consumerName);
        if (_consumers.TryGetValue(key, out var state))
        {
            state.IsActive = false;
        }
    }

    public void RecordHeartbeat(string streamName, string consumerName)
    {
        var key = new ConsumerKey(streamName, consumerName);
        if (_consumers.TryGetValue(key, out var state))
        {
            var now = _timeProvider.GetUtcNow();
            state.LastHeartbeat = now;
            state.LastLoopIteration = now;
        }
    }

    public void RecordLoopIteration(string streamName, string consumerName)
    {
        var key = new ConsumerKey(streamName, consumerName);
        if (_consumers.TryGetValue(key, out var state))
        {
            state.LastLoopIteration = _timeProvider.GetUtcNow();
        }
    }

    public IReadOnlyDictionary<ConsumerKey, ConsumerHealthState> GetAllConsumerStates()
    {
        return _consumers.ToDictionary(
            kvp => kvp.Key,
            kvp => new ConsumerHealthState
            {
                LastHeartbeat = kvp.Value.LastHeartbeat,
                LastLoopIteration = kvp.Value.LastLoopIteration,
                StartedAt = kvp.Value.StartedAt,
                IsActive = kvp.Value.IsActive
            });
    }

    /// <summary>
    /// Internal mutable state to allow lock-free updates.
    /// </summary>
    private class MutableConsumerState
    {
        public DateTimeOffset LastHeartbeat { get; set; }
        public DateTimeOffset LastLoopIteration { get; set; }
        public DateTimeOffset StartedAt { get; set; }
        public bool IsActive { get; set; }
    }
}

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
        var now = _timeProvider.GetUtcNow().UtcTicks;

        _consumers.AddOrUpdate(
            key,
            _ => new MutableConsumerState(now),
            (_, existing) =>
            {
                existing.SetActive(true);
                existing.SetStartedAt(now);
                existing.SetLastLoopIteration(now);
                return existing;
            });
    }

    public void UnregisterConsumer(string streamName, string consumerName)
    {
        var key = new ConsumerKey(streamName, consumerName);
        if (_consumers.TryGetValue(key, out var state))
        {
            state.SetActive(false);
        }
    }

    public void RecordHeartbeat(string streamName, string consumerName)
    {
        var key = new ConsumerKey(streamName, consumerName);
        if (_consumers.TryGetValue(key, out var state))
        {
            var now = _timeProvider.GetUtcNow().UtcTicks;
            state.SetLastHeartbeat(now);
            state.SetLastLoopIteration(now);
        }
    }

    public void RecordLoopIteration(string streamName, string consumerName)
    {
        var key = new ConsumerKey(streamName, consumerName);
        if (_consumers.TryGetValue(key, out var state))
        {
            state.SetLastLoopIteration(_timeProvider.GetUtcNow().UtcTicks);
        }
    }

    public IReadOnlyDictionary<ConsumerKey, ConsumerHealthState> GetAllConsumerStates()
    {
        return _consumers.ToDictionary(
            kvp => kvp.Key,
            kvp => new ConsumerHealthState
            {
                LastHeartbeat = new DateTimeOffset(Interlocked.Read(ref kvp.Value.LastHeartbeatTicks), TimeSpan.Zero),
                LastLoopIteration = new DateTimeOffset(Interlocked.Read(ref kvp.Value.LastLoopIterationTicks), TimeSpan.Zero),
                StartedAt = new DateTimeOffset(Interlocked.Read(ref kvp.Value.StartedAtTicks), TimeSpan.Zero),
                IsActive = Volatile.Read(ref kvp.Value.IsActive) == 1
            });
    }

    /// <summary>
    /// Internal mutable state to allow lock-free updates.
    /// </summary>
    private sealed class MutableConsumerState
    {
        public long LastHeartbeatTicks;
        public long LastLoopIterationTicks;
        public long StartedAtTicks;
        public int IsActive;

        public MutableConsumerState(long nowUtcTicks)
        {
            LastHeartbeatTicks = nowUtcTicks;
            LastLoopIterationTicks = nowUtcTicks;
            StartedAtTicks = nowUtcTicks;
            IsActive = 1;
        }

        public void SetLastHeartbeat(long utcTicks) => Interlocked.Exchange(ref LastHeartbeatTicks, utcTicks);

        public void SetLastLoopIteration(long utcTicks) => Interlocked.Exchange(ref LastLoopIterationTicks, utcTicks);

        public void SetStartedAt(long utcTicks) => Interlocked.Exchange(ref StartedAtTicks, utcTicks);

        public void SetActive(bool isActive) => Volatile.Write(ref IsActive, isActive ? 1 : 0);
    }
}

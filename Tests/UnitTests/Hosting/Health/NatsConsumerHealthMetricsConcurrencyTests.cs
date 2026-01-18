using FlySwattr.NATS.Hosting.Health;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Hosting.Health;

[Property("nTag", "Hosting")]
[Property("nTag", "Concurrency")]
public class NatsConsumerHealthMetricsConcurrencyTests
{
    [Test]
    public async Task ConcurrentAccess_ShouldMaintainConsistency_AndNotThrow()
    {
        // Arrange
        var metrics = new NatsConsumerHealthMetrics();
        var stream = "test-stream";
        var consumer = "test-consumer";
        
        var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromSeconds(2)); // Shortened from 5s for unit test speed

        var tasks = new List<Task>();
        var exceptions = new List<Exception>();

        // 5 threads looping Register/Unregister
        for (int i = 0; i < 5; i++)
        {
            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    while (!cts.Token.IsCancellationRequested)
                    {
                        metrics.RegisterConsumer(stream, consumer);
                        await Task.Yield(); // Give other threads a chance
                        metrics.UnregisterConsumer(stream, consumer);
                        await Task.Yield();
                    }
                }
                catch (Exception ex)
                {
                    lock (exceptions) exceptions.Add(ex);
                }
            }));
        }

        // 5 threads looping RecordHeartbeat
        for (int i = 0; i < 5; i++)
        {
            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    while (!cts.Token.IsCancellationRequested)
                    {
                        metrics.RecordHeartbeat(stream, consumer);
                        await Task.Yield();
                    }
                }
                catch (Exception ex)
                {
                    lock (exceptions) exceptions.Add(ex);
                }
            }));
        }

        // Act
        await Task.WhenAll(tasks);

        // Assert
        if (exceptions.Any())
        {
            throw new AggregateException("Concurrency errors occurred", exceptions);
        }

        // Verify state is accessible and valid
        var states = metrics.GetAllConsumerStates();
        
        // It should either be Active or Inactive, but exist (since we never remove keys in the current impl)
        states.ShouldContainKey(new ConsumerKey(stream, consumer));
        var state = states[new ConsumerKey(stream, consumer)];
        
        // Basic sanity checks
        state.StartedAt.ShouldBeLessThanOrEqualTo(DateTimeOffset.UtcNow);
        state.LastHeartbeat.ShouldBeLessThanOrEqualTo(DateTimeOffset.UtcNow);
    }
}

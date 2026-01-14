using FlySwattr.NATS.Resilience.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Polly.RateLimiting;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Resilience;

/// <summary>
/// Tests for the two-tier bulkhead isolation system:
/// - Global Pool (BulkheadManager): Pool-wide concurrency limits
/// - Per-Consumer Semaphore (ConsumerSemaphoreManager): Individual consumer limits within a pool
/// </summary>
[Property("nTag", "Resilience")]
public class TwoTierBulkheadIsolationTests
{

    /// <summary>
    /// 3.1.1 Noisy Neighbor Isolation Test
    /// 
    /// Scenario: Two consumers A and B share the "default" pool (limit 100).
    /// - Consumer A: maxConcurrency=10
    /// - Consumer B: maxConcurrency=90
    /// - Flood Consumer A with 200 concurrent requests
    /// 
    /// Assertions:
    /// 1. Consumer A processes max 10 messages at once (semaphore enforced)
    /// 2. Consumer B can simultaneously process 50 messages without being blocked by A's backlog
    /// </summary>
    [Test]
    public async Task NoisyNeighborIsolation_ConsumerAShouldNotStarveConsumerB()
    {
        // Arrange
        var config = new BulkheadConfiguration
        {
            NamedPools = new Dictionary<string, int>
            {
                ["default"] = 100
            },
            QueueLimitMultiplier = 2
        };

        var options = Substitute.For<IOptions<BulkheadConfiguration>>();
        options.Value.Returns(config);

        var bulkheadLogger = Substitute.For<ILogger<BulkheadManager>>();
        var semaphoreLogger = Substitute.For<ILogger<ConsumerSemaphoreManager>>();
        var bulkheadManager = new BulkheadManager(options, bulkheadLogger);
        var semaphoreManager = new ConsumerSemaphoreManager(semaphoreLogger);

        try
        {
            const string consumerAKey = "stream/consumerA";
            const string consumerBKey = "stream/consumerB";
            const int consumerALimit = 10;
            const int consumerBLimit = 90;

            var semaphoreA = semaphoreManager.GetConsumerSemaphore(consumerAKey, consumerALimit);
            var semaphoreB = semaphoreManager.GetConsumerSemaphore(consumerBKey, consumerBLimit);

            var consumerAActiveCount = 0;
            var consumerAMaxConcurrency = 0;
            var consumerBActiveCount = 0;
            var consumerBMaxConcurrency = 0;
            var consumerABlocker = new TaskCompletionSource();
            var consumerBBlocker = new TaskCompletionSource();

            // Act: Flood Consumer A with 200 concurrent "messages" that block until released
            var consumerATasks = new List<Task>();
            for (int i = 0; i < 200; i++)
            {
                consumerATasks.Add(Task.Run(async () =>
                {
                    await semaphoreA.WaitAsync();
                    try
                    {
                        var current = Interlocked.Increment(ref consumerAActiveCount);
                        lock (consumerATasks)
                        {
                            if (current > consumerAMaxConcurrency)
                                consumerAMaxConcurrency = current;
                        }
                        await consumerABlocker.Task;
                    }
                    finally
                    {
                        Interlocked.Decrement(ref consumerAActiveCount);
                        semaphoreA.Release();
                    }
                }));
            }

            // Give Consumer A tasks time to acquire semaphores
            await Task.Delay(100);

            // Assert: Consumer A should be capped at 10 concurrent
            consumerAActiveCount.ShouldBe(consumerALimit, 
                "Consumer A should be limited to its maxConcurrency");
            consumerAMaxConcurrency.ShouldBeLessThanOrEqualTo(consumerALimit,
                "Consumer A max observed concurrency should not exceed limit");

            // Act: Consumer B should be able to process 50 messages without being blocked
            var consumerBTasks = new List<Task>();
            for (int i = 0; i < 50; i++)
            {
                consumerBTasks.Add(Task.Run(async () =>
                {
                    await semaphoreB.WaitAsync();
                    try
                    {
                        var current = Interlocked.Increment(ref consumerBActiveCount);
                        lock (consumerBTasks)
                        {
                            if (current > consumerBMaxConcurrency)
                                consumerBMaxConcurrency = current;
                        }
                        await consumerBBlocker.Task;
                    }
                    finally
                    {
                        Interlocked.Decrement(ref consumerBActiveCount);
                        semaphoreB.Release();
                    }
                }));
            }

            // Give Consumer B tasks time to acquire semaphores
            await Task.Delay(100);

            // Assert: Consumer B should have 50 concurrent (not starved by A)
            consumerBActiveCount.ShouldBe(50, 
                "Consumer B should not be starved by Consumer A's backlog");

            // Release all blockers
            consumerABlocker.SetResult();
            consumerBBlocker.SetResult();

            await Task.WhenAll(consumerATasks);
            await Task.WhenAll(consumerBTasks);
        }
        finally
        {
            await semaphoreManager.DisposeAsync();
            await bulkheadManager.DisposeAsync();
        }
    }

    /// <summary>
    /// 3.1.2 Global Pool Saturation Test
    /// 
    /// Scenario: Flood the "default" pool with messages across 10 consumers, exceeding the global limit of 100.
    /// 
    /// Assertions:
    /// 1. BulkheadManager begins queuing execution attempts when permits exhausted
    /// 2. Once queue is full (limit * multiplier = 200), subsequent calls throw RateLimiterRejectedException
    /// </summary>
    [Test]
    public async Task GlobalPoolSaturation_ShouldRejectWhenQueueFull()
    {
        // Arrange
        const int permitLimit = 100;
        const int queueMultiplier = 2;
        const int queueLimit = permitLimit * queueMultiplier; // 200
        const int totalCapacity = permitLimit + queueLimit; // 300

        var config = new BulkheadConfiguration
        {
            NamedPools = new Dictionary<string, int>
            {
                ["default"] = permitLimit
            },
            QueueLimitMultiplier = queueMultiplier
        };

        var options = Substitute.For<IOptions<BulkheadConfiguration>>();
        options.Value.Returns(config);

        var bulkheadLogger = Substitute.For<ILogger<BulkheadManager>>();
        var bulkheadManager = new BulkheadManager(options, bulkheadLogger);

        try
        {
            var pipeline = bulkheadManager.GetPoolPipeline("default");
            var blocker = new TaskCompletionSource();
            var activeTasks = new List<Task>();
            var activeCount = 0;
            var maxConcurrency = 0;

            // Act: Saturate the pool (100 active + 200 queued = 300 total capacity)
            for (int i = 0; i < totalCapacity; i++)
            {
                var consumerIndex = i % 10; // Simulate 10 different consumers
                activeTasks.Add(pipeline.ExecuteAsync(async ct =>
                {
                    var current = Interlocked.Increment(ref activeCount);
                    lock (activeTasks)
                    {
                        if (current > maxConcurrency)
                            maxConcurrency = current;
                    }
                    await blocker.Task;
                    Interlocked.Decrement(ref activeCount);
                }).AsTask());
            }

            // Give time for all tasks to either execute or queue
            await Task.Delay(200);

            // Assert: Active count should be at the permit limit (not queue limit)
            activeCount.ShouldBe(permitLimit, 
                "Active executions should be limited to permit limit, rest should queue");
            maxConcurrency.ShouldBeLessThanOrEqualTo(permitLimit,
                "Max observed concurrency should not exceed permit limit");

            // Act & Assert: The next call should be rejected (queue is full)
            await Assert.ThrowsAsync<RateLimiterRejectedException>(async () =>
            {
                await pipeline.ExecuteAsync(async ct =>
                {
                    await Task.Delay(100, ct);
                });
            });

            // Cleanup
            blocker.SetResult();
            await Task.WhenAll(activeTasks);
        }
        finally
        {
            await bulkheadManager.DisposeAsync();
        }
    }
}

using FlySwattr.NATS.Resilience.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Polly;
using Polly.RateLimiting;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Resilience.Configuration;

[Property("nTag", "Resilience")]
public class BulkheadManagerTests : IAsyncDisposable
{
    private readonly BulkheadManager _manager;
    private readonly BulkheadConfiguration _config;
    private readonly ILogger<BulkheadManager> _logger;

    public BulkheadManagerTests()
    {
        _config = new BulkheadConfiguration
        {
            NamedPools = new Dictionary<string, int>
            {
                ["default"] = 10,
                ["critical"] = 5
            },
            QueueLimitMultiplier = 1
        };

        var options = Substitute.For<IOptions<BulkheadConfiguration>>();
        options.Value.Returns(_config);

        _logger = Substitute.For<ILogger<BulkheadManager>>();

        _manager = new BulkheadManager(options, _logger);
    }

    [Test]
    public async Task Bulkhead_CriticalConsumer_ShouldUseIsolatedPool()
    {
        // Act
        var pipeline = _manager.GetPoolPipeline("critical");

        // Assert
        // We can verify it respects the limit by checking behavior
        var limit = _manager.GetPoolLimit("critical");
        limit.ShouldBe(5);

        // Verify we can execute up to the limit
        var tasks = new List<Task>();
        var tcs = new TaskCompletionSource();
        
        for (int i = 0; i < 5; i++)
        {
            tasks.Add(pipeline.ExecuteAsync(async ct => await tcs.Task).AsTask());
        }

        // The 6th should queue or be rejected (depending on queue size)
        // Queue limit is 5 * 1 = 5.
        // So 6th enters queue.
        var queuedTask = pipeline.ExecuteAsync(async ct => await tcs.Task);
        
        // We can't easily peek into the pipeline, but we can verify it's a different instance from default
        var defaultPipeline = _manager.GetPoolPipeline("default");
        pipeline.ShouldNotBe(defaultPipeline);
        
        tcs.SetResult();
        await Task.WhenAll(tasks);
        await queuedTask;
    }

    [Test]
    public void Bulkhead_StandardConsumer_ShouldShareDefaultPool()
    {
        // Act
        var pipelineA = _manager.GetPoolPipeline("default");
        var pipelineB = _manager.GetPoolPipeline("default");
        
        // Assert
        pipelineA.ShouldBe(pipelineB);
    }

    [Test]
    public async Task Bulkhead_IsolatedPoolExhaustion_ShouldNotAffectOtherPools()
    {
        // Arrange
        var criticalPipeline = _manager.GetPoolPipeline("critical"); // Limit 5, Queue 5
        var defaultPipeline = _manager.GetPoolPipeline("default");   // Limit 10, Queue 10

        var tcs = new TaskCompletionSource();

        // Exhaust "critical" pool (5 active + 5 queued = 10 total capacity)
        var criticalTasks = new List<Task>();
        for (int i = 0; i < 10; i++)
        {
            criticalTasks.Add(criticalPipeline.ExecuteAsync(async ct => await tcs.Task).AsTask());
        }

        // Verify critical pool is full (next one should fail)
        await Assert.ThrowsAsync<RateLimiterRejectedException>(async () => 
            await criticalPipeline.ExecuteAsync(async ct => await tcs.Task));

        // Act & Assert
        // Default pool should still be available
        try 
        {
            await defaultPipeline.ExecuteAsync(async ct => await Task.CompletedTask);
        }
        catch (Exception ex)
        {
            Assert.Fail($"Default pool should not be affected: {ex.Message}");
        }

        // Cleanup
        tcs.SetResult();
        await Task.WhenAll(criticalTasks);
    }

    public async ValueTask DisposeAsync()
    {
        await _manager.DisposeAsync();
    }
}

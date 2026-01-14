using FlySwattr.NATS.Resilience.Builders;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Polly;
using Polly.CircuitBreaker;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Resilience.Builders;

[Property("nTag", "Resilience")]
public class HierarchicalResilienceBuilderTests
{
    private readonly NullLogger<HierarchicalResilienceBuilder> _logger = NullLogger<HierarchicalResilienceBuilder>.Instance;

    [Test]
    public async Task CircuitBreaker_ShouldOpen_AfterFailureThresholdReached()
    {
        // Arrange
        await using var builder = new HierarchicalResilienceBuilder(_logger);
        var globalPipeline = new ResiliencePipelineBuilder().Build();
        
        var options = new ConsumerCircuitBreakerOptions
        {
            FailureRatio = 0.5, // 50%
            MinimumThroughput = 2,
            SamplingDuration = TimeSpan.FromSeconds(2),
            BreakDuration = TimeSpan.FromSeconds(1)
        };
        
        var pipeline = builder.GetPipeline("test-consumer", globalPipeline, options);

        // Act & Assert
        // 1. Success
        await pipeline.ExecuteAsync(_ => ValueTask.CompletedTask);
        
        // 2. Fail
        try { await pipeline.ExecuteAsync<int>(_ => throw new Exception("fail")); } catch {}

        // 3. Fail (Now 2/3 = 66% failure, > 50%, MinThroughput met)
        try { await pipeline.ExecuteAsync<int>(_ => throw new Exception("fail")); } catch {}
        
        // Should be open now
        await Assert.ThrowsAsync<BrokenCircuitException>(async () => await pipeline.ExecuteAsync(_ => ValueTask.CompletedTask));
    }

    [Test]
    public async Task CircuitBreaker_ShouldTransitionToHalfOpen_AfterBreakDuration()
    {
        // Arrange
        await using var builder = new HierarchicalResilienceBuilder(_logger);
        var globalPipeline = new ResiliencePipelineBuilder().Build();
        
        var options = new ConsumerCircuitBreakerOptions
        {
            FailureRatio = 0.1, // Trip easily
            MinimumThroughput = 2,
            SamplingDuration = TimeSpan.FromSeconds(2),
            BreakDuration = TimeSpan.FromMilliseconds(500) // Short break
        };
        
        var pipeline = builder.GetPipeline("test-consumer-halfopen", globalPipeline, options);

        // Trip it
        try { await pipeline.ExecuteAsync<int>(_ => throw new Exception("fail")); } catch {}
        try { await pipeline.ExecuteAsync<int>(_ => throw new Exception("fail")); } catch {}
        
        // Verify Open
        await Assert.ThrowsAsync<BrokenCircuitException>(async () => await pipeline.ExecuteAsync(_ => ValueTask.CompletedTask));

        // Wait for BreakDuration
        await Task.Delay(600);
        
        // Next call should be allowed (Half-Open state allows a trial call)
        // If this succeeds, it closes. If it throws BrokenCircuitException, it's still open.
        // If it throws the actual exception, it was allowed through.
        
        bool executed = false;
        await pipeline.ExecuteAsync(_ => 
        {
            executed = true;
            return ValueTask.CompletedTask;
        });
        
        executed.ShouldBeTrue();
    }

    [Test]
    public async Task CircuitBreaker_ConsumerIsolation_ShouldNotAffectOtherConsumers()
    {
        // Arrange
        await using var builder = new HierarchicalResilienceBuilder(_logger);
        var globalPipeline = new ResiliencePipelineBuilder().Build();
        
        var options = new ConsumerCircuitBreakerOptions
        {
            FailureRatio = 0.1, 
            MinimumThroughput = 2,
            BreakDuration = TimeSpan.FromSeconds(10) 
        };
        
        var pipelineA = builder.GetPipeline("consumer-A", globalPipeline, options);
        var pipelineB = builder.GetPipeline("consumer-B", globalPipeline, options);

        // Trip A
        try { await pipelineA.ExecuteAsync<int>(_ => throw new Exception("fail")); } catch {}
        try { await pipelineA.ExecuteAsync<int>(_ => throw new Exception("fail")); } catch {}
        await Assert.ThrowsAsync<BrokenCircuitException>(async () => await pipelineA.ExecuteAsync(_ => ValueTask.CompletedTask));

        // B should still work
        await pipelineB.ExecuteAsync(_ => ValueTask.CompletedTask);
        await pipelineB.ExecuteAsync(_ => ValueTask.CompletedTask);
    }

    [Test]
    public async Task CircuitBreaker_SameKey_ShouldReturnSamePipeline()
    {
        // Arrange
        await using var builder = new HierarchicalResilienceBuilder(_logger);
        var globalPipeline = new ResiliencePipelineBuilder().Build();
        
        var pipeline1 = builder.GetPipeline("shared_key", globalPipeline);
        var pipeline2 = builder.GetPipeline("shared_key", globalPipeline);
        
        pipeline1.ShouldBeSameAs(pipeline2);
    }

    [Test]
    public async Task CircuitBreaker_InvalidatePipeline_ShouldRemoveFromCache()
    {
        // Arrange
        await using var builder = new HierarchicalResilienceBuilder(_logger);
        var globalPipeline = new ResiliencePipelineBuilder().Build();
        var key = "stale-consumer";
        
        var pipeline = builder.GetPipeline(key, globalPipeline);
        
        // Manually invalidate
        builder.InvalidatePipeline(key);
        
        var pipelineNew = builder.GetPipeline(key, globalPipeline);
        
        pipelineNew.ShouldNotBeSameAs(pipeline);
    }

    /// <summary>
    /// Documents the "Resilience Amnesia" vulnerability: when a pipeline is evicted after
    /// 30 minutes of inactivity, the circuit breaker state is completely reset.
    /// For low-traffic consumers, failures spread over time may never trip the circuit.
    /// </summary>
    [Test]
    public async Task Eviction_ShouldResetCircuitBreakerState_ResilienceAmnesia()
    {
        // Arrange
        await using var builder = new HierarchicalResilienceBuilder(_logger);
        var globalPipeline = new ResiliencePipelineBuilder().Build();
        var key = "low-traffic-consumer";
        
        var options = new ConsumerCircuitBreakerOptions
        {
            FailureRatio = 0.5, // 50% failure ratio
            MinimumThroughput = 5, // Need 5 calls minimum
            SamplingDuration = TimeSpan.FromMinutes(10), // Long sampling window
            BreakDuration = TimeSpan.FromSeconds(30)
        };

        // Act: Accumulate 4 failures on the original pipeline (not enough to trip)
        var pipeline = builder.GetPipeline(key, globalPipeline, options);
        for (int i = 0; i < 4; i++)
        {
            try { await pipeline.ExecuteAsync<int>(_ => throw new Exception($"fail-{i}")); } catch { }
        }

        // Simulate 31+ minutes of inactivity by invalidating the pipeline
        // (This mimics what CleanupStalePipelines does after eviction threshold)
        builder.InvalidatePipeline(key);

        // Get a fresh pipeline (simulates what happens after eviction)
        var recreatedPipeline = builder.GetPipeline(key, globalPipeline, options);

        // The recreated pipeline should be different (new circuit breaker)
        recreatedPipeline.ShouldNotBeSameAs(pipeline);

        // Act: Add 4 more failures on the recreated pipeline
        for (int i = 0; i < 4; i++)
        {
            try { await recreatedPipeline.ExecuteAsync<int>(_ => throw new Exception($"fail-after-eviction-{i}")); } catch { }
        }

        // Assert: Circuit breaker should NOT be open, demonstrating "Resilience Amnesia"
        // We've had 8 total failures across both pipelines, but only 4 on the current one
        // Since MinimumThroughput=5 wasn't met with the 4 failures here, circuit stays closed
        bool circuitIsStillClosed = true;
        try
        {
            await recreatedPipeline.ExecuteAsync(_ => ValueTask.CompletedTask);
        }
        catch (BrokenCircuitException)
        {
            circuitIsStillClosed = false;
        }

        // The circuit should still be closed because the state was reset
        circuitIsStillClosed.ShouldBeTrue("Circuit breaker state was reset after eviction - Resilience Amnesia confirmed");
    }

    /// <summary>
    /// Verifies that concurrent calls to GetPipeline during recreation (after eviction)
    /// do not create duplicate pipelines due to race conditions in the GetOrAdd logic.
    /// </summary>
    [Test]
    public async Task ConcurrentGetPipeline_ShouldNotCreateDuplicates_DuringRecreation()
    {
        // Arrange
        await using var builder = new HierarchicalResilienceBuilder(_logger);
        var globalPipeline = new ResiliencePipelineBuilder().Build();
        var key = "concurrent-recreation-consumer";
        
        // Create initial pipeline, then evict it to force recreation
        var initialPipeline = builder.GetPipeline(key, globalPipeline);
        builder.InvalidatePipeline(key);

        // Act: Launch 100 concurrent calls to GetPipeline
        const int concurrentCalls = 100;
        var tasks = new Task<ResiliencePipeline>[concurrentCalls];
        using var startSignal = new ManualResetEventSlim(false);

        for (int i = 0; i < concurrentCalls; i++)
        {
            tasks[i] = Task.Run(() =>
            {
                // Wait for the signal to start all tasks at roughly the same time
                startSignal.Wait();
                return builder.GetPipeline(key, globalPipeline);
            });
        }

        // Give threads a moment to reach the wait point, then release them all
        await Task.Delay(50);
        startSignal.Set();

        var pipelines = await Task.WhenAll(tasks);

        // Assert: All 100 calls should return the exact same pipeline instance
        var firstPipeline = pipelines[0];
        foreach (var pipeline in pipelines)
        {
            pipeline.ShouldBeSameAs(firstPipeline, "All concurrent GetPipeline calls should return the same instance");
        }

        // The recreated pipeline should be different from the invalidated one
        firstPipeline.ShouldNotBeSameAs(initialPipeline, "Recreated pipeline should be a new instance");
    }
}

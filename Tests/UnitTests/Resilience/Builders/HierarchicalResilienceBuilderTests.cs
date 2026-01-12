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
}

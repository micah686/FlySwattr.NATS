using FlySwattr.NATS.Hosting.Health;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Hosting.Health;

/// <summary>
/// Tests for NatsConsumerHealthCheck to verify zombie consumer detection.
/// </summary>
[Property("nTag", "Hosting")]
public class NatsConsumerHealthCheckTests
{
    /// <summary>
    /// Test 8.1: Zombie Consumer Detection - Stalled Handler Simulation
    /// 
    /// This test validates that the health check correctly identifies a "zombie" consumer
    /// where the consume loop has stalled (e.g., due to a deadlock in the handler).
    /// 
    /// Architectural insight: A stuck business handler should bubble up as an infrastructure
    /// health failure, prompting the orchestrator (e.g., Kubernetes) to restart the pod.
    /// </summary>
    [Test]
    public async Task CheckHealthAsync_ShouldReturnUnhealthy_WhenLoopStalledBeyondTimeout()
    {
        // Arrange
        var fakeTime = new FakeTimeProvider();
        var metrics = new NatsConsumerHealthMetrics(fakeTime);
        var options = Options.Create(new NatsConsumerHealthCheckOptions
        {
            LoopIterationTimeout = TimeSpan.FromMinutes(2)
        });
        var healthCheck = new NatsConsumerHealthCheck(metrics, options, fakeTime);
        
        // Register consumer - this sets LastLoopIteration to "now"
        metrics.RegisterConsumer("test-stream", "test-consumer");
        
        // Simulate a deadlock: the loop hasn't iterated for 3 minutes (beyond the 2-minute timeout)
        fakeTime.Advance(TimeSpan.FromMinutes(3));
        
        // Act
        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext());
        
        // Assert
        result.Status.ShouldBe(HealthStatus.Unhealthy);
        result.Description.ShouldNotBeNull();
        result.Description.ShouldContain("Zombie consumers detected");
        result.Description.ShouldContain("test-stream/test-consumer");
        result.Data.ShouldContainKey("unhealthy_consumers");
    }

    [Test]
    public async Task CheckHealthAsync_ShouldReturnHealthy_WhenLoopActivelyIterating()
    {
        // Arrange
        var fakeTime = new FakeTimeProvider();
        var metrics = new NatsConsumerHealthMetrics(fakeTime);
        var options = Options.Create(new NatsConsumerHealthCheckOptions
        {
            LoopIterationTimeout = TimeSpan.FromMinutes(2)
        });
        var healthCheck = new NatsConsumerHealthCheck(metrics, options, fakeTime);
        
        // Register consumer
        metrics.RegisterConsumer("test-stream", "test-consumer");
        
        // Simulate normal operation: loop iterates frequently
        fakeTime.Advance(TimeSpan.FromSeconds(30));
        metrics.RecordLoopIteration("test-stream", "test-consumer");
        fakeTime.Advance(TimeSpan.FromSeconds(30));
        
        // Act
        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext());
        
        // Assert
        result.Status.ShouldBe(HealthStatus.Healthy);
    }

    [Test]
    public async Task CheckHealthAsync_ShouldReturnHealthy_WhenExactlyAtTimeout()
    {
        // Arrange
        var fakeTime = new FakeTimeProvider();
        var metrics = new NatsConsumerHealthMetrics(fakeTime);
        var options = Options.Create(new NatsConsumerHealthCheckOptions
        {
            LoopIterationTimeout = TimeSpan.FromMinutes(2)
        });
        var healthCheck = new NatsConsumerHealthCheck(metrics, options, fakeTime);
        
        // Register consumer
        metrics.RegisterConsumer("test-stream", "test-consumer");
        
        // Advance exactly to the timeout boundary (not beyond)
        fakeTime.Advance(TimeSpan.FromMinutes(2));
        
        // Act
        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext());
        
        // Assert - exactly at timeout should not be unhealthy (> not >=)
        result.Status.ShouldBe(HealthStatus.Healthy);
    }

    [Test]
    public async Task CheckHealthAsync_ShouldIgnoreInactiveConsumers()
    {
        // Arrange
        var fakeTime = new FakeTimeProvider();
        var metrics = new NatsConsumerHealthMetrics(fakeTime);
        var options = Options.Create(new NatsConsumerHealthCheckOptions
        {
            LoopIterationTimeout = TimeSpan.FromMinutes(2)
        });
        var healthCheck = new NatsConsumerHealthCheck(metrics, options, fakeTime);
        
        // Register and then unregister consumer (simulating graceful shutdown)
        metrics.RegisterConsumer("test-stream", "test-consumer");
        metrics.UnregisterConsumer("test-stream", "test-consumer");
        
        // Advance time beyond timeout
        fakeTime.Advance(TimeSpan.FromMinutes(5));
        
        // Act
        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext());
        
        // Assert - inactive consumers should be skipped, not reported as unhealthy
        result.Status.ShouldBe(HealthStatus.Healthy);
    }

    [Test]
    public async Task CheckHealthAsync_ShouldReturnDegraded_WhenNoMessagesForExtendedTime()
    {
        // Arrange
        var fakeTime = new FakeTimeProvider();
        var metrics = new NatsConsumerHealthMetrics(fakeTime);
        var options = Options.Create(new NatsConsumerHealthCheckOptions
        {
            LoopIterationTimeout = TimeSpan.FromMinutes(2),
            NoMessageWarningTimeout = TimeSpan.FromMinutes(5)
        });
        var healthCheck = new NatsConsumerHealthCheck(metrics, options, fakeTime);
        
        // Register consumer
        metrics.RegisterConsumer("test-stream", "test-consumer");
        
        // Loop is still iterating (so not a zombie)
        fakeTime.Advance(TimeSpan.FromMinutes(3));
        metrics.RecordLoopIteration("test-stream", "test-consumer");
        
        // But no messages processed for 6 minutes (beyond the 5-minute warning)
        fakeTime.Advance(TimeSpan.FromMinutes(3));
        metrics.RecordLoopIteration("test-stream", "test-consumer");
        
        // Act
        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext());
        
        // Assert
        result.Status.ShouldBe(HealthStatus.Degraded);
        result.Description!.ShouldContain("no recent messages");
    }
}

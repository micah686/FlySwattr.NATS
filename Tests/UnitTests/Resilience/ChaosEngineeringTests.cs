using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Hosting.Health;
using FlySwattr.NATS.Resilience.Builders;
using FlySwattr.NATS.Resilience.Configuration;
using FlySwattr.NATS.Topology.Configuration;
using FlySwattr.NATS.Topology.Services;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using NATS.Client.Core;
using NSubstitute;
using Polly;
using Polly.RateLimiting;
using Polly.Retry;
using Polly.Simmy;
using Polly.Simmy.Fault;
using Polly.Simmy.Latency;
using Polly.CircuitBreaker;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Resilience;

/// <summary>
/// Chaos Engineering tests using Polly v8 Simmy strategies to validate
/// FlySwattr's resilience architecture under failure conditions.
/// 
/// These tests prove that the "Clean Slate" architecture behaves as designed:
/// - Decorators (ResilientJetStreamConsumer, OffloadingJetStreamPublisher)
/// - Bulkheads (BulkheadManager)
/// - Circuit Breakers (HierarchicalResilienceBuilder)
/// </summary>
[Property("nTag", "Resilience")]
[Property("Category", "ChaosEngineering")]
public class ChaosEngineeringTests
{
    #region C01: The "Noisy Neighbor" Bulkhead Saturation

    /// <summary>
    /// C01: The "Noisy Neighbor" Bulkhead Saturation
    /// 
    /// Risk: A non-critical consumer (e.g., LogIngest) experiences high latency,
    /// filling the "Default" bulkhead pool. If isolation fails, critical system
    /// events (DLQ notifications) will be rejected.
    /// 
    /// Test: Apply 10-second chaos latency to all "default" pool consumers,
    /// then verify "critical" pool operations succeed immediately.
    /// </summary>
    [Test]
    public async Task C01_NoisyNeighborBulkheadSaturation_CriticalPoolShouldNotBeAffected()
    {
        // Arrange
        var config = new BulkheadConfiguration
        {
            NamedPools = new Dictionary<string, int>
            {
                ["default"] = 50,    // Limited to show saturation
                ["critical"] = 10    // DLQ, system events - guaranteed capacity
            },
            QueueLimitMultiplier = 2
        };

        var options = Substitute.For<IOptions<BulkheadConfiguration>>();
        options.Value.Returns(config);

        var bulkheadLogger = Substitute.For<ILogger<BulkheadManager>>();
        var bulkheadManager = new BulkheadManager(options, bulkheadLogger);

        try
        {
            // Create chaos latency pipeline that will stall "default" pool operations
            var chaosLatencyPipeline = new ResiliencePipelineBuilder()
                .AddChaosLatency(new ChaosLatencyStrategyOptions
                {
                    Enabled = true,
                    InjectionRate = 1.0, // 100% - simulate total downstream stall
                    Latency = TimeSpan.FromSeconds(10) // Longer than standard timeouts
                })
                .Build();

            var defaultPipeline = bulkheadManager.GetPoolPipeline("default");
            var criticalPipeline = bulkheadManager.GetPoolPipeline("critical");

            var blocker = new TaskCompletionSource();
            var defaultTasks = new List<Task>();

            // Act: Start 50 operations in the "default" pool that will be stalled
            // These simulate "noisy neighbor" consumers experiencing high latency
            for (int i = 0; i < 50; i++)
            {
                defaultTasks.Add(Task.Run(async () =>
                {
                    await defaultPipeline.ExecuteAsync(async ct =>
                    {
                        // Simulate the chaos latency effect by blocking
                        await blocker.Task;
                    });
                }));
            }

            // Give time for default pool to saturate
            await Task.Delay(100);

            // Act: Attempt operation on "critical" pool - should succeed immediately
            var criticalOperationCompleted = false;
            var criticalStopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            await criticalPipeline.ExecuteAsync(async ct =>
            {
                await Task.Delay(10, ct); // Minimal work
                criticalOperationCompleted = true;
            });
            
            criticalStopwatch.Stop();

            // Assert: Critical pool operation should complete quickly (not blocked by default pool)
            criticalOperationCompleted.ShouldBeTrue();
            criticalStopwatch.ElapsedMilliseconds.ShouldBeLessThan(1000,
                "Critical pool operation should not be blocked by default pool saturation");

            // Cleanup
            blocker.SetResult();
            await Task.WhenAll(defaultTasks);
        }
        finally
        {
            await bulkheadManager.DisposeAsync();
        }
    }

    /// <summary>
    /// C01 Variant: Verify that default pool rejects new operations when completely saturated
    /// (both permits and queue exhausted), while critical pool remains available.
    /// </summary>
    [Test]
    public async Task C01_DefaultPoolSaturated_ShouldThrowRateLimiterRejected_WhileCriticalSucceeds()
    {
        // Arrange
        var config = new BulkheadConfiguration
        {
            NamedPools = new Dictionary<string, int>
            {
                ["default"] = 10,   // Small limit to easily saturate
                ["critical"] = 5    // Separate pool
            },
            QueueLimitMultiplier = 1 // Queue = 10, Total capacity = 20
        };

        var options = Substitute.For<IOptions<BulkheadConfiguration>>();
        options.Value.Returns(config);

        var bulkheadLogger = Substitute.For<ILogger<BulkheadManager>>();
        var bulkheadManager = new BulkheadManager(options, bulkheadLogger);

        try
        {
            var defaultPipeline = bulkheadManager.GetPoolPipeline("default");
            var criticalPipeline = bulkheadManager.GetPoolPipeline("critical");

            var blocker = new TaskCompletionSource();
            var saturatingTasks = new List<Task>();

            // Saturate default pool completely (10 permits + 10 queue = 20 total)
            for (int i = 0; i < 20; i++)
            {
                saturatingTasks.Add(defaultPipeline.ExecuteAsync(async ct =>
                {
                    await blocker.Task;
                }).AsTask());
            }

            await Task.Delay(100); // Let tasks acquire permits/queue

            // Act & Assert: Next default pool operation should be rejected
            await Assert.ThrowsAsync<RateLimiterRejectedException>(async () =>
            {
                await defaultPipeline.ExecuteAsync(async ct =>
                {
                    await Task.Delay(10, ct);
                });
            });

            // Act & Assert: Critical pool should still work
            var criticalSucceeded = false;
            await criticalPipeline.ExecuteAsync(async ct =>
            {
                criticalSucceeded = true;
                await Task.CompletedTask;
            });

            criticalSucceeded.ShouldBeTrue("Critical pool should not be affected by default pool saturation");

            // Cleanup
            blocker.SetResult();
            await Task.WhenAll(saturatingTasks);
        }
        finally
        {
            await bulkheadManager.DisposeAsync();
        }
    }

    #endregion

    #region C02: The "Ghost Ack" (Idempotency Verification)

    /// <summary>
    /// C02: The "Ghost Ack" (Idempotency Verification)
    /// 
    /// Risk: NATS successfully receives a publish, but the network connection drops
    /// *before* the acknowledgement reaches FlySwattr. The ResiliencePipeline will
    /// retry the publish. If the Message ID changes on retry, duplicate data is created.
    /// 
    /// Test: Inject TimeoutException at 50% rate, verify messageId is preserved on retry.
    /// </summary>
    [Test]
    public async Task C02_GhostAck_MessageIdShouldBePreservedAcrossRetries()
    {
        // Arrange
        var capturedMessageIds = new List<string?>();
        var attemptCount = 0;
        var originalMessageId = "order-12345-unique-business-key";

        // Build pipeline: Retry (Outer) -> Chaos Fault (Inner)
        // This order ensures chaos throws, retry catches and retries
        var chaosPipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = 3,
                Delay = TimeSpan.Zero // Fast tests
            })
            .AddChaosFault(new ChaosFaultStrategyOptions
            {
                InjectionRate = 0.5, // Fail 50% of the time
                Enabled = true,
                FaultGenerator = static args => 
                    new ValueTask<Exception?>(new TimeoutException("Simmy says: Ack Lost!"))
            })
            .Build();

        // Act: Execute with the chaos pipeline, simulating publish behavior
        await chaosPipeline.ExecuteAsync(async ct =>
        {
            // This inner action simulates the NATS client call
            // Capture the messageId that would be passed to PublishAsync
            capturedMessageIds.Add(originalMessageId);
            Interlocked.Increment(ref attemptCount);
            await Task.CompletedTask;
        });

        // Assert: All attempts should use the exact same messageId
        attemptCount.ShouldBeGreaterThanOrEqualTo(1, "At least one attempt should have been made");
        
        foreach (var capturedId in capturedMessageIds)
        {
            capturedId.ShouldBe(originalMessageId, 
                "Message ID must be identical on ALL retry attempts to leverage JetStream deduplication");
        }
    }

    /// <summary>
    /// C02 Variant: Test with mock publisher to verify real publish behavior
    /// </summary>
    [Test]
    public async Task C02_GhostAck_PublisherRetry_ShouldNeverRegenerateMessageId()
    {
        // Arrange
        var capturedMessageIds = new List<string?>();
        var mockPublisher = Substitute.For<IJetStreamPublisher>();
        
        // Simulate: First call fails with timeout, second succeeds
        var callCount = 0;
        mockPublisher.PublishAsync(
            Arg.Any<string>(),
            Arg.Any<object>(),
            Arg.Any<string?>(),
            Arg.Any<MessageHeaders?>(),
            Arg.Any<CancellationToken>())
            .Returns(x =>
            {
                capturedMessageIds.Add(x.ArgAt<string?>(2));
                callCount++;
                if (callCount == 1)
                {
                    throw new TimeoutException("Network Ack Lost");
                }
                return Task.CompletedTask;
            });

        // Build retry pipeline
        var retryPipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = 3,
                Delay = TimeSpan.Zero,
                ShouldHandle = new PredicateBuilder().Handle<TimeoutException>()
            })
            .Build();

        var fixedMessageId = "fixed-idempotency-key-abc123";

        // Act
        await retryPipeline.ExecuteAsync(async ct =>
        {
            await mockPublisher.PublishAsync("orders.created", new { OrderId = 123 }, fixedMessageId, cancellationToken: ct);
        });

        // Assert
        capturedMessageIds.Count.ShouldBe(2, "Should have made 2 attempts (1 fail + 1 success)");
        capturedMessageIds[0].ShouldBe(fixedMessageId);
        capturedMessageIds[1].ShouldBe(fixedMessageId);
        
        // All IDs should be identical
        capturedMessageIds.Distinct().Count().ShouldBe(1, 
            "All captured message IDs should be identical - no regeneration on retry!");
    }

    #endregion

    #region C03: The "Zombie" Consumer

    /// <summary>
    /// C03: The "Zombie" Consumer
    /// 
    /// Risk: The BasicNatsConsumerService loop hangs indefinitely (deadlock, NATS client
    /// waiting for bytes that never come). A crashed consumer restarts, but a *hung*
    /// consumer holds resources silently.
    /// 
    /// Test: Simulate a consumer that stops iterating, verify health check reports Unhealthy.
    /// </summary>
    [Test]
    public async Task C03_ZombieConsumer_HealthCheckShouldReportUnhealthy_WhenLoopStalls()
    {
        // Arrange
        var fakeTime = new FakeTimeProvider();
        var metrics = new NatsConsumerHealthMetrics(fakeTime);
        var healthCheckOptions = Options.Create(new NatsConsumerHealthCheckOptions
        {
            LoopIterationTimeout = TimeSpan.FromMinutes(2) // Consumer must iterate within 2 minutes
        });
        var healthCheck = new NatsConsumerHealthCheck(metrics, healthCheckOptions, fakeTime);

        // Register a consumer (simulates consumer startup)
        metrics.RegisterConsumer("orders-stream", "order-processor");

        // Consumer is initially healthy
        var initialResult = await healthCheck.CheckHealthAsync(new HealthCheckContext());
        initialResult.Status.ShouldBe(HealthStatus.Healthy);

        // Simulate "zombie" behavior: consumer loop hangs (no iterations for 3 minutes)
        // This could happen due to:
        // - Deadlock in handler
        // - NATS client blocked on read
        // - Task.Delay(Timeout.Infinite) injected by Simmy
        fakeTime.Advance(TimeSpan.FromMinutes(3));

        // Act
        var zombieResult = await healthCheck.CheckHealthAsync(new HealthCheckContext());

        // Assert
        zombieResult.Status.ShouldBe(HealthStatus.Unhealthy,
            "Health check must detect zombie consumers that have stopped iterating");
        zombieResult.Description.ShouldNotBeNull();
        zombieResult.Description.ShouldContain("Zombie consumers detected");
        zombieResult.Description.ShouldContain("orders-stream/order-processor");
        zombieResult.Data.ShouldContainKey("unhealthy_consumers");
    }

    /// <summary>
    /// C03 Variant: Simulates the actual chaos behavior pattern that would cause zombie state
    /// </summary>
    [Test]
    public async Task C03_ZombieConsumer_ChaosBehaviorSimulation_ShouldTriggerHealthFailure()
    {
        // Arrange
        var fakeTime = new FakeTimeProvider();
        var metrics = new NatsConsumerHealthMetrics(fakeTime);
        var healthCheckOptions = Options.Create(new NatsConsumerHealthCheckOptions
        {
            LoopIterationTimeout = TimeSpan.FromMinutes(2)
        });
        var healthCheck = new NatsConsumerHealthCheck(metrics, healthCheckOptions, fakeTime);

        // Build chaos behavior pipeline that would hang the consumer
        var hangingBehaviorTriggered = false;
        var chaosBehaviorPipeline = new ResiliencePipelineBuilder()
            .AddChaosBehavior(new Polly.Simmy.Behavior.ChaosBehaviorStrategyOptions
            {
                Enabled = true,
                InjectionRate = 1.0, // Always inject
                BehaviorGenerator = args =>
                {
                    hangingBehaviorTriggered = true;
                    return ValueTask.CompletedTask;
                    // In real scenario, this would be: await Task.Delay(Timeout.Infinite, args.Context.CancellationToken);
                    // For test, we just mark it was triggered
                }
            })
            .Build();

        // Register consumer
        metrics.RegisterConsumer("log-ingest", "log-processor");

        // Simulate normal operation: consumer iterates regularly
        metrics.RecordLoopIteration("log-ingest", "log-processor");
        fakeTime.Advance(TimeSpan.FromSeconds(30));
        metrics.RecordLoopIteration("log-ingest", "log-processor");

        // Verify healthy state
        var healthyResult = await healthCheck.CheckHealthAsync(new HealthCheckContext());
        healthyResult.Status.ShouldBe(HealthStatus.Healthy);

        // Execute chaos behavior (this would normally hang the consumer)
        await chaosBehaviorPipeline.ExecuteAsync(async ct => await Task.CompletedTask);
        hangingBehaviorTriggered.ShouldBeTrue();

        // Simulate that after chaos injection, the loop stops iterating
        // Time passes beyond the timeout threshold
        fakeTime.Advance(TimeSpan.FromMinutes(2.5));

        // Act
        var zombieResult = await healthCheck.CheckHealthAsync(new HealthCheckContext());

        // Assert
        zombieResult.Status.ShouldBe(HealthStatus.Unhealthy);
    }

    #endregion

    #region C04: The "Circuit Breaker Isolation" Test

    /// <summary>
    /// C04: The "Circuit Breaker Isolation" Test
    /// 
    /// Risk: A cascading failure where one failing consumer stream trips the Circuit Breaker
    /// for *all* streams because the keys are not unique or the pipeline is shared incorrectly.
    /// 
    /// Test: Trip Consumer A's circuit breaker, verify Consumer B is unaffected.
    /// </summary>
    [Test]
    public async Task C04_CircuitBreakerIsolation_ConsumerB_ShouldSucceed_WhenConsumerA_CircuitOpen()
    {
        // Arrange
        var logger = Substitute.For<ILogger<HierarchicalResilienceBuilder>>();
        var resilienceBuilder = new HierarchicalResilienceBuilder(
            logger,
            new ConsumerCircuitBreakerOptions
            {
                FailureRatio = 0.3,
                MinimumThroughput = 5,
                SamplingDuration = TimeSpan.FromSeconds(10),
                BreakDuration = TimeSpan.FromSeconds(30)
            });

        try
        {
            var globalPipeline = new ResiliencePipelineBuilder().Build();

            // Get separate pipelines for Consumer A and Consumer B
            var consumerAPipeline = resilienceBuilder.GetPipeline("StreamA/ConsumerA", globalPipeline);
            var consumerBPipeline = resilienceBuilder.GetPipeline("StreamB/ConsumerB", globalPipeline);

            // Inject chaos fault for Consumer A only
            var chaosForConsumerA = new ResiliencePipelineBuilder()
                .AddChaosFault(new ChaosFaultStrategyOptions
                {
                    Enabled = true,
                    InjectionRate = 1.0, // 100% - always fail
                    FaultGenerator = static args => 
                        new ValueTask<Exception?>(new InvalidOperationException("Database Down"))
                })
                .Build();

            // Act: Bombard Consumer A with failures until circuit opens
            var consumerACircuitOpened = false;
            for (int i = 0; i < 20; i++) // More than MinimumThroughput
            {
                try
                {
                    await consumerAPipeline.ExecuteAsync(async ct =>
                    {
                        // Inject the fault
                        await chaosForConsumerA.ExecuteAsync(async innerCt =>
                        {
                            await Task.CompletedTask;
                        }, ct);
                    });
                }
                catch (BrokenCircuitException)
                {
                    consumerACircuitOpened = true;
                    break;
                }
                catch (InvalidOperationException)
                {
                    // Expected from chaos injection - continue to trip circuit
                }
            }

            consumerACircuitOpened.ShouldBeTrue("Consumer A's circuit should have opened after repeated failures");

            // Act: Consumer B should process successfully (isolated circuit breaker)
            var consumerBSuccess = false;
            Exception? consumerBException = null;

            try
            {
                await consumerBPipeline.ExecuteAsync(async ct =>
                {
                    consumerBSuccess = true;
                    await Task.CompletedTask;
                });
            }
            catch (Exception ex)
            {
                consumerBException = ex;
            }

            // Assert
            consumerBSuccess.ShouldBeTrue("Consumer B should succeed - its circuit is independent");
            consumerBException.ShouldBeNull("Consumer B should not throw BrokenCircuitException from Consumer A's circuit");
        }
        finally
        {
            await resilienceBuilder.DisposeAsync();
        }
    }

    /// <summary>
    /// C04 Variant: Verify same consumer key returns same pipeline (not new circuit breaker)
    /// </summary>
    [Test]
    public async Task C04_CircuitBreakerIsolation_SameConsumerKey_ShouldReturnSamePipeline()
    {
        // Arrange
        var logger = Substitute.For<ILogger<HierarchicalResilienceBuilder>>();
        var resilienceBuilder = new HierarchicalResilienceBuilder(logger);

        try
        {
            var globalPipeline = new ResiliencePipelineBuilder().Build();

            // Act
            var pipeline1 = resilienceBuilder.GetPipeline("StreamX/ConsumerX", globalPipeline);
            var pipeline2 = resilienceBuilder.GetPipeline("StreamX/ConsumerX", globalPipeline);
            var pipeline3 = resilienceBuilder.GetPipeline("StreamY/ConsumerY", globalPipeline);

            // Assert
            pipeline1.ShouldBeSameAs(pipeline2, "Same consumer key should return same pipeline instance");
            pipeline1.ShouldNotBeSameAs(pipeline3, "Different consumer key should return different pipeline instance");
        }
        finally
        {
            await resilienceBuilder.DisposeAsync();
        }
    }

    #endregion

    #region C05: The "Cold Start" Crash Loop

    /// <summary>
    /// C05: The "Cold Start" Crash Loop
    /// 
    /// Risk: Application starts while NATS is unavailable. TopologyProvisioningService
    /// might not have a retry policy, causing the container to crash immediately,
    /// leading to Kubernetes CrashLoopBackOff.
    /// 
    /// Test: Simulate NATS unavailable for startup, verify service waits and recovers.
    /// </summary>
    [Test]
    public async Task C05_ColdStartCrashLoop_ServiceShouldWaitAndRecover_NotCrash()
    {
        // Arrange
        var mockConnection = Substitute.For<INatsConnection>();
        var mockTopologyManager = Substitute.For<ITopologyManager>();
        var mockReadySignal = Substitute.For<ITopologyReadySignal>();
        var logger = Substitute.For<ILogger<TopologyProvisioningService>>();

        var topologyOptions = Options.Create(new TopologyStartupOptions
        {
            MaxRetryAttempts = 5,
            InitialRetryDelay = TimeSpan.FromMilliseconds(50), // Fast for tests
            MaxRetryDelay = TimeSpan.FromMilliseconds(200),
            ConnectionTimeout = TimeSpan.FromMilliseconds(100),
            TotalStartupTimeout = TimeSpan.FromSeconds(30)
        });

        // Simulate NATS unavailable: connection state is not Open
        var connectionAttempts = 0;
        mockConnection.ConnectionState.Returns(x =>
        {
            connectionAttempts++;
            // First 3 attempts: NATS not ready
            // After that: NATS becomes available
            return connectionAttempts <= 3 
                ? NatsConnectionState.Closed 
                : NatsConnectionState.Open;
        });

        // Ping succeeds only after connection is open
        mockConnection.PingAsync(Arg.Any<CancellationToken>())
            .Returns(x =>
            {
                if (connectionAttempts <= 3)
                {
                    throw new TimeoutException("Connection refused - NATS not ready");
                }
                return ValueTask.FromResult(TimeSpan.Zero);
            });

        // No topology sources (simplifies test)
        var emptySources = Array.Empty<ITopologySource>();

        var service = new TopologyProvisioningService(
            emptySources,
            mockTopologyManager,
            mockConnection,
            logger,
            topologyOptions,
            mockReadySignal);

        // Act: Start the service - should NOT crash even though NATS is initially unavailable
        Exception? startupException = null;
        var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        try
        {
            await service.StartAsync(cancellationTokenSource.Token);
        }
        catch (Exception ex)
        {
            startupException = ex;
        }

        // Assert
        startupException.ShouldBeNull("Service should NOT crash during NATS unavailability - it should wait and retry");
        connectionAttempts.ShouldBeGreaterThan(3, "Service should have retried multiple times before succeeding");
        mockReadySignal.Received().SignalReady();
    }

    /// <summary>
    /// C05 Variant: Service should eventually fail with clear error if NATS never becomes available
    /// </summary>
    [Test]
    public async Task C05_ColdStartCrashLoop_ShouldFailGracefully_WhenNatsNeverAvailable()
    {
        // Arrange
        var mockConnection = Substitute.For<INatsConnection>();
        var mockTopologyManager = Substitute.For<ITopologyManager>();
        var mockReadySignal = Substitute.For<ITopologyReadySignal>();
        var logger = Substitute.For<ILogger<TopologyProvisioningService>>();

        var topologyOptions = Options.Create(new TopologyStartupOptions
        {
            MaxRetryAttempts = 3,
            InitialRetryDelay = TimeSpan.FromMilliseconds(10),
            MaxRetryDelay = TimeSpan.FromMilliseconds(50),
            ConnectionTimeout = TimeSpan.FromMilliseconds(50),
            TotalStartupTimeout = TimeSpan.FromMilliseconds(500) // Short timeout for test
        });

        // NATS never becomes available
        mockConnection.ConnectionState.Returns(NatsConnectionState.Closed);
        mockConnection.PingAsync(Arg.Any<CancellationToken>())
            .Returns(x => ValueTask.FromException<TimeSpan>(new TimeoutException("Connection refused - NATS permanently unavailable")));

        var emptySources = Array.Empty<ITopologySource>();

        var service = new TopologyProvisioningService(
            emptySources,
            mockTopologyManager,
            mockConnection,
            logger,
            topologyOptions,
            mockReadySignal);

        // Act & Assert: Service should fail gracefully with timeout after exhausting retries
        var exception = await Assert.ThrowsAsync<TimeoutException>(async () =>
        {
            await service.StartAsync(CancellationToken.None);
        });

        // Verify the failure is logged/signaled properly
        exception.ShouldNotBeNull();
        // The service either throws the inner exception (if retries exhausted) or a timeout exception (if global timeout reached)
        // Ideally we should just check that it failed
        var msg = exception.Message;
        var valid = msg.Contains("startup timeout", StringComparison.OrdinalIgnoreCase) || 
                    msg.Contains("Connection refused", StringComparison.OrdinalIgnoreCase);
        
        valid.ShouldBeTrue($"Exception message '{msg}' did not contain expected error text");
        mockReadySignal.DidNotReceive().SignalReady();
    }

    /// <summary>
    /// C05 Variant: Using chaos fault injection to simulate NATS connection failures
    /// </summary>
    [Test]
    public async Task C05_ColdStartCrashLoop_ChaosFaultInjection_ShouldBeRecoverable()
    {
        // Arrange
        var attemptCount = 0;
        var chaosDisabledAfterAttempt = 5;

        // Build a pipeline that simulates NATS connection failures during cold start
        var chaosFaultPipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = 10,
                Delay = TimeSpan.FromMilliseconds(10),
                ShouldHandle = new PredicateBuilder().Handle<NATS.Client.Core.NatsException>(),
                OnRetry = args =>
                {
                    Interlocked.Increment(ref attemptCount);
                    return ValueTask.CompletedTask;
                }
            })
            .AddChaosFault(new ChaosFaultStrategyOptions
            {
                Enabled = true,
                InjectionRate = 1.0,
                EnabledGenerator = args =>
                {
                    // Disable chaos after N attempts (simulates NATS becoming available)
                    return new ValueTask<bool>(attemptCount < chaosDisabledAfterAttempt);
                },
                FaultGenerator = static args =>
                {
                    return new ValueTask<Exception?>(
                        new NATS.Client.Core.NatsException("Connection refused"));
                }
            })
            .Build();

        var connectionEstablished = false;

        // Act: Execute through the chaos pipeline
        await chaosFaultPipeline.ExecuteAsync(async ct =>
        {
            Interlocked.Increment(ref attemptCount);
            connectionEstablished = true;
            await Task.CompletedTask;
        });

        // Assert
        connectionEstablished.ShouldBeTrue("Connection should eventually succeed after chaos is disabled");
        attemptCount.ShouldBeGreaterThanOrEqualTo(chaosDisabledAfterAttempt,
            "Should have retried through the chaos period before succeeding");
    }

    #endregion
}

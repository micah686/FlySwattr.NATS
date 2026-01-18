using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Resilience.Builders;
using FlySwattr.NATS.Resilience.Decorators;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Polly;
using Polly.CircuitBreaker;
using Polly.Retry;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Resilience.Decorators;

[Property("nTag", "Resilience")]
public class ResilientJetStreamPublisherTests : IAsyncDisposable
{
    private readonly IJetStreamPublisher _inner;
    private readonly HierarchicalResilienceBuilder _resilienceBuilder;
    private readonly ILogger<ResilientJetStreamPublisher> _logger;
    private readonly ResilientJetStreamPublisher _sut;

    public ResilientJetStreamPublisherTests()
    {
        _inner = Substitute.For<IJetStreamPublisher>();
        
        var builderLogger = Substitute.For<ILogger<HierarchicalResilienceBuilder>>();
        _resilienceBuilder = new HierarchicalResilienceBuilder(builderLogger);
        
        _logger = Substitute.For<ILogger<ResilientJetStreamPublisher>>();

        _sut = new ResilientJetStreamPublisher(_inner, _resilienceBuilder, _logger);
    }

    public async ValueTask DisposeAsync()
    {
        await _resilienceBuilder.DisposeAsync();
    }

    [Test]
    public async Task PublishAsync_ShouldCallInner_WithMessageId()
    {
        // Arrange
        var subject = "test.subject";
        var message = "payload";
        var messageId = "msg-123";

        // Act
        await _sut.PublishAsync(subject, message, messageId);

        // Assert
        await _inner.Received(1).PublishAsync(subject, message, messageId, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PublishAsync_Overload_ShouldPassNullMessageId()
    {
        // Arrange
        var subject = "test.subject";
        var message = "payload";

        // Act
        await _sut.PublishAsync(subject, message);

        // Assert
        await _inner.Received(1).PublishAsync(subject, message, null, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PublishAsync_ShouldRetry_OnTransientException()
    {
        // Arrange
        var subject = "test.subject";
        var message = "payload";
        var messageId = "msg-123";

        // Fail once then succeed
        _inner.PublishAsync(subject, message, messageId, Arg.Any<CancellationToken>())
            .Returns(
                x => throw new TimeoutException("Simulated timeout"),
                x => Task.CompletedTask
            );

        // Act
        await _sut.PublishAsync(subject, message, messageId);

        // Assert
        await _inner.Received(2).PublishAsync(subject, message, messageId, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PublishAsync_OnRetry_ShouldUseSameMessageId()
    {
        // Arrange
        var subject = "test.subject";
        var message = "payload";
        var originalMessageId = "idempotency-key-abc123";
        var capturedMessageIds = new List<string?>();

        _inner.PublishAsync(subject, message, Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(
                x => { capturedMessageIds.Add(x.ArgAt<string?>(2)); throw new TimeoutException("Retry 1"); },
                x => { capturedMessageIds.Add(x.ArgAt<string?>(2)); throw new TimeoutException("Retry 2"); },
                x => { capturedMessageIds.Add(x.ArgAt<string?>(2)); return Task.CompletedTask; }
            );

        // Act
        await _sut.PublishAsync(subject, message, originalMessageId);

        // Assert - all 3 attempts should use the exact same messageId
        await Assert.That(capturedMessageIds.Count).IsEqualTo(3);
        foreach (var capturedId in capturedMessageIds)
        {
            await Assert.That(capturedId).IsEqualTo(originalMessageId);
        }
    }

    [Test]
    public async Task PublishAsync_OnMultipleRetries_ShouldNeverRegenerateMessageId()
    {
        // Arrange
        var subject = "test.subject";
        var message = "payload";
        var businessKeyId = "order-12345-created";

        // Fail 3 times (max retries), then succeed on 4th (if allowed) - but we have 3 max retries
        // So with initial + 3 retries = 4 total calls max
        _inner.PublishAsync(subject, message, Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(
                x => throw new TimeoutException("Attempt 1"),
                x => throw new TimeoutException("Attempt 2"),
                x => throw new TimeoutException("Attempt 3"),
                x => Task.CompletedTask
            );

        // Act
        await _sut.PublishAsync(subject, message, businessKeyId);

        // Assert - verify ALL calls used the SAME messageId (critical for deduplication)
        await _inner.Received(4).PublishAsync(
            subject, 
            message, 
            Arg.Is<string?>(id => id == businessKeyId), // Must be exactly the original ID
            Arg.Any<CancellationToken>());
    }

    #region 3.3 Publisher Resilience vs. Driver Reconnects

    /// <summary>
    /// Verifies that NatsNoRespondersException (service not available) is NOT treated as 
    /// transient by Polly. This exception indicates no JetStream responders, which typically 
    /// means the stream doesn't exist or JetStream is disabled - retrying won't help.
    /// 
    /// In a real scenario with MaxReconnect=0 (fail fast), we rely on Polly for transient 
    /// errors like timeouts, but NatsNoRespondersException should propagate immediately.
    /// </summary>
    [Test]
    public async Task PublishAsync_ShouldNotRetry_OnNatsNoRespondersException()
    {
        // Arrange
        var subject = "test.subject";
        var message = "payload";
        var messageId = "msg-no-responders";

        _inner.PublishAsync(subject, message, messageId, Arg.Any<CancellationToken>())
            .Returns(x => throw new NATS.Client.Core.NatsNoRespondersException());

        // Act & Assert - should throw immediately without retrying
        await Assert.ThrowsAsync<NATS.Client.Core.NatsNoRespondersException>(
            async () => await _sut.PublishAsync(subject, message, messageId));

        // Verify only 1 call was made (no retries)
        await _inner.Received(1).PublishAsync(subject, message, messageId, Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// Verifies that driver-layer IOException exceptions ARE treated as transient and trigger 
    /// retries. This is critical when NATS driver MaxReconnect=0 (fail fast) and we rely 
    /// solely on Polly for retry logic.
    /// </summary>
    [Test]
    public async Task PublishAsync_WithDriverLayerIOException_ShouldRetry()
    {
        // Arrange
        var subject = "test.subject";
        var message = "payload";
        var messageId = "msg-io-retry";

        // Fail with IOException (driver layer), then succeed
        _inner.PublishAsync(subject, message, messageId, Arg.Any<CancellationToken>())
            .Returns(
                x => throw new System.IO.IOException("Connection reset by peer"),
                x => Task.CompletedTask
            );

        // Act
        await _sut.PublishAsync(subject, message, messageId);

        // Assert - should have retried once
        await _inner.Received(2).PublishAsync(subject, message, messageId, Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// Verifies that total retry duration is bounded and doesn't cause requests to hang 
    /// indefinitely. With 3 max retries and exponential backoff, the total time should 
    /// be predictable: initial + retry1 + retry2 + retry3.
    /// 
    /// This prevents the "multiplicative retry explosion" when NATS driver retries are 
    /// combined with Polly retries - we ensure application-layer timeout constraints.
    /// </summary>
    [Test]
    public async Task PublishAsync_TotalDuration_ShouldRespectTimeoutConstraints()
    {
        // Arrange
        var subject = "test.subject";
        var message = "payload";
        var messageId = "msg-duration-test";

        // Fail consistently with transient errors to exhaust all retries
        _inner.PublishAsync(subject, message, messageId, Arg.Any<CancellationToken>())
            .Returns(x => throw new TimeoutException("Simulated timeout"));

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Act
        try
        {
            await _sut.PublishAsync(subject, message, messageId);
        }
        catch (TimeoutException)
        {
            // Expected - all retries exhausted
        }

        stopwatch.Stop();

        // Assert - With exponential backoff (1s base, jitter), 4 attempts (initial + 3 retries)
        // Expected max: ~1s + ~2s + ~4s = ~7s base + jitter overhead
        // We use a generous upper bound of 15 seconds to account for jitter and test runner variance
        // The key assertion is that it DOESN'T hang for minutes (which would happen with 
        // multiplicative retry: 3 Polly * 10 NATS = 30 attempts)
        await Assert.That(stopwatch.Elapsed.TotalSeconds).IsLessThan(15);

        // Should have made 4 attempts (initial + 3 retries)
        await _inner.Received(4).PublishAsync(subject, message, messageId, Arg.Any<CancellationToken>());
    }

    #endregion

    #region Circuit Breaker Integration

    /// <summary>
    /// Verifies that when the circuit breaker is open, PublishAsync throws BrokenCircuitException
    /// immediately WITHOUT making a network call to the inner publisher. This is critical for
    /// fail-fast behavior and prevents wasting resources on calls that will fail.
    /// 
    /// Note: ResilientJetStreamPublisher has TWO circuit breakers in series:
    /// 1. Global Publisher CB (MinimumThroughput=10, hardcoded)
    /// 2. Consumer-level CB (MinimumThroughput=2, from ConsumerCircuitBreakerOptions)
    /// 
    /// We trip the consumer-level breaker, which has more aggressive settings.
    /// Each failed publish attempt triggers 4 inner calls (initial + 3 retries), so 2 publish calls
    /// = 8 inner calls, enough to trip the consumer-level breaker with MinimumThroughput=2.
    /// </summary>
    [Test]
    public async Task PublishAsync_WhenCircuitOpen_ShouldThrowBrokenCircuitException_WithoutNetworkCall()
    {
        // Arrange - Create a publisher with a low-threshold consumer circuit breaker
        var inner = Substitute.For<IJetStreamPublisher>();
        var builderLogger = Substitute.For<ILogger<HierarchicalResilienceBuilder>>();
        
        // Fast retry options
        var retryOptions = new RetryStrategyOptions
        {
            MaxRetryAttempts = 3,
            BackoffType = DelayBackoffType.Constant,
            Delay = TimeSpan.Zero,
            ShouldHandle = new PredicateBuilder().Handle<Exception>()
        };
        
        // Consumer-level circuit breaker with very aggressive settings
        var cbOptions = new ConsumerCircuitBreakerOptions
        {
            FailureRatio = 0.5,        // 50% failure rate trips
            MinimumThroughput = 2,     // Minimum allowed by Polly
            SamplingDuration = TimeSpan.FromSeconds(60),
            BreakDuration = TimeSpan.FromSeconds(30)
        };
        
        await using var resilienceBuilder = new HierarchicalResilienceBuilder(builderLogger, cbOptions);
        var logger = Substitute.For<ILogger<ResilientJetStreamPublisher>>();
        var sut = new ResilientJetStreamPublisher(inner, resilienceBuilder, logger, retryOptions);
        
        var subject = "test.subject";
        var message = "payload";

        // All inner calls fail with TimeoutException
        inner.PublishAsync(subject, message, Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(x => throw new TimeoutException("Simulated failure"));

        // Make multiple publish calls to trip the circuit
        // Consumer CB needs MinimumThroughput=2 at 50% failure rate = need >= 2 failed outcomes
        // Each publish call = 1 outcome (after retries exhausted)
        // NOTE: Catch any exception since circuit may trip mid-loop
        for (int i = 0; i < 5; i++)
        {
            try { await sut.PublishAsync(subject, message, $"msg-{i}"); } 
            catch (Exception) { /* TimeoutException or BrokenCircuitException */ }
        }

        // Clear the mock to track only the final call
        inner.ClearReceivedCalls();

        // Act & Assert - Next call should throw BrokenCircuitException immediately
        await Assert.ThrowsAsync<BrokenCircuitException>(
            async () => await sut.PublishAsync(subject, message, "msg-final"));

        // Critical assertion: inner publisher should NOT have been called
        // because the circuit breaker rejected the call before it reached the inner publisher
        await inner.DidNotReceive().PublishAsync(
            Arg.Any<string>(), 
            Arg.Any<string>(), 
            Arg.Any<string?>(), 
            Arg.Any<CancellationToken>());
    }

    #endregion

    #region Exception Whitelisting

    /// <summary>
    /// Verifies that OperationCanceledException does NOT count toward the circuit breaker's
    /// failure rate. This is critical because user-initiated cancellations (e.g., request timeouts,
    /// graceful shutdown) should not trip the system-wide circuit breaker and cause outages.
    /// 
    /// The policy filters OperationCanceledException via ShouldHandle predicate:
    /// ShouldHandle = new PredicateBuilder().Handle&lt;Exception&gt;(ex => !IsOperationCanceled(ex))
    /// 
    /// Note: OperationCanceledException also triggers retries (IsTransient includes it),
    /// but those retries don't count toward circuit breaker failures.
    /// </summary>
    [Test]
    public async Task PublishAsync_WithOperationCanceledException_ShouldNotTripCircuitBreaker()
    {
        // Arrange - Create a publisher with a low-threshold circuit breaker
        var inner = Substitute.For<IJetStreamPublisher>();
        var builderLogger = Substitute.For<ILogger<HierarchicalResilienceBuilder>>();
        
        // Fast retry options
        var retryOptions = new RetryStrategyOptions
        {
            MaxRetryAttempts = 3,
            BackoffType = DelayBackoffType.Constant,
            Delay = TimeSpan.Zero,
            ShouldHandle = new PredicateBuilder().Handle<Exception>()
        };
        
        // Low threshold - trip after 2 failures at 50% failure rate
        var cbOptions = new ConsumerCircuitBreakerOptions
        {
            FailureRatio = 0.5,
            MinimumThroughput = 2,  // Minimum allowed by Polly
            SamplingDuration = TimeSpan.FromSeconds(60),
            BreakDuration = TimeSpan.FromSeconds(30)
        };
        
        await using var resilienceBuilder = new HierarchicalResilienceBuilder(builderLogger, cbOptions);
        var logger = Substitute.For<ILogger<ResilientJetStreamPublisher>>();
        var sut = new ResilientJetStreamPublisher(inner, resilienceBuilder, logger, retryOptions);
        
        var subject = "test.subject";
        var message = "payload";

        // All inner calls throw OperationCanceledException
        // This exception should NOT count toward circuit breaker failure rate
        inner.PublishAsync(subject, message, Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(x => throw new OperationCanceledException("User cancelled"));

        // Make many publish calls - far more than needed to trip a normal circuit
        // Each call will exhaust retries and throw OperationCanceledException
        for (int i = 0; i < 10; i++)
        {
            try { await sut.PublishAsync(subject, message, $"msg-cancel-{i}"); }
            catch (OperationCanceledException) { /* Expected */ }
        }

        // Clear the mock and reconfigure for success
        inner.ClearReceivedCalls();
        inner.PublishAsync(subject, message, "msg-success", Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        // Act - This call should succeed because circuit is still CLOSED
        // If OperationCanceledException counted toward failures, circuit would be open
        await sut.PublishAsync(subject, message, "msg-success");

        // Assert - The final call succeeded (no BrokenCircuitException)
        // This proves OperationCanceledException didn't trip the circuit
        await inner.Received(1).PublishAsync(subject, message, "msg-success", Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// Verifies mixed exception behavior: OperationCanceledException is excluded from
    /// circuit breaker failure counting, while TimeoutException IS counted.
    /// This ensures only "real" failures trip the circuit.
    /// </summary>
    [Test]
    public async Task PublishAsync_MixedExceptions_OnlyRealFailuresCountTowardCircuitBreaker()
    {
        // Arrange
        var inner = Substitute.For<IJetStreamPublisher>();
        var builderLogger = Substitute.For<ILogger<HierarchicalResilienceBuilder>>();
        
        // Fast retry options to prevent test hanging
        var retryOptions = new RetryStrategyOptions
        {
            MaxRetryAttempts = 3,
            BackoffType = DelayBackoffType.Constant,
            Delay = TimeSpan.Zero,
            ShouldHandle = new PredicateBuilder().Handle<Exception>()
        };
        
        // Aggressive settings for fast test execution
        var cbOptions = new ConsumerCircuitBreakerOptions
        {
            FailureRatio = 0.5,
            MinimumThroughput = 2,  // Minimum allowed by Polly
            SamplingDuration = TimeSpan.FromSeconds(60),
            BreakDuration = TimeSpan.FromSeconds(30)
        };
        
        await using var resilienceBuilder = new HierarchicalResilienceBuilder(builderLogger, cbOptions);
        var logger = Substitute.For<ILogger<ResilientJetStreamPublisher>>();
        var sut = new ResilientJetStreamPublisher(inner, resilienceBuilder, logger, retryOptions);
        
        var subject = "test.subject";
        var message = "payload";

        // Phase 1: Multiple cancellations - should NOT trip circuit
        inner.PublishAsync(subject, message, Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(x => throw new OperationCanceledException("User cancelled"));

        for (int i = 0; i < 10; i++)
        {
            try { await sut.PublishAsync(subject, message, $"msg-cancel-{i}"); } 
            catch (OperationCanceledException) { }
        }

        // Phase 2: Real failures - SHOULD trip circuit after enough calls
        inner.PublishAsync(subject, message, Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(x => throw new TimeoutException("Network timeout"));

        // Make enough calls to trip the circuit (5 calls should be plenty)
        // Catch any exception since circuit may throw BrokenCircuitException once tripped
        for (int i = 0; i < 15; i++)
        {
            try { await sut.PublishAsync(subject, message, $"msg-fail-{i}"); } 
            catch (Exception) { /* TimeoutException or BrokenCircuitException */ }
        }

        // Phase 3: Circuit should now be OPEN
        inner.ClearReceivedCalls();

        // Act & Assert - Should throw BrokenCircuitException, not TimeoutException
        await Assert.ThrowsAsync<BrokenCircuitException>(
            async () => await sut.PublishAsync(subject, message, "msg-after-trip"));

        // Inner was not called because circuit rejected the request
        await inner.DidNotReceive().PublishAsync(
            Arg.Any<string>(), 
            Arg.Any<string>(), 
            Arg.Any<string?>(), 
            Arg.Any<CancellationToken>());
    }

    #endregion
}



using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Resilience.Builders;
using FlySwattr.NATS.Resilience.Decorators;
using Microsoft.Extensions.Logging;
using NSubstitute;
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
}

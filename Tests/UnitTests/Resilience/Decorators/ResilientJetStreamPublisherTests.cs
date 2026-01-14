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
}

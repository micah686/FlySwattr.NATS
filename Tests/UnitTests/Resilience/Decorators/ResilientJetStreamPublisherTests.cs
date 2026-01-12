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
}

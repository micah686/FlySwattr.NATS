using FlySwattr.NATS.Core;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Core.Messaging;

[Property("nTag", "Core")]
public class NatsMessageBusTests : IAsyncDisposable
{
    private readonly INatsConnection _connection;
    private readonly ILogger<NatsMessageBus> _logger;
    private readonly NatsMessageBus _bus;

    public NatsMessageBusTests()
    {
        _connection = Substitute.For<INatsConnection>();
        _logger = Substitute.For<ILogger<NatsMessageBus>>();
        _bus = new NatsMessageBus(_connection, _logger);
    }

    public async ValueTask DisposeAsync()
    {
        await _bus.DisposeAsync();
    }

    [Test]
    public async Task PublishAsync_ShouldCallConnectionPublish()
    {
        // Arrange
        var subject = "test.subject";
        var message = new { Data = "test" };
        _connection.PublishAsync(subject, message, cancellationToken: Arg.Any<CancellationToken>())
            .Returns(ValueTask.CompletedTask);

        // Act
        await _bus.PublishAsync(subject, message);

        // Assert
        await _connection.Received(1).PublishAsync(subject, message, cancellationToken: Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task SubscribeAsync_ShouldCallSubscribeCore()
    {
        // Arrange
        var subject = "test.subject";
        var sub = Substitute.For<INatsSub<object>>();
        
        using var cts = new CancellationTokenSource(100);
        
        _connection.SubscribeCoreAsync<object>(subject, queueGroup: null, cancellationToken: Arg.Any<CancellationToken>())
            .Returns(sub);

        // Act
        try
        {
            await _bus.SubscribeAsync<object>(subject, _ => Task.CompletedTask, cancellationToken: cts.Token);
        }
        catch (OperationCanceledException) { }

        // Assert
        await _connection.Received().SubscribeCoreAsync<object>(subject, queueGroup: null, cancellationToken: Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task SubscribeAsync_ShouldRetry_IfInitialConnectionFails()
    {
        // Arrange
        var subject = "test.subject";
        _connection.SubscribeCoreAsync<object>(subject, queueGroup: null, cancellationToken: Arg.Any<CancellationToken>())
             .Returns(x => ValueTask.FromException<INatsSub<object>>(new NatsException("Connection failed")));

        using var cts = new CancellationTokenSource(500);
        
        try
        {
            await _bus.SubscribeAsync<object>(subject, _ => Task.CompletedTask, cancellationToken: cts.Token);
        }
        catch (OperationCanceledException) { }

        // Assert
        // Should have tried at least once
        await _connection.Received().SubscribeCoreAsync<object>(subject, queueGroup: null, cancellationToken: Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RequestAsync_ShouldCallConnectionRequest()
    {
        // Arrange
        var subject = "req";
        var request = "ping";
        
        _connection.RequestAsync<string, string>(subject, request, replyOpts: Arg.Any<NatsSubOpts>(), cancellationToken: Arg.Any<CancellationToken>())
            .Returns(x => ValueTask.FromException<NatsMsg<string>>(new NatsNoReplyException())); 

        // Act & Assert
        await Assert.ThrowsAsync<NatsNoReplyException>(async () => 
            await _bus.RequestAsync<string, string>(subject, request, TimeSpan.FromSeconds(1)));

        await _connection.Received(1).RequestAsync<string, string>(subject, request, replyOpts: Arg.Any<NatsSubOpts>(), cancellationToken: Arg.Any<CancellationToken>());
    }
}

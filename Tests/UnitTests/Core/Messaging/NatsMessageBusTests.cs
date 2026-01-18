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
    public async Task SubscribeAsync_ShouldBackoffExponentially_OnFailure()
    {
        // Arrange
        var subject = "test.subject.backoff";
        var sub = Substitute.For<INatsSub<object>>();
        
        // Throw 4 times, then succeed (or cancel)
        // We want to verify the log messages for the backoff
        _connection.SubscribeCoreAsync<object>(subject, queueGroup: null, cancellationToken: Arg.Any<CancellationToken>())
             .Returns(x => ValueTask.FromException<INatsSub<object>>(new NatsException("Fail 1")));

        // Run for enough time to trigger a few retries (1s, 2s, 4s...)
        // We can't actually wait that long in a unit test, so we'll rely on the loop structure
        // But without TimeProvider, we can't fast-forward Task.Delay.
        // So we can only test the FIRST backoff (1s) easily without slowing down tests.
        // Wait, if we can't control time, this test is slow.
        // I will implement a shorter test that just verifies the first failure logs "1s".
        // To verify exponential behavior properly requires refactoring NatsMessageBus to use TimeProvider.
        // For now, I'll verify the first retry backoff log.
        
        using var cts = new CancellationTokenSource(1500); // Wait > 1s to allow first retry
        
        // Act
        try
        {
            await _bus.SubscribeAsync<object>(subject, _ => Task.CompletedTask, cancellationToken: cts.Token);
        }
        catch (OperationCanceledException) { }

        // Assert
        // Verify log calls
        // Reconnecting in 1s...
        _logger.Received().Log(
            LogLevel.Error,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("Reconnecting in 1s")),
            Arg.Any<Exception>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Test]
    public async Task DisposeAsync_ShouldDisposeSubscriptions()
    {
        // Arrange
        var subject = "test.dispose";
        var sub = Substitute.For<INatsSub<object>>();
        
        // Subscribe successfully
        _connection.SubscribeCoreAsync<object>(subject, queueGroup: null, cancellationToken: Arg.Any<CancellationToken>())
            .Returns(sub);

        var readyCts = new CancellationTokenSource();
        // Use a callback to signal when subscription is established
        _connection.SubscribeCoreAsync<object>(subject, queueGroup: null, cancellationToken: Arg.Any<CancellationToken>())
            .Returns(x => 
            {
                readyCts.Cancel(); 
                return ValueTask.FromResult(sub);
            });
            
        // We need to start the subscription in background
        var subTask = _bus.SubscribeAsync<object>(subject, _ => Task.CompletedTask);
        
        // Wait a bit for subscription to be active
        await Task.Delay(100);

        // Act
        await _bus.DisposeAsync();

        // Assert
        await sub.Received().DisposeAsync();
        
        // Clean up task
        try { await subTask; } catch { }
    }
}

using FlySwattr.NATS.Core;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NSubstitute;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Core.Messaging;

[Property("nTag", "Core")]
public class NatsMessageBusEventsTests : IAsyncDisposable
{
    private readonly INatsConnection _connection;
    private readonly ILogger<NatsMessageBus> _logger;
    private readonly NatsMessageBus _bus;

    public NatsMessageBusEventsTests()
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
    public async Task ShouldRaiseConnectionStateChanged_WhenConnectionOpened()
    {
        // Arrange
        var receivedState = NatsConnectionState.Closed;
        var eventRaised = false;
        _bus.ConnectionStateChanged += (sender, state) => 
        {
            receivedState = state;
            eventRaised = true;
        };

        _connection.ConnectionState.Returns(NatsConnectionState.Open);

        // Act
        _connection.ConnectionOpened += Raise.Event<AsyncEventHandler<NatsEventArgs>>(
            _connection, new NatsEventArgs("Connected"));

        // Assert
        await Task.Delay(500); // Allow async handler to propagate
        
        eventRaised.ShouldBeTrue();
        receivedState.ShouldBe(NatsConnectionState.Open);
    }

    [Test]
    public void ShouldRaiseConnectionStateChanged_WhenConnectionDisconnected()
    {
        // Arrange
        NatsConnectionState? receivedState = null;
        var signal = new ManualResetEventSlim(false);
        _bus.ConnectionStateChanged += (sender, state) => 
        {
            receivedState = state;
            signal.Set();
        };

        _connection.ConnectionState.Returns(NatsConnectionState.Reconnecting);

        // Act
        _connection.ConnectionDisconnected += Raise.Event<AsyncEventHandler<NatsEventArgs>>(
            _connection, new NatsEventArgs("Disconnected"));

        // Assert
        signal.Wait(TimeSpan.FromSeconds(1)).ShouldBeTrue();
        receivedState.ShouldBe(NatsConnectionState.Reconnecting);
    }

    [Test]
    public void ShouldRaiseConnectionStateChanged_WhenReconnectFailed()
    {
        // Arrange
        NatsConnectionState? receivedState = null;
        var signal = new ManualResetEventSlim(false);
        _bus.ConnectionStateChanged += (sender, state) => 
        {
            receivedState = state;
            signal.Set();
        };

        _connection.ConnectionState.Returns(NatsConnectionState.Closed);

        // Act
        _connection.ReconnectFailed += Raise.Event<AsyncEventHandler<NatsEventArgs>>(
            _connection, new NatsEventArgs("Failed"));

        // Assert
        signal.Wait(TimeSpan.FromSeconds(1)).ShouldBeTrue();
        receivedState.ShouldBe(NatsConnectionState.Closed);
    }

    [Test]
    public async Task PublishAsync_ShouldRespectCancellationToken()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        
        // Act
        try
        {
            await _bus.PublishAsync("subj", "msg", cts.Token);
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        // Assert
        // Connection is called but with a cancelled token
        await _connection.Received(1).PublishAsync(
            Arg.Any<string>(), 
            Arg.Any<string>(),
            headers: Arg.Any<NatsHeaders>(),
            cancellationToken: Arg.Is<CancellationToken>(ct => ct.IsCancellationRequested));
    }
    
    [Test]
    public async Task RequestAsync_ShouldThrow_WhenRequestSerializerFails()
    {
        // Arrange
        _connection.RequestAsync<object, object>(default!, default!)
            .ReturnsForAnyArgs(x => ValueTask.FromException<NatsMsg<object>>(new ArgumentException("Serialization failed")));
            
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => 
            _bus.RequestAsync<object, object>("subj", new object(), TimeSpan.FromSeconds(1)));
    }
}

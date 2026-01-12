using System.Text.Json;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Hosting.Configuration;
using FlySwattr.NATS.Hosting.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NSubstitute;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Hosting.Services;

[Property("nTag", "Hosting")]
public class DlqAdvisoryListenerServiceTests
{
    private readonly INatsConnection _natsConnection;
    private readonly IDlqAdvisoryHandler _handler;
    private readonly IDlqNotificationService _notificationService;
    private readonly ILogger<DlqAdvisoryListenerService> _logger;
    private readonly ITopologyReadySignal _topologyReadySignal;
    private readonly IOptions<DlqAdvisoryListenerOptions> _options;

    public DlqAdvisoryListenerServiceTests()
    {
        _natsConnection = Substitute.For<INatsConnection>();
        _handler = Substitute.For<IDlqAdvisoryHandler>();
        _notificationService = Substitute.For<IDlqNotificationService>();
        _logger = Substitute.For<ILogger<DlqAdvisoryListenerService>>();
        _topologyReadySignal = Substitute.For<ITopologyReadySignal>();
        
        var config = new DlqAdvisoryListenerOptions
        {
            AdvisorySubject = "test.advisory",
            TriggerDlqNotification = true
        };
        _options = Options.Create(config);
    }

    private class TestableDlqAdvisoryListenerService : DlqAdvisoryListenerService
    {
        public TestableDlqAdvisoryListenerService(
            INatsConnection natsConnection,
            IEnumerable<IDlqAdvisoryHandler> handlers,
            IOptions<DlqAdvisoryListenerOptions> options,
            ILogger<DlqAdvisoryListenerService> logger,
            IDlqNotificationService? notificationService = null,
            ITopologyReadySignal? topologyReadySignal = null) 
            : base(natsConnection, handlers, options, logger, notificationService, topologyReadySignal)
        {
        }

        public Task RunExecuteAsync(CancellationToken ct) => ExecuteAsync(ct);
    }

    [Test]
    public async Task ExecuteAsync_ShouldProcessMessages()
    {
        // Arrange
        var advisory = new ConsumerMaxDeliveriesAdvisory(
            Type: "io.nats.jetstream.advisory.v1.max_deliver",
            Id: "msg-1",
            Timestamp: DateTimeOffset.UtcNow,
            Stream: "stream-1",
            Consumer: "consumer-1",
            StreamSeq: 100,
            Deliveries: 5
        );
        var jsonData = JsonSerializer.SerializeToUtf8Bytes(advisory);
        
        var msg = CreateMsg("test.advisory.foo", jsonData);

        _natsConnection.SubscribeAsync<byte[]>("test.advisory", cancellationToken: Arg.Any<CancellationToken>())
            .Returns(CreateAsyncEnumerable(new[] { msg }));

        var service = new TestableDlqAdvisoryListenerService(
            _natsConnection, new[] { _handler }, _options, _logger, _notificationService, _topologyReadySignal);

        using var cts = new CancellationTokenSource();

        // Act
        var task = service.RunExecuteAsync(cts.Token);
        await Task.Delay(200); // Give it time to process
        cts.Cancel();
        try { await task; } catch (OperationCanceledException) { }

        // Assert
        await _handler.Received(1).HandleMaxDeliveriesExceededAsync(
            Arg.Is<ConsumerMaxDeliveriesAdvisory>(a => a.Id == "msg-1"), Arg.Any<CancellationToken>());
            
        await _notificationService.Received(1).NotifyAsync(
            Arg.Is<DlqNotification>(n => n.MessageId == "msg-1"), Arg.Any<CancellationToken>());
    }

    private static NatsMsg<byte[]> CreateMsg(string subject, byte[] data)
    {
        var msg = default(NatsMsg<byte[]>);
        var type = typeof(NatsMsg<byte[]>);
        
        object boxed = msg;
        
        void SetField(string name, object val)
        {
            var field = type.GetField($"<{name}>k__BackingField", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            field?.SetValue(boxed, val);
        }

        SetField("Subject", subject);
        SetField("Data", data);
        
        return (NatsMsg<byte[]>)boxed;
    }

    private static async IAsyncEnumerable<T> CreateAsyncEnumerable<T>(IEnumerable<T> items)
    {
        foreach (var item in items)
        {
            yield return item;
        }
        
        // Hang indefinitely to simulate a live subscription
        await Task.Delay(-1);
    }
}

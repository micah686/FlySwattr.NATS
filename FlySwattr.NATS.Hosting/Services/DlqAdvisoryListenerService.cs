using System.Text.Json;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Hosting.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.Core;

namespace FlySwattr.NATS.Hosting.Services;

/// <summary>
/// Background service that monitors NATS JetStream advisory events for consumer delivery failures.
/// Subscribes to $JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.> to detect "poison messages"
/// and "stale consumers" that exceed their MaxDeliver limit.
/// </summary>
/// <remarks>
/// This service provides the "Control Plane" active monitoring capability, giving operations teams
/// visibility into server-side delivery failures that would otherwise go unnoticed.
/// </remarks>
public partial class DlqAdvisoryListenerService : BackgroundService
{
    private readonly INatsConnection _natsConnection;
    private readonly IEnumerable<IDlqAdvisoryHandler> _handlers;
    private readonly IDlqNotificationService? _notificationService;
    private readonly DlqAdvisoryListenerOptions _options;
    private readonly ILogger<DlqAdvisoryListenerService> _logger;
    private readonly ITopologyReadySignal? _topologyReadySignal;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public DlqAdvisoryListenerService(
        INatsConnection natsConnection,
        IEnumerable<IDlqAdvisoryHandler> handlers,
        IOptions<DlqAdvisoryListenerOptions> options,
        ILogger<DlqAdvisoryListenerService> logger,
        IDlqNotificationService? notificationService = null,
        ITopologyReadySignal? topologyReadySignal = null)
    {
        _natsConnection = natsConnection ?? throw new ArgumentNullException(nameof(natsConnection));
        _handlers = handlers ?? throw new ArgumentNullException(nameof(handlers));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _notificationService = notificationService;
        _topologyReadySignal = topologyReadySignal;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Wait for topology to be ready before subscribing (Safety Mode)
        if (_topologyReadySignal != null)
        {
            try
            {
                LogWaitingForTopology();
                await _topologyReadySignal.WaitAsync(stoppingToken);
                LogTopologyReady();
            }
            catch (OperationCanceledException)
            {
                LogTopologyWaitCanceled();
                return;
            }
            catch (Exception ex)
            {
                LogTopologyFailed(ex);
                return;
            }
        }

        LogStarting(_options.AdvisorySubject);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await SubscribeAndProcessAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // Graceful shutdown
                break;
            }
            catch (Exception ex)
            {
                LogSubscriptionError(ex);
                
                try
                {
                    await Task.Delay(_options.ReconnectDelay, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }

        LogStopping();
    }

    private async Task SubscribeAndProcessAsync(CancellationToken stoppingToken)
    {
        await foreach (var msg in _natsConnection.SubscribeAsync<byte[]>(_options.AdvisorySubject, cancellationToken: stoppingToken))
        {
            try
            {
                await ProcessAdvisoryMessageAsync(msg, stoppingToken);
            }
            catch (Exception ex)
            {
                LogProcessingError(msg.Subject, ex);
            }
        }
    }

    private async Task ProcessAdvisoryMessageAsync(NatsMsg<byte[]> msg, CancellationToken cancellationToken)
    {
        if (msg.Data == null || msg.Data.Length == 0)
        {
            LogEmptyPayload(msg.Subject);
            return;
        }

        ConsumerMaxDeliveriesAdvisory? advisory;
        try
        {
            advisory = JsonSerializer.Deserialize<ConsumerMaxDeliveriesAdvisory>(msg.Data, JsonOptions);
        }
        catch (JsonException ex)
        {
            LogDeserializationError(msg.Subject, ex);
            return;
        }

        if (advisory == null)
        {
            LogNullAdvisory(msg.Subject);
            return;
        }

        // Apply filters
        if (!ShouldProcessAdvisory(advisory))
        {
            LogFilteredOut(advisory.Stream, advisory.Consumer);
            return;
        }

        LogAdvisoryReceived(advisory.Stream, advisory.Consumer, advisory.StreamSeq, advisory.Deliveries);

        // Invoke all registered handlers
        var handlerTasks = new List<Task>();
        foreach (var handler in _handlers)
        {
            handlerTasks.Add(InvokeHandlerSafelyAsync(handler, advisory, cancellationToken));
        }

        // Optionally trigger the DLQ notification service for backward compatibility
        if (_options.TriggerDlqNotification && _notificationService != null)
        {
            handlerTasks.Add(SendDlqNotificationAsync(advisory, cancellationToken));
        }

        await Task.WhenAll(handlerTasks);
    }

    private bool ShouldProcessAdvisory(ConsumerMaxDeliveriesAdvisory advisory)
    {
        if (_options.StreamFilter.Count > 0 && !_options.StreamFilter.Contains(advisory.Stream))
        {
            return false;
        }

        if (_options.ConsumerFilter.Count > 0 && !_options.ConsumerFilter.Contains(advisory.Consumer))
        {
            return false;
        }

        return true;
    }

    private async Task InvokeHandlerSafelyAsync(
        IDlqAdvisoryHandler handler,
        ConsumerMaxDeliveriesAdvisory advisory,
        CancellationToken cancellationToken)
    {
        try
        {
            await handler.HandleMaxDeliveriesExceededAsync(advisory, cancellationToken);
        }
        catch (Exception ex)
        {
            LogHandlerError(handler.GetType().Name, advisory.Stream, advisory.Consumer, ex);
        }
    }

    private async Task SendDlqNotificationAsync(
        ConsumerMaxDeliveriesAdvisory advisory,
        CancellationToken cancellationToken)
    {
        try
        {
            var notification = new DlqNotification(
                MessageId: advisory.Id,
                OriginalStream: advisory.Stream,
                OriginalConsumer: advisory.Consumer,
                OriginalSubject: $"{advisory.Stream}.{advisory.Consumer}", // Approximate subject
                OriginalSequence: advisory.StreamSeq,
                DeliveryCount: advisory.Deliveries,
                ErrorReason: "MaxDeliver limit exceeded (server-side advisory)",
                OccurredAt: advisory.Timestamp
            );

            await _notificationService!.NotifyAsync(notification, cancellationToken);
        }
        catch (Exception ex)
        {
            LogNotificationError(advisory.Stream, advisory.Consumer, ex);
        }
    }

    // Source-generated logging for zero-allocation
    [LoggerMessage(Level = LogLevel.Information, Message = "DLQ Advisory Listener starting, subscribing to {Subject}")]
    private partial void LogStarting(string subject);

    [LoggerMessage(Level = LogLevel.Information, Message = "DLQ Advisory Listener stopping")]
    private partial void LogStopping();

    [LoggerMessage(Level = LogLevel.Error, Message = "Subscription error in DLQ Advisory Listener, will retry")]
    private partial void LogSubscriptionError(Exception exception);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Received advisory: Stream={Stream}, Consumer={Consumer}, StreamSeq={StreamSeq}, Deliveries={Deliveries}")]
    private partial void LogAdvisoryReceived(string stream, string consumer, ulong streamSeq, int deliveries);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Empty advisory payload received on {Subject}")]
    private partial void LogEmptyPayload(string subject);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to deserialize advisory from {Subject}")]
    private partial void LogDeserializationError(string subject, Exception exception);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Null advisory after deserialization from {Subject}")]
    private partial void LogNullAdvisory(string subject);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Advisory filtered out: Stream={Stream}, Consumer={Consumer}")]
    private partial void LogFilteredOut(string stream, string consumer);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error processing advisory message from {Subject}")]
    private partial void LogProcessingError(string subject, Exception exception);

    [LoggerMessage(Level = LogLevel.Error, Message = "Handler {HandlerName} failed for advisory: Stream={Stream}, Consumer={Consumer}")]
    private partial void LogHandlerError(string handlerName, string stream, string consumer, Exception exception);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to send DLQ notification for advisory: Stream={Stream}, Consumer={Consumer}")]
    private partial void LogNotificationError(string stream, string consumer, Exception exception);

    [LoggerMessage(Level = LogLevel.Information, Message = "DLQ Advisory Listener waiting for topology ready signal...")]
    private partial void LogWaitingForTopology();

    [LoggerMessage(Level = LogLevel.Information, Message = "DLQ Advisory Listener received topology ready signal.")]
    private partial void LogTopologyReady();

    [LoggerMessage(Level = LogLevel.Warning, Message = "DLQ Advisory Listener topology wait was canceled during shutdown.")]
    private partial void LogTopologyWaitCanceled();

    [LoggerMessage(Level = LogLevel.Error, Message = "DLQ Advisory Listener cannot start: topology provisioning failed.")]
    private partial void LogTopologyFailed(Exception exception);
}

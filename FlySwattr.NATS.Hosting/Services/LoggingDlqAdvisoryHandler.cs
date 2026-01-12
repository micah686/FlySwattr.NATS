using FlySwattr.NATS.Abstractions;
using Microsoft.Extensions.Logging;

namespace FlySwattr.NATS.Hosting.Services;

/// <summary>
/// Default implementation of <see cref="IDlqAdvisoryHandler"/> that logs advisory events.
/// </summary>
public partial class LoggingDlqAdvisoryHandler : IDlqAdvisoryHandler
{
    private readonly ILogger<LoggingDlqAdvisoryHandler> _logger;

    public LoggingDlqAdvisoryHandler(ILogger<LoggingDlqAdvisoryHandler> logger)
    {
        _logger = logger;
    }

    /// <inheritdoc />
    public Task HandleMaxDeliveriesExceededAsync(
        ConsumerMaxDeliveriesAdvisory advisory,
        CancellationToken cancellationToken = default)
    {
        LogMaxDeliveriesExceeded(
            advisory.Stream,
            advisory.Consumer,
            advisory.StreamSeq,
            advisory.Deliveries,
            advisory.Domain);
        
        return Task.CompletedTask;
    }

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "Consumer MAX_DELIVERIES exceeded: Stream={Stream}, Consumer={Consumer}, " +
                  "StreamSeq={StreamSeq}, Deliveries={Deliveries}, Domain={Domain}")]
    private partial void LogMaxDeliveriesExceeded(
        string stream, 
        string consumer, 
        ulong streamSeq, 
        int deliveries,
        string? domain);
}

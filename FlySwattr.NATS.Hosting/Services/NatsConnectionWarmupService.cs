using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;

namespace FlySwattr.NATS.Hosting.Services;

/// <summary>
/// Eagerly connects to NATS on application startup to avoid first-publish latency.
/// </summary>
/// <remarks>
/// <para><b>Failure behaviour:</b> If the NATS server is unreachable within
/// <c>connectTimeout</c> (default 30 s), the warmup exception is <em>logged but not
/// re-thrown</em>.  The application continues to start, and <see cref="NatsConnection"/>
/// will attempt a lazy first-use connection on the first publish or subscribe call.</para>
/// <para>This means startup does not hard-fail on a transient NATS outage, but the first
/// message published or consumed after startup may incur extra latency while the connection
/// is established, and early messages may fail if the server remains unreachable.</para>
/// <para>If you need a hard startup guarantee (fail-fast on no NATS), throw inside the
/// <c>catch</c> block here, or use
/// <see cref="FlySwattr.NATS.Hosting.Services.ConfiguredNatsConsumerHostedService"/>
/// which enforces connection at consumer startup instead.</para>
/// </remarks>
public sealed partial class NatsConnectionWarmupService : IHostedService
{
    private readonly NatsConnection _connection;
    private readonly ILogger<NatsConnectionWarmupService> _logger;
    private readonly TimeSpan _connectTimeout;

    public NatsConnectionWarmupService(
        NatsConnection connection,
        ILogger<NatsConnectionWarmupService> logger,
        TimeSpan? connectTimeout = null)
    {
        _connection = connection;
        _logger = logger;
        _connectTimeout = connectTimeout ?? TimeSpan.FromSeconds(30);
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        LogWarmupStarting();
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(_connectTimeout);
        try
        {
            await _connection.ConnectAsync().AsTask().WaitAsync(cts.Token);
            LogWarmupComplete(_connection.ConnectionState.ToString());
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw; // App shutting down
        }
        catch (Exception ex)
        {
            LogWarmupFailed(ex);
            // Don't throw — allow the app to start and connect lazily
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    [LoggerMessage(Level = LogLevel.Information, Message = "Warming up NATS connection...")]
    private partial void LogWarmupStarting();

    [LoggerMessage(Level = LogLevel.Information, Message = "NATS connection established. State: {State}")]
    private partial void LogWarmupComplete(string state);

    [LoggerMessage(Level = LogLevel.Warning, Message = "NATS connection warmup failed. Connection will be retried lazily.")]
    private partial void LogWarmupFailed(Exception exception);
}

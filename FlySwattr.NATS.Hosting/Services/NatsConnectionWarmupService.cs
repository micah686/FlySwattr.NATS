using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;

namespace FlySwattr.NATS.Hosting.Services;

/// <summary>
/// Eagerly connects to NATS on application startup to avoid first-publish latency.
/// Non-fatal: if connection fails, the app starts normally and connects lazily.
/// </summary>
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
            await _connection.ConnectAsync();
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

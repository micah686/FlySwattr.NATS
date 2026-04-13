using FlySwattr.NATS.Abstractions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace FlySwattr.NATS.Hosting.Services;

/// <summary>
/// Background service that periodically sweeps the claim-check Object Store bucket
/// and deletes objects older than the configured TTL. This is Layer 3 of the
/// three-layer cleanup strategy — a monitoring/safety net for environments where
/// Object Store MaxAge isn't supported or proactive monitoring is desired.
/// </summary>
public sealed partial class ClaimCheckCleanupService : BackgroundService
{
    private readonly IObjectStore _objectStore;
    private readonly ILogger<ClaimCheckCleanupService> _logger;
    private readonly TimeSpan _ttl;
    private readonly TimeSpan _sweepInterval;

    public ClaimCheckCleanupService(
        IObjectStore objectStore,
        ILogger<ClaimCheckCleanupService> logger,
        TimeSpan ttl,
        TimeSpan sweepInterval)
    {
        _objectStore = objectStore;
        _logger = logger;
        _ttl = ttl;
        _sweepInterval = sweepInterval;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        LogStarted(_sweepInterval, _ttl);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_sweepInterval, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            try
            {
                await SweepAsync(stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                LogSweepFailed(ex);
            }
        }
    }

    private async Task SweepAsync(CancellationToken cancellationToken)
    {
        var objects = await _objectStore.ListAsync(cancellationToken: cancellationToken);
        var cutoff = DateTimeOffset.UtcNow - _ttl;
        var deletedCount = 0;
        var totalCount = 0;

        foreach (var obj in objects)
        {
            totalCount++;

            if (obj.LastModified < cutoff)
            {
                try
                {
                    await _objectStore.DeleteAsync(obj.Key, cancellationToken);
                    deletedCount++;
                }
                catch (Exception ex)
                {
                    LogObjectDeleteFailed(obj.Key, ex);
                }
            }
        }

        LogSweepComplete(deletedCount, totalCount);
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Claim-check cleanup service started. Sweep interval: {SweepInterval}, TTL: {Ttl}")]
    private partial void LogStarted(TimeSpan sweepInterval, TimeSpan ttl);

    [LoggerMessage(Level = LogLevel.Information, Message = "Claim-check sweep complete. Deleted {DeletedCount} expired object(s) out of {TotalCount} total.")]
    private partial void LogSweepComplete(int deletedCount, int totalCount);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to delete expired claim-check object {ObjectKey}")]
    private partial void LogObjectDeleteFailed(string objectKey, Exception exception);

    [LoggerMessage(Level = LogLevel.Error, Message = "Claim-check sweep failed")]
    private partial void LogSweepFailed(Exception exception);
}

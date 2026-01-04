using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;

namespace FlySwattr.NATS.Hosting.Health;

public class NatsStartupCheck : IHostedService
{
    private readonly INatsConnection _connection;
    private readonly ILogger<NatsStartupCheck> _logger;

    public NatsStartupCheck(INatsConnection connection, ILogger<NatsStartupCheck> logger)
    {
        _connection = connection;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Performing NATS startup liveness check...");
        try
        {
            // Force connection and ping. This ensures the URL is reachable and auth works.
            await _connection.PingAsync(cancellationToken);
            _logger.LogInformation("NATS startup check passed.");
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "NATS startup check failed. Application will likely fail.");
            throw; // Fail startup
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

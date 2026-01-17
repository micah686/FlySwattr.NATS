using Microsoft.Extensions.Diagnostics.HealthChecks;
using NATS.Client.Core;
using NATS.Client.JetStream;

namespace FlySwattr.NATS.Hosting.Health;

internal class NatsHealthCheck : IHealthCheck
{
    private readonly INatsConnection _connection;
    private readonly INatsJSContext _jsContext;

    public NatsHealthCheck(INatsConnection connection, INatsJSContext jsContext)
    {
        _connection = connection;
        _jsContext = jsContext;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        if (_connection.ConnectionState != NatsConnectionState.Open)
        {
            return HealthCheckResult.Unhealthy($"NATS connection state is {_connection.ConnectionState}.");
        }

        try
        {
            await _jsContext.GetAccountInfoAsync(cancellationToken);

            // Fix MED-3: Removed hardcoded stream check.
            // Verification of JetStream availability via GetAccountInfoAsync is sufficient for liveness.
            // Existence of specific streams is a deployment concern.

            return HealthCheckResult.Healthy("NATS connection is open and JetStream is responding.");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Degraded($"NATS connection is open but JetStream is unavailable: {ex.Message}");
        }
    }
}

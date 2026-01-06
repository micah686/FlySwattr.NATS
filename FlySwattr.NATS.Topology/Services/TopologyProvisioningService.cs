using FlySwattr.NATS.Abstractions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace FlySwattr.NATS.Topology.Services;

/// <summary>
/// An <see cref="IHostedService"/> that provisions NATS topology (streams and consumers) on application startup.
/// Collects specifications from all registered <see cref="ITopologySource"/> implementations and ensures they exist.
/// </summary>
public class TopologyProvisioningService : IHostedService
{
    private readonly IEnumerable<ITopologySource> _topologySources;
    private readonly ITopologyManager _topologyManager;
    private readonly ILogger<TopologyProvisioningService> _logger;

    public TopologyProvisioningService(
        IEnumerable<ITopologySource> topologySources,
        ITopologyManager topologyManager,
        ILogger<TopologyProvisioningService> logger)
    {
        _topologySources = topologySources;
        _topologyManager = topologyManager;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting topology provisioning...");

        var sources = _topologySources.ToList();
        if (sources.Count == 0)
        {
            _logger.LogWarning("No ITopologySource implementations registered. No topology will be provisioned.");
            return;
        }

        _logger.LogInformation("Found {Count} topology source(s).", sources.Count);

        // Collect all specs
        var allStreams = sources.SelectMany(s => s.GetStreams()).ToList();
        var allConsumers = sources.SelectMany(s => s.GetConsumers()).ToList();

        _logger.LogInformation("Provisioning {StreamCount} stream(s) and {ConsumerCount} consumer(s)...",
            allStreams.Count, allConsumers.Count);

        // Provision streams first (consumers depend on streams)
        foreach (var streamSpec in allStreams)
        {
            try
            {
                await _topologyManager.EnsureStreamAsync(streamSpec, cancellationToken);
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                _logger.LogError(ex, "Failed to provision stream {StreamName}. Continuing with remaining topology.",
                    streamSpec.Name);
                // Continue provisioning other streams - don't fail startup for a single stream failure
            }
        }

        // Provision consumers
        foreach (var consumerSpec in allConsumers)
        {
            try
            {
                await _topologyManager.EnsureConsumerAsync(consumerSpec, cancellationToken);
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                _logger.LogError(ex, "Failed to provision consumer {ConsumerName} on stream {StreamName}. Continuing with remaining topology.",
                    consumerSpec.DurableName, consumerSpec.StreamName);
                // Continue provisioning other consumers
            }
        }

        _logger.LogInformation("Topology provisioning completed.");
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

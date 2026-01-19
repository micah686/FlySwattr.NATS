using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Topology.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using Polly;
using Polly.Retry;

namespace FlySwattr.NATS.Topology.Services;

/// <summary>
/// An <see cref="IHostedService"/> that provisions NATS topology (streams, consumers, buckets, and object stores) on application startup.
/// Collects specifications from all registered <see cref="ITopologySource"/> implementations and ensures they exist.
///
/// Implements a "Cold Start" protection pattern using a Polly retry policy to wait for NATS connection
/// to become available before attempting JetStream management operations. This prevents application
/// crash loops during infrastructure instability (e.g., Kubernetes sidecar startup delays).
///
/// Automatically provisions "batteries included" infrastructure:
/// - DLQ streams are auto-derived from consumer DeadLetterPolicy definitions
/// - The fs-dlq-entries KV bucket is auto-created when any consumer has a DeadLetterPolicy
/// - The payload offloading object store is auto-created when enabled
///
/// Signals <see cref="ITopologyReadySignal"/> when provisioning completes to coordinate dependent services.
/// </summary>
internal class TopologyProvisioningService : IHostedService
{
    private readonly IEnumerable<ITopologySource> _topologySources;
    private readonly ITopologyManager _topologyManager;
    private readonly INatsConnection _connection;
    private readonly ITopologyReadySignal? _readySignal;
    private readonly ILogger<TopologyProvisioningService> _logger;
    private readonly TopologyStartupOptions _options;

    public TopologyProvisioningService(
        IEnumerable<ITopologySource> topologySources,
        ITopologyManager topologyManager,
        INatsConnection connection,
        ILogger<TopologyProvisioningService> logger,
        IOptions<TopologyStartupOptions> options,
        ITopologyReadySignal? readySignal = null)
    {
        _topologySources = topologySources;
        _topologyManager = topologyManager;
        _connection = connection;
        _logger = logger;
        _options = options.Value;
        _readySignal = readySignal;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting topology provisioning...");

        try
        {
            // Step 1: Wait for NATS connection to be established (Cold Start protection)
            await WaitForConnectionAsync(cancellationToken);

            var sources = _topologySources.ToList();
            if (sources.Count == 0)
            {
                _logger.LogWarning("No ITopologySource implementations registered. No topology will be provisioned.");

                // Even with no topology sources, we may need to provision auto-infrastructure
                await ProvisionAutoInfrastructureAsync(hasConsumersWithDlq: false, cancellationToken);

                _readySignal?.SignalReady();
                return;
            }

            _logger.LogInformation("Found {Count} topology source(s).", sources.Count);

            // Collect all specs
            var allStreams = sources.SelectMany(s => s.GetStreams()).ToList();
            var allConsumers = sources.SelectMany(s => s.GetConsumers()).ToList();
            var allBuckets = sources.SelectMany(s => s.GetBuckets()).ToList();
            var allObjectStores = sources.SelectMany(s => s.GetObjectStores()).ToList();

            _logger.LogInformation(
                "Provisioning {StreamCount} stream(s), {ConsumerCount} consumer(s), {BucketCount} bucket(s), and {ObjectStoreCount} object store(s)...",
                allStreams.Count, allConsumers.Count, allBuckets.Count, allObjectStores.Count);

            // Check if any consumer has a DLQ policy
            var hasConsumersWithDlq = allConsumers.Any(c => c.DeadLetterPolicy != null);

            // Provision auto-infrastructure first (DLQ bucket, payload offloading bucket)
            await ProvisionAutoInfrastructureAsync(hasConsumersWithDlq, cancellationToken);

            // Provision KV buckets from topology sources
            foreach (var bucketSpec in allBuckets)
            {
                try
                {
                    await _topologyManager.EnsureBucketAsync(bucketSpec, cancellationToken);
                }
                catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
                {
                    _logger.LogError(ex, "Failed to provision KV bucket {BucketName}. Continuing with remaining topology.",
                        bucketSpec.Name);
                }
            }

            // Provision Object Stores from topology sources
            foreach (var objectStoreSpec in allObjectStores)
            {
                try
                {
                    await _topologyManager.EnsureObjectStoreAsync(objectStoreSpec, cancellationToken);
                }
                catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
                {
                    _logger.LogError(ex, "Failed to provision Object Store {BucketName}. Continuing with remaining topology.",
                        objectStoreSpec.Name);
                }
            }

            // Provision streams (consumers depend on streams)
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

            // Provision consumers (DLQ streams are auto-created by EnsureConsumerAsync)
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

            // Signal dependent services that topology is ready
            _readySignal?.SignalReady();
        }
        catch (OperationCanceledException)
        {
            // Cancellation requested - don't signal failure, just propagate
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Topology provisioning failed with unrecoverable error.");
            _readySignal?.SignalFailed(ex);
            throw;
        }
    }

    /// <summary>
    /// Provisions "batteries included" auto-infrastructure like DLQ buckets and payload offloading buckets.
    /// </summary>
    private async Task ProvisionAutoInfrastructureAsync(bool hasConsumersWithDlq, CancellationToken cancellationToken)
    {
        // Auto-create DLQ KV bucket when any consumer has a DeadLetterPolicy
        if (hasConsumersWithDlq && _options.AutoCreateDlqBucket)
        {
            _logger.LogInformation("Auto-creating DLQ entries bucket '{BucketName}' (consumers with DeadLetterPolicy detected)...",
                _options.DlqBucketName);
            try
            {
                await _topologyManager.EnsureBucketAsync(
                    BucketName.From(_options.DlqBucketName),
                    StorageType.File,
                    cancellationToken);
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                _logger.LogError(ex, "Failed to auto-create DLQ bucket '{BucketName}'. DLQ functionality may be impaired.",
                    _options.DlqBucketName);
            }
        }

        // Auto-create payload offloading object store when configured
        if (_options.AutoCreatePayloadOffloadingBucket && !string.IsNullOrEmpty(_options.PayloadOffloadingBucketName))
        {
            _logger.LogInformation("Auto-creating payload offloading bucket '{BucketName}'...",
                _options.PayloadOffloadingBucketName);
            try
            {
                await _topologyManager.EnsureObjectStoreAsync(
                    BucketName.From(_options.PayloadOffloadingBucketName),
                    StorageType.File,
                    cancellationToken);
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                _logger.LogError(ex, "Failed to auto-create payload offloading bucket '{BucketName}'. Large payload handling may fail.",
                    _options.PayloadOffloadingBucketName);
            }
        }
    }

    /// <summary>
    /// Implements a connection guard that waits for NATS to become available using exponential backoff.
    /// This protects against "Cold Start" scenarios in container orchestration environments where
    /// sidecars (especially Istio, NATS) may not be immediately ready when the application starts.
    /// </summary>
    private async Task WaitForConnectionAsync(CancellationToken cancellationToken)
    {
        // If connection is already open, no need to wait
        if (_connection.ConnectionState == NatsConnectionState.Open)
        {
            _logger.LogDebug("NATS connection already open. Proceeding with topology provisioning.");
            return;
        }

        _logger.LogInformation(
            "NATS connection not yet established (state: {State}). Waiting for connection with exponential backoff...",
            _connection.ConnectionState);

        // Build retry pipeline with exponential backoff
        var pipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = _options.MaxRetryAttempts,
                Delay = _options.InitialRetryDelay,
                MaxDelay = _options.MaxRetryDelay,
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                ShouldHandle = new PredicateBuilder().Handle<Exception>(),
                OnRetry = args =>
                {
                    _logger.LogWarning(
                        "NATS connection attempt {AttemptNumber} failed (state: {State}). " +
                        "Retrying in {Delay}... Exception: {Exception}",
                        args.AttemptNumber + 1,
                        _connection.ConnectionState,
                        args.RetryDelay,
                        args.Outcome.Exception?.Message ?? "No response");
                    return ValueTask.CompletedTask;
                }
            })
            .Build();

        // Combine with global startup timeout if configured
        using var timeoutCts = _options.TotalStartupTimeout > TimeSpan.Zero
            ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
            : null;
        
        if (timeoutCts != null)
        {
            timeoutCts.CancelAfter(_options.TotalStartupTimeout);
        }

        var effectiveCt = timeoutCts?.Token ?? cancellationToken;

        try
        {
            await pipeline.ExecuteAsync(async ct =>
            {
                // Check connection state first
                if (_connection.ConnectionState == NatsConnectionState.Open)
                {
                    _logger.LogDebug("NATS connection is now open.");
                    return;
                }

                // Attempt to ping to verify connectivity and trigger reconnection if needed
                // NATS.Net v2 connection will attempt to connect on first use if not already connected
                using var pingCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                pingCts.CancelAfter(_options.ConnectionTimeout);

                try
                {
                    _logger.LogDebug("Attempting NATS ping to verify connectivity...");
                    await _connection.PingAsync(pingCts.Token);
                    _logger.LogInformation("NATS ping successful. Connection established.");
                }
                catch (OperationCanceledException) when (pingCts.IsCancellationRequested && !ct.IsCancellationRequested)
                {
                    // Ping timeout - throw generic exception to trigger retry
                    throw new TimeoutException($"NATS ping timed out after {_options.ConnectionTimeout}");
                }
            }, effectiveCt);
        }
        catch (OperationCanceledException) when (timeoutCts?.IsCancellationRequested == true && !cancellationToken.IsCancellationRequested)
        {
            // Global startup timeout exceeded
            var error = new TimeoutException(
                $"NATS connection could not be established within the startup timeout of {_options.TotalStartupTimeout}. " +
                $"Current connection state: {_connection.ConnectionState}. " +
                "This may indicate NATS is unreachable or the infrastructure is not yet ready.");
            throw error;
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

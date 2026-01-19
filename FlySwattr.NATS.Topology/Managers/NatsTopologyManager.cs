using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.KeyValueStore;
using NATS.Client.ObjectStore;

namespace FlySwattr.NATS.Topology.Managers;

internal class NatsTopologyManager : ITopologyManager
{
    private readonly INatsJSContext _jsContext;
    private readonly INatsKVContext _kvContext;
    private readonly INatsObjContext _objContext;
    private readonly ILogger<NatsTopologyManager> _logger;
    private readonly IDlqPolicyRegistry _dlqRegistry;
    private readonly Medallion.Threading.IDistributedLockProvider _lockProvider;

    public NatsTopologyManager(INatsJSContext jsContext, INatsKVContext kvContext, INatsObjContext objContext, ILogger<NatsTopologyManager> logger, IDlqPolicyRegistry dlqRegistry, Medallion.Threading.IDistributedLockProvider lockProvider)
    {
        _jsContext = jsContext;
        _kvContext = kvContext;
        _objContext = objContext;
        _logger = logger;
        _dlqRegistry = dlqRegistry;
        _lockProvider = lockProvider;
    }

    private async Task<T?> GetOrDefaultAsync<T>(Func<ValueTask<T>> getter) where T : class
    {
        try
        {
            return await getter();
        }
        catch (NatsJSApiException ex) when (ex.Error.Code == 404)
        {
            return null;
        }
    }

    private static bool IsResourceExistsError(NatsJSApiException ex) =>
        ex.Error.Code == 400 && 
        (ex.Error.ErrCode == 10058 /* stream already exists */ ||
         ex.Error.ErrCode == 10148 /* consumer already exists */ ||
         ex.Error.Description?.Contains("already", StringComparison.OrdinalIgnoreCase) == true);

    public async Task EnsureStreamAsync(StreamSpec spec, CancellationToken cancellationToken = default)
    {
        try
        {
            int maxMsgSize = spec.MaxMsgSize > int.MaxValue ? int.MaxValue : (int)spec.MaxMsgSize;
            if (spec.MaxMsgSize == -1) maxMsgSize = -1;

            var config = new StreamConfig(spec.Name.Value, spec.Subjects)
            {
                MaxBytes = spec.MaxBytes,
                MaxMsgSize = maxMsgSize,
                MaxAge = spec.MaxAge,
                Storage = spec.StorageType == StorageType.Memory ? StreamConfigStorage.Memory : StreamConfigStorage.File,
                Retention = spec.RetentionPolicy switch
                {
                    StreamRetention.Limits => StreamConfigRetention.Limits,
                    StreamRetention.Interest => StreamConfigRetention.Interest,
                    StreamRetention.WorkQueue => StreamConfigRetention.Workqueue,
                    _ => throw new ArgumentOutOfRangeException(nameof(spec.RetentionPolicy), spec.RetentionPolicy, "Unknown retention policy")
                },
                NumReplicas = spec.Replicas
            };

            _logger.LogInformation("Ensuring stream {Stream}...", spec.Name);

            var lockKey = $"topology_lock_stream_{spec.Name.Value}";
            await using var _ = await _lockProvider.CreateLock(lockKey).AcquireAsync(TimeSpan.FromSeconds(30), cancellationToken);

            var existingStream = await GetOrDefaultAsync(() => 
                _jsContext.GetStreamAsync(spec.Name.Value, cancellationToken: cancellationToken));

            if (existingStream == null)
            {
                await _jsContext.CreateStreamAsync(config, cancellationToken);
                _logger.LogInformation("Stream {Stream} created.", spec.Name);
            }
            else
            {
                if (StreamConfigChanged(existingStream.Info.Config, config))
                {
                    ValidateStreamUpdate(existingStream.Info.Config, config);

                    _logger.LogDebug("Stream {Stream} exists, updating configuration...", spec.Name);
                    await _jsContext.UpdateStreamAsync(config, cancellationToken);
                    _logger.LogInformation("Stream {Stream} updated.", spec.Name);
                }
                else
                {
                    _logger.LogDebug("Stream {Stream} exists and is up to date.", spec.Name);
                }
            }

            _logger.LogInformation("Stream {Stream} ready.", spec.Name);
        }
        catch (Exception ex)
        {
             _logger.LogError(ex, "Error creating stream {Stream}", spec.Name);
             throw;
        }
    }

    private void ValidateStreamUpdate(StreamConfig existing, StreamConfig desired)
    {
        if (existing.Storage != desired.Storage)
            throw new InvalidOperationException($"Storage type cannot be changed from {existing.Storage} to {desired.Storage}");
        
        if (existing.Retention != desired.Retention)
            throw new InvalidOperationException($"Retention policy cannot be changed from {existing.Retention} to {desired.Retention}");
        
        if (existing.NumReplicas != desired.NumReplicas)
            throw new InvalidOperationException($"Replica count cannot be changed from {existing.NumReplicas} to {desired.NumReplicas}");
        
        if (existing.Discard != desired.Discard)
            throw new InvalidOperationException($"Discard policy cannot be changed from {existing.Discard} to {desired.Discard}");
        
        // Allow changes to: MaxBytes, MaxAge, MaxMsgSize (with warning)
        if (existing.MaxBytes != desired.MaxBytes)
            _logger.LogWarning("Changing MaxBytes from {Old} to {New}", existing.MaxBytes, desired.MaxBytes);
    }

    private bool StreamConfigChanged(StreamConfig existing, StreamConfig desired)
    {
        if (existing.Storage != desired.Storage) return true;
        if (existing.Retention != desired.Retention) return true;
        if (existing.NumReplicas != desired.NumReplicas) return true;
        if (existing.Discard != desired.Discard) return true;
        if (existing.MaxBytes != desired.MaxBytes) return true;
        if (existing.MaxAge != desired.MaxAge) return true;
        if (existing.MaxMsgSize != desired.MaxMsgSize) return true;

        // Check subjects
        var existingSubjects = existing.Subjects ?? new List<string>();
        var desiredSubjects = desired.Subjects ?? new List<string>();
        
        if (existingSubjects.Count != desiredSubjects.Count) return true;
        
        // Use a simple set comparison
        var existingSet = new HashSet<string>(existingSubjects);
        return !existingSet.SetEquals(desiredSubjects);
    }

    private async Task<bool> StreamExistsAsync(string streamName, CancellationToken cancellationToken)
    {
        var stream = await GetOrDefaultAsync(() => _jsContext.GetStreamAsync(streamName, cancellationToken: cancellationToken));
        return stream != null;
    }

    public async Task EnsureConsumerAsync(ConsumerSpec spec, CancellationToken cancellationToken = default)
    {
        try
        {
            ICollection<TimeSpan>? backoff = null;
            if (spec.Backoff != null)
            {
                backoff = spec.Backoff;
            }

            var config = new ConsumerConfig
            {
                Name = spec.DurableName.Value,
                DurableName = spec.DurableName.Value,
                Description = spec.Description,
                FilterSubject = spec.FilterSubjects.Count > 0 ? null : spec.FilterSubject,
                FilterSubjects = spec.FilterSubjects.Count > 0 ? spec.FilterSubjects : null,
                AckPolicy = spec.AckPolicy switch
                {
                    AckPolicy.None => ConsumerConfigAckPolicy.None,
                    AckPolicy.All => ConsumerConfigAckPolicy.All,
                    AckPolicy.Explicit => ConsumerConfigAckPolicy.Explicit,
                    _ => throw new ArgumentOutOfRangeException(nameof(spec.AckPolicy), spec.AckPolicy, "Unknown ack policy")
                },
                AckWait = spec.AckWait,
                MaxDeliver = spec.MaxDeliver,
                DeliverPolicy = spec.DeliverPolicy switch
                {
                    DeliverPolicy.All => ConsumerConfigDeliverPolicy.All,
                    DeliverPolicy.Last => ConsumerConfigDeliverPolicy.Last,
                    DeliverPolicy.New => ConsumerConfigDeliverPolicy.New,
                    DeliverPolicy.ByStartSequence => ConsumerConfigDeliverPolicy.ByStartSequence,
                    DeliverPolicy.ByStartTime => ConsumerConfigDeliverPolicy.ByStartTime,
                    DeliverPolicy.LastPerSubject => ConsumerConfigDeliverPolicy.LastPerSubject,
                    _ => throw new ArgumentOutOfRangeException(nameof(spec.DeliverPolicy), spec.DeliverPolicy, "Unknown deliver policy")
                },
                Backoff = backoff,
            };

            _logger.LogInformation("Ensuring consumer {Stream}/{Consumer}...", spec.StreamName, spec.DurableName);

            var lockKey = $"topology_lock_consumer_{spec.StreamName.Value}_{spec.DurableName.Value}";
            await using var _ = await _lockProvider.CreateLock(lockKey).AcquireAsync(TimeSpan.FromSeconds(30), cancellationToken);

            try
            {
                await _jsContext.CreateOrUpdateConsumerAsync(spec.StreamName.Value, config, cancellationToken);
            }
            catch (NatsJSApiException ex) when (IsImmutablePropertyError(ex))
            {
                // Consumer exists but has immutable properties that differ from our config
                // Recreation strategy: delete and recreate the consumer
                _logger.LogWarning(
                    "Consumer {Stream}/{Consumer} has immutable property conflicts (ErrCode: {ErrCode}). Recreating consumer...",
                    spec.StreamName, spec.DurableName, ex.Error.ErrCode);
                
                await _jsContext.DeleteConsumerAsync(spec.StreamName.Value, spec.DurableName.Value, cancellationToken);
                await _jsContext.CreateOrUpdateConsumerAsync(spec.StreamName.Value, config, cancellationToken);
                
                _logger.LogInformation("Consumer {Stream}/{Consumer} recreated successfully.", spec.StreamName, spec.DurableName);
            }
            
            _logger.LogInformation("Consumer {Stream}/{Consumer} ready.", spec.StreamName, spec.DurableName);

            if (spec.DeadLetterPolicy != null)
            {
                await EnsureDlqInfrastructureAsync(spec.DeadLetterPolicy, cancellationToken);
                _dlqRegistry.Register(spec.StreamName.Value, spec.DurableName.Value, spec.DeadLetterPolicy);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating consumer {Consumer} on stream {Stream}", spec.DurableName, spec.StreamName);
            throw;
        }
    }
    
    /// <summary>
    /// Checks if the NATS error is due to attempting to modify immutable consumer properties.
    /// Error codes:
    /// - 10058: consumer name already in use (with different config)
    /// - 10148: consumer configuration cannot be updated (immutable properties)
    /// Also checks description for common immutable property error messages.
    /// </summary>
    private static bool IsImmutablePropertyError(NatsJSApiException ex)
    {
        if (ex.Error.ErrCode == 10058 || ex.Error.ErrCode == 10148)
            return true;

        // Check error description for immutable property messages
        var description = ex.Error.Description?.ToLowerInvariant() ?? string.Empty;
        return description.Contains("can not be updated") ||
               description.Contains("cannot be updated") ||
               description.Contains("immutable");
    }

    private async Task EnsureDlqInfrastructureAsync(DeadLetterPolicy policy, CancellationToken cancellationToken)
    {
        var dlqStreamName = policy.TargetStream.Value;
        var dlqSubject = policy.TargetSubject;

        var dlqStreamConfig = new StreamConfig(dlqStreamName, new[] { dlqSubject })
        {
            Storage = StreamConfigStorage.File,
            Retention = StreamConfigRetention.Limits,
            MaxAge = TimeSpan.FromDays(30)
        };

        _logger.LogInformation("Ensuring DLQ stream {Stream}...", dlqStreamName);

        // Use idempotent create-first approach to avoid race conditions
        try
        {
            await _jsContext.CreateStreamAsync(dlqStreamConfig, cancellationToken);
            _logger.LogInformation("DLQ Stream {Stream} created.", dlqStreamName);
        }
        catch (NatsJSApiException ex) when (IsResourceExistsError(ex))
        {
            // DLQ stream already exists, update it
            await _jsContext.UpdateStreamAsync(dlqStreamConfig, cancellationToken);
            _logger.LogInformation("DLQ Stream {Stream} updated.", dlqStreamName);
        }

        _logger.LogInformation("DLQ Stream {Stream} ready.", dlqStreamName);

        var consumerName = !string.IsNullOrEmpty(policy.SourceStream) && !string.IsNullOrEmpty(policy.SourceConsumer)
            ? $"dlq_{policy.SourceStream}_{policy.SourceConsumer}"
            : "dlq_stream_consumer";
        var consumerConfig = new ConsumerConfig
        {
            Name = consumerName,
            DurableName = consumerName,
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            DeliverPolicy = ConsumerConfigDeliverPolicy.All
        };

        _logger.LogInformation("Ensuring DLQ consumer {Stream}/{Consumer}...", dlqStreamName, consumerName);
        await _jsContext.CreateOrUpdateConsumerAsync(dlqStreamName, consumerConfig, cancellationToken);
        _logger.LogInformation("DLQ consumer {Stream}/{Consumer} ready.", dlqStreamName, consumerName);
    }

    public async Task EnsureBucketAsync(BucketName name, StorageType storageType, CancellationToken cancellationToken = default)
    {
        await EnsureBucketAsync(new BucketSpec
        {
            Name = name,
            StorageType = storageType
        }, cancellationToken);
    }

    public async Task EnsureBucketAsync(BucketSpec spec, CancellationToken cancellationToken = default)
    {
        try
        {
             var config = new NatsKVConfig(spec.Name.Value)
             {
                 Storage = spec.StorageType == StorageType.Memory ? NatsKVStorageType.Memory : NatsKVStorageType.File,
                 MaxBytes = spec.MaxBytes,
                 History = spec.History,
                 Description = spec.Description,
                 MaxAge = spec.MaxAge
             };

             var lockKey = $"topology_lock_kv_bucket_{spec.Name.Value}";
             await using var _ = await _lockProvider.CreateLock(lockKey).AcquireAsync(TimeSpan.FromSeconds(30), cancellationToken);

             // Create-first to avoid TOCTOU races during concurrent startups
             try
             {
                 await _kvContext.CreateStoreAsync(config, cancellationToken: cancellationToken);
                 _logger.LogInformation("KV Bucket {Bucket} created.", spec.Name);
             }
             catch (NatsJSApiException ex) when (IsResourceExistsError(ex))
             {
                 _logger.LogDebug("KV Bucket {Bucket} already exists.", spec.Name);
             }

             _logger.LogInformation("KV Bucket {Bucket} ready.", spec.Name);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating KV bucket {Bucket}", spec.Name);
            throw;
        }
    }

    public async Task EnsureObjectStoreAsync(BucketName name, StorageType storageType, CancellationToken cancellationToken = default)
    {
        await EnsureObjectStoreAsync(new ObjectStoreSpec
        {
            Name = name,
            StorageType = storageType
        }, cancellationToken);
    }

    public async Task EnsureObjectStoreAsync(ObjectStoreSpec spec, CancellationToken cancellationToken = default)
    {
        try
        {
             var config = new NatsObjConfig(spec.Name.Value)
             {
                 Storage = spec.StorageType == StorageType.Memory ? NatsObjStorageType.Memory : NatsObjStorageType.File,
                 MaxBytes = spec.MaxBytes,
                 Description = spec.Description
             };

             var lockKey = $"topology_lock_obj_store_{spec.Name.Value}";
             await using var _ = await _lockProvider.CreateLock(lockKey).AcquireAsync(TimeSpan.FromSeconds(30), cancellationToken);

             // Create-first to avoid TOCTOU races during concurrent startups
             try
             {
                 await _objContext.CreateObjectStoreAsync(config, cancellationToken: cancellationToken);
                 _logger.LogInformation("Object Store {Bucket} created.", spec.Name);
             }
             catch (NatsJSApiException ex) when (IsResourceExistsError(ex))
             {
                 _logger.LogDebug("Object Store {Bucket} already exists.", spec.Name);
             }

             _logger.LogInformation("Object Store {Bucket} ready.", spec.Name);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating Object Store {Bucket}", spec.Name);
            throw;
        }
    }
}

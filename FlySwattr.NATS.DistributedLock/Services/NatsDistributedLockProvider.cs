using Medallion.Threading;
using Microsoft.Extensions.Logging;
using global::NATS.Client.JetStream;
using global::NATS.Client.KeyValueStore;
using Polly;
using Polly.Retry;

namespace FlySwattr.NATS.DistributedLock.Services;

/// <summary>
/// NATS KV-backed distributed lock provider implementing DistributedLock.Core interfaces.
/// Uses Polly for retry with exponential backoff and jitter to prevent thundering herd.
/// </summary>
internal class NatsDistributedLockProvider : IDistributedLockProvider
{
    private readonly INatsKVContext _kvContext;
    private readonly ILogger<NatsDistributedLockProvider> _logger;
    private readonly TimeSpan _ttl;
    private const string BucketName = "topology_locks";
    private static readonly TimeSpan DefaultTtl = TimeSpan.FromMinutes(5);

    public NatsDistributedLockProvider(INatsKVContext kvContext, ILogger<NatsDistributedLockProvider> logger)
        : this(kvContext, logger, DefaultTtl)
    {
    }

    public NatsDistributedLockProvider(INatsKVContext kvContext, ILogger<NatsDistributedLockProvider> logger, TimeSpan ttl)
    {
        _kvContext = kvContext;
        _logger = logger;
        _ttl = ttl;
    }

    public IDistributedLock CreateLock(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        return new NatsDistributedLock(_kvContext, name, _logger, BucketName, _ttl);
    }

    private sealed class NatsDistributedLock : IDistributedLock
    {
        private readonly INatsKVContext _kvContext;
        private readonly ILogger _logger;
        private readonly string _bucketName;
        private readonly TimeSpan _ttl;

        public string Name { get; }

        public NatsDistributedLock(
            INatsKVContext kvContext,
            string name,
            ILogger logger,
            string bucketName,
            TimeSpan ttl)
        {
            _kvContext = kvContext;
            Name = name;
            _logger = logger;
            _bucketName = bucketName;
            _ttl = ttl;
        }

        [Obsolete("Prefer TryAcquireAsync to avoid thread pool starvation. This method may deadlock in SynchronizationContext environments.")]
        public IDistributedSynchronizationHandle? TryAcquire(TimeSpan timeout = default, CancellationToken cancellationToken = default)
        {
            // Task.Run offloads to a thread pool thread without a SynchronizationContext, preventing deadlock
            return Task.Run(() => TryAcquireAsync(timeout, cancellationToken).AsTask(), cancellationToken).GetAwaiter().GetResult();
        }

        [Obsolete("Prefer AcquireAsync to avoid thread pool starvation. This method may deadlock in SynchronizationContext environments.")]
        public IDistributedSynchronizationHandle Acquire(TimeSpan? timeout = null, CancellationToken cancellationToken = default)
        {
            // Task.Run offloads to a thread pool thread without a SynchronizationContext, preventing deadlock
            return Task.Run(() => AcquireAsync(timeout, cancellationToken).AsTask(), cancellationToken).GetAwaiter().GetResult();
        }

        public async ValueTask<IDistributedSynchronizationHandle?> TryAcquireAsync(TimeSpan timeout = default, CancellationToken cancellationToken = default)
        {
            // Build Polly retry pipeline with exponential backoff + jitter
            var retryPipeline = BuildRetryPipeline(timeout);

            // Create a timeout cancellation token to enforce the timeout
            using var timeoutCts = timeout > TimeSpan.Zero && timeout != Timeout.InfiniteTimeSpan
                ? new CancellationTokenSource(timeout)
                : new CancellationTokenSource();

            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken,
                timeoutCts.Token);

            try
            {
                return await retryPipeline.ExecuteAsync(async ct =>
                {
                    try
                    {
                        // Ensure bucket exists on each retry attempt (in case of transient failures)
                        var store = await EnsureBucketAndGetStoreAsync(ct);

                        // Try to create the key atomically - fails if key exists
                        // CreateAsync returns the revision number which we track for OCC
                        var revision = await store.CreateAsync(Name,
                            System.Text.Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")),
                            cancellationToken: ct);

                        _logger.LogInformation("Acquired lock: {Key} (revision: {Revision})", Name, revision);
                        return (IDistributedSynchronizationHandle?)new NatsLockHandle(store, Name, _logger, _ttl, revision);
                    }
                    catch (NatsKVCreateException)
                    {
                        // Key exists - check if it's a tombstone (empty value = free lock)
                        try
                        {
                            var store = await EnsureBucketAndGetStoreAsync(ct);
                            var entry = await store.GetEntryAsync<byte[]>(Name, cancellationToken: ct);

                            // Empty value means tombstone - lock is free, try to acquire
                            if (entry.Value == null || entry.Value.Length == 0)
                            {
                                // Take ownership via UpdateAsync with revision check
                                var newRevision = await store.UpdateAsync(
                                    Name,
                                    System.Text.Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")),
                                    entry.Revision,
                                    cancellationToken: ct);

                                _logger.LogInformation("Acquired lock from tombstone: {Key} (revision: {Revision})", Name, newRevision);
                                return (IDistributedSynchronizationHandle?)new NatsLockHandle(store, Name, _logger, _ttl, newRevision);
                            }

                            // Non-empty value means lock is held by another client
                            // The KV store should auto-expire based on MaxAge, so just wait and retry
                            throw new LockNotAcquiredException();
                        }
                        catch (NatsKVWrongLastRevisionException)
                        {
                            // Race condition - another client got it first
                            throw new LockNotAcquiredException();
                        }
                        catch (NatsKVKeyNotFoundException)
                        {
                            // Key was deleted between our attempts - retry will try CreateAsync again
                            throw new LockNotAcquiredException();
                        }
                        catch (NatsKVException kvEx)
                        {
                            _logger.LogDebug(kvEx, "KV error while checking tombstone for lock {Key}", Name);
                            throw new LockNotAcquiredException();
                        }
                        catch (NatsJSApiException jsEx)
                        {
                            _logger.LogDebug(jsEx, "JetStream API error while checking tombstone for lock {Key}", Name);
                            throw new LockNotAcquiredException();
                        }
                    }
                    catch (NatsKVException ex)
                    {
                        // Other KV error, log and throw to trigger retry
                        _logger.LogDebug(ex, "KV error while acquiring lock {Key}", Name);
                        throw new LockNotAcquiredException();
                    }
                    catch (NatsJSApiException ex)
                    {
                        // NATS error, log and throw to trigger retry
                        _logger.LogDebug(ex, "NATS JetStream API error while acquiring lock {Key}", Name);
                        throw new LockNotAcquiredException();
                    }
                    catch (Exception ex)
                    {
                        // Catch any other unexpected errors (including from EnsureBucketAndGetStoreAsync)
                        _logger.LogWarning(ex, "Unexpected error while acquiring lock {Key}, will retry", Name);
                        throw new LockNotAcquiredException();
                    }
                }, linkedCts.Token);
            }
            catch (OperationCanceledException) when (timeoutCts.Token.IsCancellationRequested)
            {
                // Timeout occurred
                _logger.LogWarning("Lock acquisition for {Key} timed out after {Timeout}", Name, timeout);
                return null;
            }
            catch (LockNotAcquiredException)
            {
                // All retries exhausted
                _logger.LogWarning("Failed to acquire lock {Key} after all retry attempts", Name);
                return null;
            }
            catch (Exception ex)
            {
                // Unexpected error that escaped the retry pipeline
                _logger.LogError(ex, "Unexpected error in lock acquisition for {Key}", Name);
                throw;
            }
        }

        public async ValueTask<IDistributedSynchronizationHandle> AcquireAsync(TimeSpan? timeout = null, CancellationToken cancellationToken = default)
        {
            var effectiveTimeout = timeout ?? Timeout.InfiniteTimeSpan;
            
            var handle = await TryAcquireAsync(effectiveTimeout, cancellationToken);
            
            if (handle == null)
            {
                throw new TimeoutException($"Could not acquire lock '{Name}' within {effectiveTimeout}");
            }
            
            return handle;
        }

        private async Task<INatsKVStore> EnsureBucketAndGetStoreAsync(CancellationToken cancellationToken)
        {
            try
            {
                await _kvContext.CreateStoreAsync(
                    new NatsKVConfig(_bucketName) { MaxAge = _ttl },
                    cancellationToken: cancellationToken);
            }
            catch (NatsJSApiException ex) when (ex.Error.Code == 400 && ex.Error.Description?.Contains("already") == true)
            {
                // Bucket already exists - ignore
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error creating KV bucket {Bucket}, will attempt to get existing store", _bucketName);
            }

            return await _kvContext.GetStoreAsync(_bucketName, cancellationToken: cancellationToken);
        }

        private ResiliencePipeline<IDistributedSynchronizationHandle?> BuildRetryPipeline(TimeSpan timeout)
        {
            // Calculate max retries based on timeout
            // With exponential backoff (50ms, 100ms, 200ms, 400ms, 800ms, 800ms, ...),
            // after ~5 attempts we hit the 800ms cap and stay there
            // Cap at a reasonable maximum to prevent integer overflow and excessive retry attempts
            const int MaxReasonableRetries = 10000; // Prevent int overflow and excessive retries

            int maxRetries;
            if (timeout == TimeSpan.Zero)
            {
                maxRetries = 1;
            }
            else if (timeout == Timeout.InfiniteTimeSpan)
            {
                maxRetries = MaxReasonableRetries;
            }
            else
            {
                // Estimate retries: first 5 attempts use exponential backoff (sum ≈ 50+100+200+400+800 = 1550ms)
                // Remaining attempts use 800ms each
                // So: retries ≈ 5 + (timeout - 1550ms) / 800ms
                var timeoutMs = timeout.TotalMilliseconds;
                if (timeoutMs <= 1550)
                {
                    // Only exponential phase, rough estimate
                    maxRetries = Math.Max(1, (int)(timeoutMs / 300));
                }
                else
                {
                    // Include both exponential and steady-state phases
                    maxRetries = 5 + (int)((timeoutMs - 1550) / 800);
                }
                maxRetries = Math.Min(maxRetries, MaxReasonableRetries);
            }

            return new ResiliencePipelineBuilder<IDistributedSynchronizationHandle?>()
                .AddRetry(new RetryStrategyOptions<IDistributedSynchronizationHandle?>
                {
                    ShouldHandle = new PredicateBuilder<IDistributedSynchronizationHandle?>()
                        .Handle<LockNotAcquiredException>(),
                    MaxRetryAttempts = maxRetries,
                    DelayGenerator = args =>
                    {
                        // Exponential backoff: 50ms * 2^attempt, capped at 800ms
                        // Guard against overflow: cap attempt number to prevent Math.Pow overflow
                        var attemptNumber = Math.Min(args.AttemptNumber, 10); // 2^10 = 1024, so 50 * 1024 = 51200ms
                        var baseDelay = TimeSpan.FromMilliseconds(50 * Math.Pow(2, attemptNumber));
                        var cappedDelay = TimeSpan.FromMilliseconds(Math.Min(baseDelay.TotalMilliseconds, 800));

                        // Add jitter: ±25%
                        var jitterFactor = 1.0 + (Random.Shared.NextDouble() - 0.5) * 0.5;
                        var finalDelay = TimeSpan.FromMilliseconds(cappedDelay.TotalMilliseconds * jitterFactor);

                        return ValueTask.FromResult<TimeSpan?>(finalDelay);
                    },
                    OnRetry = args =>
                    {
                        _logger.LogDebug(
                            "Lock acquisition attempt {Attempt} failed for {Key}, retrying in {Delay}ms",
                            args.AttemptNumber + 1, Name, args.RetryDelay.TotalMilliseconds);
                        return ValueTask.CompletedTask;
                    }
                })
                .Build();
        }

        private sealed class LockNotAcquiredException : Exception { }
    }

    private sealed class NatsLockHandle : IDistributedSynchronizationHandle
    {
        private readonly INatsKVStore _store;
        private readonly string _key;
        private readonly ILogger _logger;
        private readonly CancellationTokenSource _heartbeatCts = new();
        private readonly CancellationTokenSource _handleLostCts = new();
        private readonly Task _heartbeatTask;
        private int _disposed;
        private ulong _currentRevision;

        public CancellationToken HandleLostToken => _handleLostCts.Token;

        public NatsLockHandle(INatsKVStore store, string key, ILogger logger, TimeSpan ttl, ulong initialRevision)
        {
            _store = store;
            _key = key;
            _logger = logger;
            _currentRevision = initialRevision;
            
            // Start heartbeat at half the TTL interval
            _heartbeatTask = HeartbeatLoopAsync(ttl / 2, _heartbeatCts.Token);
        }

        private async Task HeartbeatLoopAsync(TimeSpan interval, CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(interval, ct);
                    
                    // Use UpdateAsync with revision check (OCC) to prevent split-brain
                    // If revision doesn't match, another node acquired the lock
                    var newRevision = await _store.UpdateAsync(
                        _key, 
                        System.Text.Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")), 
                        _currentRevision,
                        cancellationToken: ct);
                    
                    // Update our tracked revision for the next heartbeat
                    _currentRevision = newRevision;
                }
                catch (OperationCanceledException) 
                { 
                    break; 
                }
                catch (NatsKVWrongLastRevisionException ex)
                {
                    // Revision mismatch - lock was stolen by another node!
                    _logger.LogWarning(
                        "Lock {Key} lost due to revision mismatch (expected {Expected}, current {Current}). " +
                        "Another node may have acquired the lock.", 
                        _key, _currentRevision, ex.Message);
                    
                    // Signal that the lock has been lost
                    await _handleLostCts.CancelAsync();
                    break;
                }
                catch (NatsKVKeyNotFoundException)
                {
                    // Key was deleted - lock is gone
                    _logger.LogWarning("Lock {Key} lost - key no longer exists", _key);
                    await _handleLostCts.CancelAsync();
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Lock heartbeat failed for {Key}", _key);
                }
            }
        }

        public void Dispose()
        {
            // Fire-and-forget with best-effort error handling
            // Lock TTL will auto-expire if this fails
            _ = Task.Run(async () =>
            {
                try
                {
                    await DisposeAsync();
                }
                catch (Exception)
                {
                    // Lock will auto-release via TTL; swallow exception
                }
            });
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 1)
                return;

            try
            {
                await _heartbeatCts.CancelAsync();
                await _heartbeatTask;
                
                // Use tombstone approach: UpdateAsync with revision check to write empty value
                // This prevents race conditions where we might delete a lock held by another client
                // If revision doesn't match, another node owns the lock now - do nothing
                await _store.UpdateAsync(
                    _key, 
                    Array.Empty<byte>(), // Empty value = tombstone (lock is free)
                    _currentRevision,
                    cancellationToken: CancellationToken.None);
                _logger.LogInformation("Released lock: {Key}", _key);
            }
            catch (NatsKVWrongLastRevisionException)
            {
                // Revision mismatch - we no longer own this lock, nothing to release
                _logger.LogDebug("Lock {Key} already released or reacquired by another node", _key);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to release lock: {Key}", _key);
            }
            finally
            {
                _heartbeatCts.Dispose();
                _handleLostCts.Dispose();
            }
        }
    }
}

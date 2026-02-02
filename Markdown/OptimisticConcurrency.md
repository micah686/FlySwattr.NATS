# Optimistic Concurrency Control (OCC) with FlySwattr.NATS

Optimistic Concurrency Control (OCC) is a strategy where you assume multiple transactions can complete without affecting each other, and you only check for conflicts before committing changes.

The standard `IKeyValueStore` interface in FlySwattr is designed for simplified **Last-Write-Wins** semantics (`Get` returns the value, `Put` overwrites it). It does not natively expose the version/revision numbers required for atomic Compare-And-Swap (CAS) operations.

However, FlySwattr provides two robust ways to handle concurrency without modifying the core framework interfaces.

## Option 1: Safe Concurrency via Distributed Locks (Recommended for Simplicity)

If you prefer to stick strictly to the `IKeyValueStore` abstraction, you can ensure data consistency by serializing access to a specific key using `IDistributedLockProvider`. While this is technically *Pessimistic* Concurrency (locking), it achieves the goal of preventing lost updates.

### Implementation

Inject `IDistributedLockProvider` alongside your store. FlySwattr registers this automatically when you call `AddEnterpriseNATSMessaging()` or `AddFlySwattrNatsDistributedLock()`.

```csharp
using FlySwattr.NATS.Abstractions;
using Medallion.Threading;

public class JobStateService
{
    private readonly IKeyValueStore _store;
    private readonly IDistributedLockProvider _lockProvider;

    public JobStateService(
        Func<string, IKeyValueStore> storeFactory,
        IDistributedLockProvider lockProvider)
    {
        _store = storeFactory("jobs-bucket");
        _lockProvider = lockProvider;
    }

    public async Task UpdateJobStateSafelyAsync(string jobId, Action<JobState> updateAction)
    {
        // 1. Acquire a lock for the specific Job ID
        // This ensures no other process can modify this job while we are working on it.
        await using var handle = await _lockProvider.AcquireLockAsync(
            $"lock:{jobId}",
            TimeSpan.FromSeconds(5));

        if (handle == null)
        {
             throw new TimeoutException($"Could not acquire lock for job {jobId}");
        }

        // 2. Read the latest state (Safe because we hold the lock)
        var state = await _store.GetAsync<JobState>(jobId) ?? new JobState();

        // 3. Apply changes locally
        updateAction(state);

        // 4. Save the new state
        await _store.PutAsync(jobId, state);

        // Lock is released automatically when 'handle' is disposed
    }
}
```

## Option 2: Native NATS OCC (Recommended for High Throughput)

For true Optimistic Concurrency Control (using NATS Revisions), you can bypass the simplified `IKeyValueStore` abstraction and use the underlying `INatsKVContext` which is automatically registered by FlySwattr.

This approach allows you to use the native `Update` operation which requires the **Reference Revision** to succeed.

### Implementation

Inject `INatsKVContext` directly.

```csharp
using NATS.Client.KeyValueStore;

public class OptimisticJobStateService
{
    private readonly INatsKVContext _kvContext;

    public OptimisticJobStateService(INatsKVContext kvContext)
    {
        _kvContext = kvContext;
    }

    public async Task UpdateJobStateOptimisticallyAsync(string jobId, Action<JobState> updateAction)
    {
        var bucket = await _kvContext.CreateStoreAsync("jobs-bucket");

        var retry = 3;
        while (retry-- > 0)
        {
            try
            {
                // 1. Get the Entry, which includes the Value AND the Revision
                var entry = await bucket.GetEntryAsync<JobState>(jobId);

                // Handle "Not Found" case if necessary
                if (entry.Value == null)
                {
                    // Handle creation logic or throw
                    var newState = new JobState();
                    updateAction(newState);
                    await bucket.PutAsync(jobId, newState);
                    return;
                }

                // 2. Apply changes
                var currentState = entry.Value;
                updateAction(currentState);

                // 3. Attempt Atomic Update (CAS)
                // This only succeeds if the server's revision matches 'entry.Revision'
                await bucket.UpdateAsync(jobId, currentState, entry.Revision);
                return; // Success!
            }
            catch (NatsKVWrongLastRevisionException)
            {
                // 4. Concurrency conflict detected!
                // The record was modified by someone else since our GetEntryAsync()
                // Loop to reload and try again.
                continue;
            }
        }

        throw new Exception($"Failed to update job {jobId} after multiple retries due to concurrency.");
    }
}
```

**Note:** The exception type is `NatsKVWrongLastRevisionException` in NATS.Net v2.x.

## Summary

| Strategy | Pros | Cons |
|:--- |:--- |:--- |
| **Distributed Lock** | Simple, uses `IKeyValueStore`, guaranteed order. | Adds latency due to locking mechanism. |
| **Native OCC** | High performance, no locks, standard pattern. | Requires `INatsKVContext` dependency instead of `IKeyValueStore`. |

## When to Use Which

- **Distributed Lock**: Best for operations that are infrequent or where simplicity is paramount. Also useful when you need to coordinate across multiple keys or resources.

- **Native OCC**: Best for high-throughput scenarios where contention is expected to be low and you want to minimize latency. Ideal for single-key updates with retry logic.

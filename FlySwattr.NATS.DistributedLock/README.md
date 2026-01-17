# FlySwattr.NATS.DistributedLock

**Distributed concurrency control backed by NATS.**

This library implements the `DistributedLock.Core` interfaces using NATS Key-Value buckets. It allows you to coordinate tasks across multiple instances of your application.

## 🌟 Key Features

*   **Standard Interface:** Implements `IDistributedLockProvider` from the popular `DistributedLock` library ecosystem.
*   **Lease-Based:** Locks have a TTL (Time-To-Live). If a process crashes, the lock is automatically released by NATS.
*   **Heartbeats:** Automatically refreshes the lock lease while the process is alive.
*   **Optimistic Concurrency:** Uses NATS revision numbers (CAS) to ensure safe acquisition and release.

## 📦 Usage

```csharp
public class Scheduler(IDistributedLockProvider lockProvider)
{
    public async Task RunDailyJob()
    {
        // Try to acquire lock - non-blocking
        await using var handle = await lockProvider.TryAcquireLockAsync("daily-report-job");
        
        if (handle != null)
        {
            // We got the lock! Run the job.
            await GenerateReport();
        }
        else
        {
            // Another instance is already running it.
            _logger.LogInformation("Skipping job, locked by another instance.");
        }
    }
}
```

### Registration

```csharp
services.AddFlySwattrNatsDistributedLock();
```

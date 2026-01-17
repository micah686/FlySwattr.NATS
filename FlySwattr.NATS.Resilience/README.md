# FlySwattr.NATS.Resilience

**Enterprise-grade reliability patterns for NATS JetStream.**

This library integrates `Polly` policies directly into the NATS consumer and publisher pipeline. It protects your system from cascading failures, resource exhaustion, and transient network glitches.

## 🛡️ Key Features

### 1. Two-Tier Bulkhead Isolation
Prevents a single "noisy neighbor" consumer from starving the entire application of resources.
*   **Global Pool:** Limits total concurrent processing for a group of consumers (e.g., "default" pool = 100).
*   **Per-Consumer Semaphore:** Enforces fairness by capping individual consumers (e.g., "LogIngest" can only use 20 of the 100 permits).

### 2. Hierarchical Circuit Breakers
*   **Consumer-Level Isolation:** If one consumer starts failing (e.g., database down), its circuit trips independently. Other consumers on the same connection continue processing normally.
*   **Smart Recovery:** Automatically probes connection health before fully reopening traffic.

### 3. Intelligent Retries
*   **Transient Errors:** Automatically retries network glitches, timeouts, and `IOException`s with exponential backoff and jitter.
*   **Idempotency Preservation:** Ensures retries reuse the original `MessageId` so JetStream can de-duplicate correctly.

## 📦 Usage

Add resilience to your NATS setup:

```csharp
services.AddFlySwattrNatsResilience(options =>
{
    // Define Bulkhead Pools
    options.NamedPools["default"] = 100;
    options.NamedPools["critical"] = 10; // Dedicated lane for high-priority msgs
});
```

The resilience decorators (`ResilientJetStreamConsumer`, `ResilientJetStreamPublisher`) are automatically applied to the dependency injection container.

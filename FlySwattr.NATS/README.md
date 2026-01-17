# FlySwattr.NATS

**The "Golden Path" entry point for building production-grade NATS applications in .NET.**

This is the meta-package that bundles all FlySwattr capabilities into a single, cohesive suite. It provides a simple, opinionated configuration API to set up a robust event-driven architecture with just a few lines of code.

## 🚀 Quick Start

Instead of wiring up individual components manually, use the `AddEnterpriseNATSMessaging` extension to configure everything at once.

```csharp
// Program.cs
builder.Services.AddEnterpriseNATSMessaging(options =>
{
    // 1. Connection (Required)
    options.Core.Url = "nats://localhost:4222";
    
    // 2. Enable Reliability Features (Optional, on by default)
    options.EnableResilience = true;
    options.EnablePayloadOffloading = true; // Claim Check for >1MB msgs
    options.EnableCaching = true;           // FusionCache for KV stores
    options.EnableTopologyProvisioning = true;
});
```

## 🌟 Features Included

By referencing this package, you get access to the full power of the FlySwattr ecosystem:

*   **Core Messaging:** High-performance Pub/Sub and Request/Reply using `NATS.Net` v2.
*   **Resilience:** Integrated Circuit Breakers, Bulkheads, and Retry policies via `Polly`.
*   **Large Payloads:** Automatic "Claim Check" pattern handles messages larger than NATS limits by offloading payload to Object Store.
*   **Caching:** L1 (Memory) + L2 (NATS KV) caching with "Stale-While-Revalidate" protection using `FusionCache`.
*   **Topology Management:** Declarative "Infrastructure-as-Code" for Streams and Consumers.
*   **Distributed Locking:** Concurrency control primitives backed by NATS KV.
*   **Observability:** Health checks, Dead Letter Queue (DLQ) monitoring, and structured logging.

## 🔧 Advanced Configuration

You can fine-tune every aspect of the stack through the options delegate:

```csharp
builder.Services.AddEnterpriseNATSMessaging(opts =>
{
    // Tune Connection
    opts.Core.MaxConcurrency = 500;
    opts.Core.ReconnectWait = TimeSpan.FromSeconds(2);

    // Tune Resilience
    opts.Resilience.NamedPools["critical"] = 20; // Reserved capacity for critical events
    opts.Resilience.QueueLimitMultiplier = 5;

    // Tune Caching
    opts.Caching.MemoryCacheDuration = TimeSpan.FromMinutes(10);
    opts.Caching.FailSafeMaxDuration = TimeSpan.FromHours(4); // Serve stale if NATS is down
});
```

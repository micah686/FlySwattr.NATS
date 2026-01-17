# FlySwattr.NATS

**The "Golden Path" entry point for building production-grade NATS applications in .NET.**

This is the meta-package that bundles all FlySwattr capabilities into a single, cohesive suite. It provides a simple, opinionated configuration API to set up a robust event-driven architecture with just a few lines of code.

## 🚀 Quick Start

Instead of wiring up individual components manually, use the `AddEnterpriseNATSMessaging` extension to configure everything at once.

### Minimal Configuration

The bare minimum to get started - all reliability features are enabled by default:

```csharp
// Program.cs
builder.Services.AddEnterpriseNATSMessaging(options =>
{
    // Only the connection URL is required
    options.Core.Url = "nats://localhost:4222";
});
```

### Simple Configuration

Most common production setup with basic customization:

```csharp
builder.Services.AddEnterpriseNATSMessaging(options =>
{
    // Connection
    options.Core.Url = "nats://my-cluster:4222";

    // Toggle features (all default to true)
    options.EnableResilience = true;
    options.EnablePayloadOffloading = true;  // Claim Check for large messages
    options.EnableCaching = true;             // FusionCache for KV stores
    options.EnableDistributedLock = true;     // NATS KV-backed locking
    options.EnableTopologyProvisioning = true; // Auto-create streams/consumers
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

## 🔧 Complete Configuration Reference

Every available option with explanations and defaults. Use this as a reference when fine-tuning for your production workload:

```csharp
using NATS.Client.Core;

builder.Services.AddEnterpriseNATSMessaging(opts =>
{
    // ═══════════════════════════════════════════════════════════════════════
    // CORE CONNECTION SETTINGS
    // ═══════════════════════════════════════════════════════════════════════

    // REQUIRED: NATS server URL
    opts.Core.Url = "nats://localhost:4222";  // Default: "nats://localhost:4222"

    // OPTIONAL: Authentication (choose one or omit for no auth)
    opts.Core.NatsAuth = NatsAuthOpts.Default
        .WithUsername("user")
        .WithPassword("pass");
    // OR: opts.Core.NatsAuth = NatsAuthOpts.Default.WithToken("my-token");
    // OR: opts.Core.NatsAuth = NatsAuthOpts.Default.WithNKey("seed-key");

    // OPTIONAL: TLS configuration
    opts.Core.TlsOpts = new NatsTlsOpts
    {
        Mode = TlsMode.Require,
        InsecureSkipVerify = false  // Set true only for dev/testing
    };

    // Connection resilience
    opts.Core.ReconnectWait = TimeSpan.FromSeconds(2);    // Default: null (uses NATS.Net default)
    opts.Core.MaxReconnect = -1;                          // Default: null (unlimited retries)
    opts.Core.MaxConcurrency = 100;                       // Default: 100 parallel operations

    // Serializer configuration
    opts.Core.MaxPayloadSize = 10 * 1024 * 1024;          // Default: 10MB

    // ═══════════════════════════════════════════════════════════════════════
    // FEATURE TOGGLES
    // ═══════════════════════════════════════════════════════════════════════

    opts.EnableResilience = true;              // Default: true (circuit breakers + bulkheads)
    opts.EnablePayloadOffloading = true;       // Default: true (Claim Check pattern)
    opts.EnableCaching = true;                 // Default: true (FusionCache for KV)
    opts.EnableDistributedLock = true;         // Default: true (NATS KV-backed locks)
    opts.EnableTopologyProvisioning = true;    // Default: true (auto-create streams/consumers)

    // ═══════════════════════════════════════════════════════════════════════
    // PAYLOAD OFFLOADING (Claim Check Pattern)
    // Automatically offloads large messages to Object Store
    // ═══════════════════════════════════════════════════════════════════════

    opts.ClaimCheckBucket = "claim-checks";                          // Default: "claim-checks"
    opts.PayloadOffloading.ThresholdBytes = 64 * 1024;               // Default: 64KB
    opts.PayloadOffloading.ClaimCheckHeaderName = "X-ClaimCheck-Ref"; // Default: "X-ClaimCheck-Ref"
    opts.PayloadOffloading.ClaimCheckTypeHeaderName = "X-ClaimCheck-Type"; // Default: "X-ClaimCheck-Type"
    opts.PayloadOffloading.ObjectKeyPrefix = "claimcheck";           // Default: "claimcheck"
    opts.PayloadOffloading.ObjectStoreServiceKey = null;             // Default: null (use default store)

    // ═══════════════════════════════════════════════════════════════════════
    // RESILIENCE (Bulkhead Isolation + Circuit Breakers)
    // Prevents cascade failures and resource exhaustion
    // ═══════════════════════════════════════════════════════════════════════

    // Named bulkhead pools for workload isolation
    opts.Resilience.NamedPools["default"] = 100;        // Default: 100
    opts.Resilience.NamedPools["critical"] = 10;        // Default: 10 (DLQ, system events)
    opts.Resilience.NamedPools["highPriority"] = 20;    // Default: 20 (user-configurable tier)

    // Queue limit = PermitLimit × QueueLimitMultiplier
    opts.Resilience.QueueLimitMultiplier = 2;           // Default: 2

    // ═══════════════════════════════════════════════════════════════════════
    // CACHING (FusionCache for KV Store)
    // L1 (memory) + L2 (NATS KV) with Stale-While-Revalidate
    // ═══════════════════════════════════════════════════════════════════════

    opts.Caching.MemoryCacheDuration = TimeSpan.FromMinutes(5);     // Default: 5 minutes
    opts.Caching.FailSafeMaxDuration = TimeSpan.FromHours(1);       // Default: 1 hour
    opts.Caching.FactorySoftTimeout = TimeSpan.FromSeconds(1);      // Default: 1 second
    opts.Caching.FactoryHardTimeout = TimeSpan.FromSeconds(10);     // Default: 10 seconds
    opts.Caching.NotFoundCacheDuration = TimeSpan.FromSeconds(30);  // Default: 30 seconds

    // ═══════════════════════════════════════════════════════════════════════
    // HEALTH CHECKS (Zombie Consumer Detection)
    // Monitors consume loops to detect silent failures
    // ═══════════════════════════════════════════════════════════════════════

    opts.HealthCheck.LoopIterationTimeout = TimeSpan.FromMinutes(2);  // Default: 2 minutes
    opts.HealthCheck.NoMessageWarningTimeout = null;                  // Default: null (disabled)
    // Optional: Set to TimeSpan.FromMinutes(10) to warn if no messages for 10 min

    // ═══════════════════════════════════════════════════════════════════════
    // TOPOLOGY STARTUP (Cold Start Resilience)
    // Controls retry behavior when NATS is not immediately available
    // (e.g., Kubernetes sidecar startup delays)
    // ═══════════════════════════════════════════════════════════════════════

    opts.TopologyStartup.MaxRetryAttempts = 10;                       // Default: 10
    opts.TopologyStartup.InitialRetryDelay = TimeSpan.FromSeconds(1); // Default: 1 second
    opts.TopologyStartup.MaxRetryDelay = TimeSpan.FromSeconds(30);    // Default: 30 seconds
    opts.TopologyStartup.ConnectionTimeout = TimeSpan.FromSeconds(5); // Default: 5 seconds
    opts.TopologyStartup.TotalStartupTimeout = TimeSpan.FromMinutes(2); // Default: 2 minutes
});
```

### Configuration Best Practices

**Development:**
```csharp
opts.Core.Url = "nats://localhost:4222";
// All features enabled by default - perfect for testing the full stack
```

**Production (High-Throughput):**
```csharp
opts.Core.MaxConcurrency = 500;
opts.Resilience.NamedPools["default"] = 200;
opts.Resilience.QueueLimitMultiplier = 3;
opts.PayloadOffloading.ThresholdBytes = 64 * 1024; // 64KB
```

**Production (Low-Latency):**
```csharp
opts.EnableCaching = false;  // Disable if you need real-time consistency
opts.Caching.MemoryCacheDuration = TimeSpan.FromSeconds(30);  // Or use shorter cache
```

**Edge/Lightweight:**
```csharp
opts.EnableResilience = false;
opts.EnablePayloadOffloading = false;
opts.EnableCaching = false;
// Minimal overhead for constrained environments
```

## 📋 Additional Setup (Beyond AddEnterpriseNATSMessaging)

Some components require manual registration as they're app-specific:

### 1. Topology Sources (Infrastructure-as-Code)

To use declarative topology provisioning, register your `ITopologySource` implementations:

```csharp
// After AddEnterpriseNATSMessaging
services.AddNatsTopologySource<MyApplicationTopology>();
services.AddNatsTopologySource<SharedInfrastructureTopology>();
```

### 2. DLQ Advisory Listener (Optional)

Monitor NATS server-side delivery failures (when consumers exceed MaxDeliver):

```csharp
services.AddNatsDlqAdvisoryListener(opts =>
{
    // Default: "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>"
    opts.AdvisorySubject = "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>";

    // Optional: Filter by stream names
    opts.StreamFilter.Add("ORDERS_STREAM");
    opts.StreamFilter.Add("PAYMENTS_STREAM");

    // Optional: Filter by consumer names
    opts.ConsumerFilter.Add("order-processor");

    // Whether to trigger existing IDlqNotificationService (default: true)
    opts.TriggerDlqNotification = true;

    // Reconnect delay after subscription error (default: 5 seconds)
    opts.ReconnectDelay = TimeSpan.FromSeconds(5);
});

// Add custom handlers for alerts (multiple can be registered)
services.AddDlqAdvisoryHandler<PagerDutyAlertHandler>();
services.AddDlqAdvisoryHandler<SlackNotificationHandler>();
```

### 3. Background Consumers (Manual Registration)

For manually configured consumers (vs auto-provisioned ones):

```csharp
services.AddNatsConsumer<OrderCreatedEvent>(
    streamName: "ORDERS_STREAM",
    consumerName: "order-processor",
    handler: async (ctx) =>
    {
        var order = ctx.Message;
        await ProcessOrder(order);
    },
    configureOptions: opts =>
    {
        // Concurrency
        opts.MaxConcurrency = 10;                         // Default: 10

        // Resilience
        opts.ResiliencePipelineKey = "highPriority";      // Default: null (uses "default" pool)

        // DLQ Configuration
        opts.DlqPolicy = new DeadLetterPolicy
        {
            MaxRetries = 3,
            BackoffStrategy = BackoffStrategy.Exponential
        };
        opts.DlqPublisherServiceKey = null;               // Default: null (uses default publisher)

        // Large Payload Support
        opts.ObjectStoreServiceKey = null;                // Default: null (uses default store)

        // Custom Poison Handler
        opts.PoisonHandlerKey = null;                     // Default: null (uses DefaultDlqPoisonHandler)

        // Middleware
        opts.EnableLoggingMiddleware = true;              // Default: true
        opts.EnableValidationMiddleware = true;           // Default: true
        opts.AddMiddleware<CustomAuditMiddleware>();      // Add custom middleware
    });
```


# FlySwattr.NATS

An enterprise-grade wrapper library for [NATS.Net](https://github.com/nats-io/nats.net), providing a production-ready messaging platform with advanced features including JetStream support, resilience patterns, topology management, dead-letter queues (DLQ), payload offloading, caching, and distributed locking.

### AI Notice:
This library has been generated with AI, but also partially reviewed by hand for issues.

## Features

- **Core NATS Messaging** - Publish/subscribe, request-response patterns with automatic reconnection
- **JetStream Support** - Durable, at-least-once messaging with push and pull consumers
- **Claim Check Pattern** - Automatic large payload offloading to Object Store
- **Resilience Patterns** - Circuit breakers, bulkhead isolation, and retry policies via Polly
- **Dead Letter Queues** - Automatic DLQ infrastructure with advisory monitoring and remediation
- **Topology as Code** - Declarative stream, consumer, and bucket provisioning
- **Key-Value Store** - Abstraction over NATS KV with optional FusionCache L1+L2 caching
- **Object Store** - Abstraction over NATS Object Store for large binary data
- **Distributed Locking** - NATS KV-based locks via Medallion.Threading
- **Health Checks** - Consumer zombie detection and startup validation
- **OpenTelemetry** - Built-in tracing and metrics instrumentation
- **Middleware Pipeline** - Extensible message processing with logging and validation

## Installation

The library is organized into modular NuGet packages:

| Package | Description |
|---------|-------------|
| `FlySwattr.NATS` | Main orchestration package ("Golden Path" entry point) |
| `FlySwattr.NATS.Abstractions` | Core interfaces and value objects |
| `FlySwattr.NATS.Core` | Core implementation (message bus, JetStream, stores) |
| `FlySwattr.NATS.Resilience` | Resilience patterns (bulkheads, circuit breakers) |
| `FlySwattr.NATS.Caching` | FusionCache integration for KV stores |
| `FlySwattr.NATS.Topology` | Topology management and auto-provisioning |
| `FlySwattr.NATS.Hosting` | Background services and health checks |
| `FlySwattr.NATS.DistributedLock` | Distributed locking via NATS KV |

## Quick Start

### Golden Path Configuration (Recommended)

The simplest way to get started is using `AddEnterpriseNATSMessaging()`, which configures all subsystems with production-ready defaults:

```csharp
builder.Services.AddEnterpriseNATSMessaging(opts =>
{
    // Core connection (required)
    opts.Core.Url = "nats://localhost:4222";

    // All features enabled by default:
    // - Payload offloading (64KB threshold)
    // - Resilience (circuit breakers, bulkheads)
    // - Caching (FusionCache L1+L2)
    // - Distributed locking
    // - Topology provisioning
    // - DLQ advisory monitoring
});

// Register your topology source
builder.Services.AddNatsTopologySource<OrdersTopology>();
```

### Defining Topology

Implement `ITopologySource` to declare your streams and consumers:

```csharp
public class OrdersTopology : ITopologySource
{
    public IEnumerable<StreamSpec> GetStreams() =>
    [
        new StreamSpec
        {
            Name = StreamName.From("ORDERS"),
            Subjects = ["orders.>"],
            StorageType = StorageType.File,
            RetentionPolicy = StreamRetention.Limits,
            MaxAge = TimeSpan.FromDays(7)
        }
    ];

    public IEnumerable<ConsumerSpec> GetConsumers() =>
    [
        new ConsumerSpec
        {
            StreamName = StreamName.From("ORDERS"),
            DurableName = ConsumerName.From("orders-processor"),
            FilterSubject = "orders.created",
            DeliverPolicy = DeliverPolicy.All,
            AckPolicy = AckPolicy.Explicit,
            AckWait = TimeSpan.FromSeconds(30),
            MaxDeliver = 5,
            DeadLetterPolicy = new DeadLetterPolicy
            {
                SourceStream = "ORDERS",
                SourceConsumer = "orders-processor",
                TargetStream = StreamName.From("ORDERS-DLQ"),
                TargetSubject = "orders.dlq"
            }
        }
    ];

    public IEnumerable<BucketSpec> GetBuckets() =>
    [
        new BucketSpec { Name = BucketName.From("order-state") }
    ];
}
```

### Publishing Messages

```csharp
public class OrderService
{
    private readonly IJetStreamPublisher _publisher;

    public OrderService(IJetStreamPublisher publisher)
    {
        _publisher = publisher;
    }

    public async Task CreateOrderAsync(Order order, CancellationToken ct)
    {
        // Message ID is required for JetStream idempotency
        await _publisher.PublishAsync(
            "orders.created",
            order,
            messageId: $"Order-{order.Id}-Created",
            cancellationToken: ct);
    }
}
```

### Consuming Messages

Use `AddNatsTopologyWithConsumers` for unified topology and handler registration:

```csharp
builder.Services.AddNatsTopologyWithConsumers<OrdersTopology>(topology =>
{
    topology.MapConsumer<OrderCreatedEvent>("orders-processor", async ctx =>
    {
        var order = ctx.Message;

        // Process the order...
        await ProcessOrderAsync(order);

        // Acknowledge successful processing
        await ctx.AckAsync();
    });
});
```

Or register individual consumers:

```csharp
builder.Services.AddNatsConsumer<OrderCreatedEvent>(
    streamName: "ORDERS",
    consumerName: "orders-processor",
    handler: async ctx =>
    {
        await ProcessOrderAsync(ctx.Message);
        await ctx.AckAsync();
    },
    configureOptions: opts =>
    {
        opts.MaxConcurrency = 10;
        opts.EnableLoggingMiddleware = true;
        opts.EnableValidationMiddleware = true;
    });
```

### Using Key-Value Store

```csharp
public class OrderStateService
{
    private readonly IKeyValueStore _store;

    public OrderStateService(Func<string, IKeyValueStore> storeFactory)
    {
        _store = storeFactory("order-state");
    }

    public async Task<OrderState?> GetStateAsync(string orderId, CancellationToken ct)
    {
        return await _store.GetAsync<OrderState>(orderId, ct);
    }

    public async Task SaveStateAsync(string orderId, OrderState state, CancellationToken ct)
    {
        await _store.PutAsync(orderId, state, ct);
    }

    public async Task WatchOrderAsync(string orderId, CancellationToken ct)
    {
        await _store.WatchAsync<OrderState>(orderId, async changeEvent =>
        {
            if (changeEvent.IsPut)
            {
                Console.WriteLine($"Order {orderId} updated: {changeEvent.Value}");
            }
        }, ct);
    }
}
```

### Using Object Store

```csharp
public class DocumentService
{
    private readonly IObjectStore _store;

    public DocumentService(Func<string, IObjectStore> storeFactory)
    {
        _store = storeFactory("documents");
    }

    public async Task UploadAsync(string key, Stream data, CancellationToken ct)
    {
        await _store.PutAsync(key, data, ct);
    }

    public async Task DownloadAsync(string key, Stream target, CancellationToken ct)
    {
        await _store.GetAsync(key, target, ct);
    }
}
```

### Distributed Locking

```csharp
public class CriticalOperationService
{
    private readonly IDistributedLockProvider _lockProvider;

    public CriticalOperationService(IDistributedLockProvider lockProvider)
    {
        _lockProvider = lockProvider;
    }

    public async Task PerformCriticalOperationAsync(string resourceId)
    {
        await using var handle = await _lockProvider.AcquireLockAsync(
            $"lock:{resourceId}",
            TimeSpan.FromSeconds(30));

        if (handle == null)
        {
            throw new TimeoutException($"Could not acquire lock for {resourceId}");
        }

        // Perform critical operation while holding the lock
        await DoCriticalWorkAsync();
    }
}
```

### Request-Response Pattern

```csharp
public class QueryService
{
    private readonly IMessageBus _messageBus;

    public QueryService(IMessageBus messageBus)
    {
        _messageBus = messageBus;
    }

    public async Task<OrderDetails?> GetOrderDetailsAsync(string orderId, CancellationToken ct)
    {
        var response = await _messageBus.RequestAsync<GetOrderRequest, OrderDetails>(
            "orders.query",
            new GetOrderRequest { OrderId = orderId },
            timeout: TimeSpan.FromSeconds(5),
            ct);

        return response;
    }
}
```

## Configuration Options

### EnterpriseNatsOptions

| Property | Default | Description |
|----------|---------|-------------|
| `Core.Url` | `nats://localhost:4222` | NATS server URL |
| `Core.MaxConcurrency` | `100` | Maximum concurrent operations |
| `Core.MaxPayloadSize` | `10MB` | Maximum message payload size |
| `EnablePayloadOffloading` | `true` | Enable Claim Check pattern |
| `PayloadOffloading.ThresholdBytes` | `64KB` | Offload messages larger than this |
| `EnableResilience` | `true` | Enable circuit breakers and bulkheads |
| `EnableCaching` | `true` | Enable FusionCache for KV stores |
| `EnableDistributedLock` | `true` | Enable distributed locking |
| `EnableTopologyProvisioning` | `true` | Auto-provision streams/consumers |
| `EnableDlqAdvisoryListener` | `true` | Monitor DLQ advisory events |

### Modular Registration

For fine-grained control, register individual components:

```csharp
// Core only
services.AddFlySwattrNatsCore(opts => opts.Url = "nats://localhost:4222");

// Add specific features
services.AddPayloadOffloading();
services.AddFlySwattrNatsResilience();
services.AddFlySwattrNatsCaching();
services.AddFlySwattrNatsTopology();
services.AddNatsTopologyProvisioning();
services.AddFlySwattrNatsDistributedLock();
services.AddFlySwattrNatsHosting();
services.AddNatsDlqAdvisoryListener();
```

## Dead Letter Queue (DLQ) Management

### Automatic DLQ Infrastructure

When consumers have a `DeadLetterPolicy`, FlySwattr automatically:
- Creates the DLQ stream specified in `TargetStream`
- Creates the `fs-dlq-entries` KV bucket for metadata storage
- Monitors NATS advisory events for `MAX_DELIVERIES` exceeded
- Stores failed message metadata for remediation

### DLQ Remediation

```csharp
public class DlqAdminService
{
    private readonly IDlqRemediationService _remediation;

    public DlqAdminService(IDlqRemediationService remediation)
    {
        _remediation = remediation;
    }

    public async Task<IReadOnlyList<DlqMessageEntry>> ListFailedMessagesAsync(CancellationToken ct)
    {
        return await _remediation.ListAsync(
            filterStream: "ORDERS",
            filterStatus: DlqMessageStatus.Pending,
            limit: 100,
            ct);
    }

    public async Task ReplayMessageAsync(string entryId, CancellationToken ct)
    {
        var result = await _remediation.ReplayAsync(entryId, ct);

        if (!result.Success)
        {
            Console.WriteLine($"Replay failed: {result.Error}");
        }
    }

    public async Task ArchiveMessageAsync(string entryId, string reason, CancellationToken ct)
    {
        await _remediation.ArchiveAsync(entryId, reason, ct);
    }
}
```

### Custom Advisory Handlers

```csharp
public class SlackAlertHandler : IDlqAdvisoryHandler
{
    public async Task HandleMaxDeliveriesExceededAsync(
        ConsumerMaxDeliveriesAdvisory advisory,
        CancellationToken ct)
    {
        await SendSlackAlertAsync(
            $"Message failed after {advisory.Deliveries} attempts: " +
            $"Stream={advisory.Stream}, Consumer={advisory.Consumer}");
    }
}

// Register custom handler
services.AddDlqAdvisoryHandler<SlackAlertHandler>();
```

## OpenTelemetry Integration

FlySwattr includes built-in OpenTelemetry instrumentation:

### Tracing
- ActivitySource: `FlySwattr.NATS`
- Spans for: publish, subscribe, request, consume operations

### Metrics
- Meter: `FlySwattr.NATS`
- Counters: `flyswattr.nats.messages.published`, `.received`, `.failed`
- Histograms: `.message.processing.duration`, `.publish.duration`, `.kv.operation.duration`

### Semantic Conventions
- `messaging.system`: `nats`
- `messaging.destination.name`: Subject/stream name
- `messaging.nats.stream`: JetStream stream
- `messaging.nats.consumer`: Consumer name

## Middleware Pipeline

The consumer middleware pipeline follows a Russian Doll pattern:

```csharp
public class MetricsMiddleware<T> : IConsumerMiddleware<T>
{
    public async Task InvokeAsync(
        IJsMessageContext<T> context,
        Func<Task> next,
        CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        try
        {
            await next();
            RecordSuccess(sw.Elapsed);
        }
        catch (Exception ex)
        {
            RecordFailure(sw.Elapsed, ex);
            throw;
        }
    }
}

// Register custom middleware
services.AddNatsConsumer<OrderEvent>("ORDERS", "processor", handler, opts =>
{
    opts.AddMiddleware<MetricsMiddleware<OrderEvent>>();
});
```

Built-in middleware:
- `LoggingMiddleware<T>` - Logs message handling with duration
- `ValidationMiddleware<T>` - FluentValidation integration (failures route to DLQ)

## Architecture

### Decorator Chain

The library uses the Decorator pattern for extensibility:

```
Application Code
       ↓
IJetStreamPublisher / IJetStreamConsumer
       ↓
ResilientJetStreamPublisher/Consumer (circuit breakers, bulkheads)
       ↓
OffloadingJetStreamPublisher/Consumer (claim check pattern)
       ↓
NatsJetStreamBus (core implementation)
       ↓
INatsJSContext (NATS.Net v2)
```

### Project Dependencies

```
FlySwattr.NATS (Orchestration)
    ├── FlySwattr.NATS.Abstractions
    ├── FlySwattr.NATS.Core
    ├── FlySwattr.NATS.Resilience
    ├── FlySwattr.NATS.Caching
    ├── FlySwattr.NATS.Topology
    ├── FlySwattr.NATS.Hosting
    └── FlySwattr.NATS.DistributedLock
```

## Requirements

- .NET 10.0+
- NATS Server 2.10+ (with JetStream enabled)

## License

MIT

## Contributing

Contributions are welcome. Please open an issue or submit a pull request.

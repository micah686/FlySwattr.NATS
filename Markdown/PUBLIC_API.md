# Public API Reference

This document lists the public API surface of the `FlySwattr.NATS` libraries. These are the classes, interfaces, and methods that consumers are expected to use directly.

---

## 1. Configuration & Dependency Injection

### Golden Path (Recommended)

*   **`AddEnterpriseNATSMessaging(Action<EnterpriseNatsOptions> configure)`**
    *   Configures all subsystems (Core, Offloading, Resilience, Caching, Topology, Hosting, DistributedLock) in the correct order with production-ready defaults.

### Modular Registration

| Extension Method | Package | Description |
|------------------|---------|-------------|
| `AddFlySwattrNatsCore(Action<NatsConfiguration>)` | Core | Base NATS connection, serializers, and core buses |
| `AddPayloadOffloading(Action<PayloadOffloadingOptions>?, string)` | Core | Claim Check pattern for large messages |
| `AddFlySwattrNatsResilience(Action<BulkheadConfiguration>?)` | Resilience | Circuit breakers, bulkheads, and retry policies |
| `AddFlySwattrNatsCaching(Action<FusionCacheConfiguration>?)` | Caching | FusionCache L1+L2 for KV stores |
| `AddFlySwattrNatsTopology()` | Topology | Core topology manager and ready signal |
| `AddNatsTopologySource<TSource>()` | Topology | Register an `ITopologySource` implementation |
| `AddNatsTopology<TSource>(Action<TopologyBuilder<TSource>>)` | Topology | Register topology with handler mappings |
| `AddNatsTopologyProvisioning(Action<TopologyStartupOptions>?)` | Topology | Auto-provision streams/consumers on startup |
| `AddFlySwattrNatsDistributedLock()` | DistributedLock | NATS KV-based distributed locking |
| `AddFlySwattrNatsHosting(Action<NatsConsumerHealthCheckOptions>?)` | Hosting | Health checks and startup probes |
| `AddNatsConsumer<TMessage>(...)` | Hosting | Register a background worker for a consumer |
| `AddNatsTopologyWithConsumers<TSource>(Action<TopologyBuilder<TSource>>)` | Hosting | Unified topology + consumer registration with auto-start |
| `AddNatsDlqAdvisoryListener(Action<DlqAdvisoryListenerOptions>?)` | Hosting | Monitor NATS advisory events for delivery failures |
| `AddDlqAdvisoryHandler<THandler>()` | Hosting | Register custom advisory event handlers |

---

## 2. Messaging Interfaces

### Core Messaging (`IMessageBus`)

For standard publish/subscribe and request-response patterns.

```csharp
public interface IMessageBus
{
    Task PublishAsync<T>(string subject, T message, CancellationToken cancellationToken = default);

    Task PublishAsync<T>(string subject, T message, MessageHeaders? headers,
        CancellationToken cancellationToken = default);

    Task SubscribeAsync<T>(string subject, Func<IMessageContext<T>, Task> handler,
        string? queueGroup = null, CancellationToken cancellationToken = default);

    Task<TResponse?> RequestAsync<TRequest, TResponse>(string subject, TRequest request,
        TimeSpan timeout, CancellationToken cancellationToken = default);
}
```

### JetStream Publishing (`IJetStreamPublisher`)

For durable, at-least-once messaging with idempotency.

```csharp
public interface IJetStreamPublisher
{
    /// <param name="messageId">Required. Business-key-derived ID for de-duplication.</param>
    Task PublishAsync<T>(string subject, T message, string? messageId,
        MessageHeaders? headers = null, CancellationToken cancellationToken = default);
}
```

### JetStream Consuming (`IJetStreamConsumer`)

For durable message consumption with acknowledgment control.

```csharp
public interface IJetStreamConsumer
{
    // Push consumer (server sends messages)
    Task ConsumeAsync<T>(StreamName stream, SubjectName subject,
        Func<IJsMessageContext<T>, Task> handler,
        JetStreamConsumeOptions? options = null, CancellationToken cancellationToken = default);

    // Pull consumer (client requests messages with back-pressure)
    Task ConsumePullAsync<T>(StreamName stream, ConsumerName consumer,
        Func<IJsMessageContext<T>, Task> handler,
        JetStreamConsumeOptions? options = null, CancellationToken cancellationToken = default);
}
```

---

## 3. Message Context Interfaces

### `IMessageContext<T>`

Base context for core NATS messages.

```csharp
public interface IMessageContext<T>
{
    T Message { get; }
    string Subject { get; }
    MessageHeaders Headers { get; }
    string? ReplyTo { get; }

    Task RespondAsync<TResponse>(TResponse response, CancellationToken cancellationToken = default);
}
```

### `IJsMessageContext<T>`

Extended context for JetStream messages with acknowledgment control.

```csharp
public interface IJsMessageContext<T> : IMessageContext<T>
{
    // Acknowledgment strategies
    Task AckAsync(CancellationToken cancellationToken = default);
    Task NackAsync(TimeSpan? delay = null, CancellationToken cancellationToken = default);
    Task TermAsync(CancellationToken cancellationToken = default);
    Task InProgressAsync(CancellationToken cancellationToken = default);

    // JetStream metadata
    ulong Sequence { get; }
    DateTimeOffset Timestamp { get; }
    bool Redelivered { get; }
    uint NumDelivered { get; }
}
```

---

## 4. State Stores

### Key-Value Store (`IKeyValueStore`)

Abstraction over NATS KV buckets with optional caching.

```csharp
public interface IKeyValueStore
{
    Task PutAsync<T>(string key, T value, CancellationToken cancellationToken = default);

    Task<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default);

    Task DeleteAsync(string key, CancellationToken cancellationToken = default);

    Task WatchAsync<T>(string key, Func<KvChangeEvent<T>, Task> handler,
        CancellationToken cancellationToken = default);

    IAsyncEnumerable<string> GetKeysAsync(IEnumerable<string> patterns,
        CancellationToken cancellationToken = default);
}
```

### Object Store (`IObjectStore`)

Abstraction over NATS Object Store for large binary data.

```csharp
public interface IObjectStore
{
    Task<string> PutAsync(string key, Stream data, CancellationToken cancellationToken = default);

    Task GetAsync(string key, Stream target, CancellationToken cancellationToken = default);

    Task DeleteAsync(string key, CancellationToken cancellationToken = default);

    Task<ObjectInfo?> GetInfoAsync(string key, bool showDeleted = false,
        CancellationToken cancellationToken = default);

    Task UpdateMetaAsync(string key, ObjectMetaInfo meta,
        CancellationToken cancellationToken = default);

    Task<IEnumerable<ObjectInfo>> ListAsync(bool showDeleted = false,
        CancellationToken cancellationToken = default);

    Task WatchAsync(Func<ObjectInfo, Task> handler, CancellationToken cancellationToken = default);
}
```

---

## 5. Topology Management

### `ITopologyManager`

Programmatic creation of NATS JetStream resources.

```csharp
public interface ITopologyManager
{
    Task EnsureStreamAsync(StreamSpec spec, CancellationToken cancellationToken = default);

    Task EnsureConsumerAsync(ConsumerSpec spec, CancellationToken cancellationToken = default);

    Task EnsureBucketAsync(BucketName name, StorageType storageType,
        CancellationToken cancellationToken = default);

    Task EnsureBucketAsync(BucketSpec spec, CancellationToken cancellationToken = default);

    Task EnsureObjectStoreAsync(BucketName name, StorageType storageType,
        CancellationToken cancellationToken = default);

    Task EnsureObjectStoreAsync(ObjectStoreSpec spec, CancellationToken cancellationToken = default);
}
```

### `ITopologySource`

Implement to define your application's topology.

```csharp
public interface ITopologySource
{
    IEnumerable<StreamSpec> GetStreams();
    IEnumerable<ConsumerSpec> GetConsumers();
    IEnumerable<BucketSpec> GetBuckets();          // Default: empty
    IEnumerable<ObjectStoreSpec> GetObjectStores(); // Default: empty
}
```

### `ITopologyReadySignal`

Coordination signal for startup sequencing.

```csharp
public interface ITopologyReadySignal
{
    Task WaitForReadyAsync(CancellationToken cancellationToken = default);
}
```

---

## 6. Dead Letter Queue (DLQ)

### `IDlqStore`

Persistent storage for DLQ message metadata.

```csharp
public interface IDlqStore
{
    Task StoreAsync(DlqMessageEntry entry, CancellationToken cancellationToken = default);

    Task<DlqMessageEntry?> GetAsync(string id, CancellationToken cancellationToken = default);

    Task<IReadOnlyList<DlqMessageEntry>> ListAsync(
        string? filterStream = null,
        string? filterConsumer = null,
        int limit = 100,
        CancellationToken cancellationToken = default);

    Task<bool> UpdateStatusAsync(string id, DlqMessageStatus status,
        CancellationToken cancellationToken = default);

    Task<bool> DeleteAsync(string id, CancellationToken cancellationToken = default);
}
```

### `IDlqRemediationService`

Operations for DLQ message remediation.

```csharp
public interface IDlqRemediationService
{
    Task<DlqMessageEntry?> InspectAsync(string id, CancellationToken cancellationToken = default);

    Task<IReadOnlyList<DlqMessageEntry>> ListAsync(
        string? filterStream = null,
        string? filterConsumer = null,
        DlqMessageStatus? filterStatus = null,
        int limit = 100,
        CancellationToken cancellationToken = default);

    Task<DlqRemediationResult> ReplayAsync(string id, CancellationToken cancellationToken = default);

    Task<DlqRemediationResult> ReplayWithModificationAsync<T>(string id, T modifiedPayload,
        CancellationToken cancellationToken = default);

    Task<DlqRemediationResult> ArchiveAsync(string id, string? reason = null,
        CancellationToken cancellationToken = default);

    Task<DlqRemediationResult> DeleteAsync(string id, CancellationToken cancellationToken = default);
}
```

### `IDlqAdvisoryHandler`

Handle NATS advisory events for delivery failures.

```csharp
public interface IDlqAdvisoryHandler
{
    Task HandleMaxDeliveriesExceededAsync(ConsumerMaxDeliveriesAdvisory advisory,
        CancellationToken cancellationToken = default);
}
```

---

## 7. Middleware

### `IConsumerMiddleware<T>`

Extensible message processing pipeline.

```csharp
public interface IConsumerMiddleware<T>
{
    Task InvokeAsync(IJsMessageContext<T> context, Func<Task> next, CancellationToken ct);
}
```

Built-in implementations:
- `LoggingMiddleware<T>` - Logs message handling with duration
- `ValidationMiddleware<T>` - FluentValidation integration

---

## 8. Distributed Locking

Uses `Medallion.Threading.IDistributedLockProvider` interface.

Registration: `AddFlySwattrNatsDistributedLock()`

---

## 9. Configuration Options

### Primary Configuration Classes

| Class | Description |
|-------|-------------|
| `EnterpriseNatsOptions` | Top-level configuration for Golden Path |
| `NatsConfiguration` | Core NATS connection settings (URL, Auth, TLS) |
| `PayloadOffloadingOptions` | Claim Check pattern settings (threshold, bucket names) |
| `BulkheadConfiguration` | Resilience pool limits and concurrency |
| `FusionCacheConfiguration` | KV caching TTL and fail-safe durations |
| `TopologyStartupOptions` | Startup connection retries and timeouts |
| `NatsConsumerOptions` | Per-consumer settings (middleware, handlers) |
| `NatsConsumerHealthCheckOptions` | Health check thresholds |
| `DlqAdvisoryListenerOptions` | Advisory listener configuration |
| `ConsumerResilienceOptions` | Retry and circuit breaker defaults |
| `TelemetryOptions` | OpenTelemetry tracing and metrics settings |

### Specification Classes

| Class | Description |
|-------|-------------|
| `StreamSpec` | Stream configuration (subjects, retention, storage) |
| `ConsumerSpec` | Consumer configuration (filter, ack policy, DLQ) |
| `BucketSpec` | KV bucket configuration |
| `ObjectStoreSpec` | Object store configuration |
| `DeadLetterPolicy` | DLQ routing configuration |

---

## 10. Value Objects

Strongly-typed identifiers with validation:

| Type | Description |
|------|-------------|
| `StreamName` | Stream identifier (alphanumeric, underscore, hyphen) |
| `ConsumerName` | Consumer identifier |
| `SubjectName` | Subject pattern (supports NATS wildcards: `*` and `>`) |
| `BucketName` | KV/Object store bucket name |
| `QueueGroup` | Queue group name |

---

## 11. Enums

```csharp
public enum StreamRetention { Limits, Interest, WorkQueue }
public enum StorageType { File, Memory }
public enum AckPolicy { None, All, Explicit }
public enum DeliverPolicy { All, Last, New, ByStartSequence, ByStartTime, LastPerSubject }
public enum DlqMessageStatus { Pending, Processing, Resolved, Archived }
```

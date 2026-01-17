# Public API Reference

This document lists the public API surface of the `FlySwattr.NATS` libraries. These are the classes, interfaces, and methods that consumers are expected to use directly. Internal implementation details and helper classes are excluded.

## 1. Configuration & Dependency Injection

The primary entry points for configuring the library are the extension methods on `IServiceCollection`.

### Golden Path (Recommended)
*   **`AddEnterpriseNATSMessaging(Action<EnterpriseNatsOptions> configure)`**
    *   Configures all subsystems (Core, Offloading, Resilience, Caching, Topology, Hosting) in the correct order.

### Modular Registration
*   **`AddFlySwattrNatsCore(Action<NatsConfiguration> configure)`**
    *   Registers base NATS connection, serializers, and core buses.
*   **`AddPayloadOffloading(Action<PayloadOffloadingOptions>? configure)`**
    *   Adds Claim Check pattern support for large messages.
*   **`AddFlySwattrNatsResilience(Action<BulkheadConfiguration>? configure)`**
    *   Adds circuit breakers, bulkheads, and retry policies.
*   **`AddFlySwattrNatsCaching(Action<FusionCacheConfiguration>? configure)`**
    *   Adds L1+L2 caching for Key-Value stores.
*   **`AddFlySwattrNatsTopology()`**
    *   Adds topology management services.
*   **`AddNatsTopologySource<TSource>()`**
    *   Registers a topology definition source.
*   **`AddNatsTopologyProvisioning(Action<TopologyStartupOptions>? configure)`**
    *   Enables auto-provisioning of streams/consumers on startup.
*   **`AddFlySwattrNatsDistributedLock()`**
    *   Adds distributed locking support (via Medallion).
*   **`AddFlySwattrNatsHosting(Action<NatsConsumerHealthCheckOptions>? configure)`**
    *   Adds health checks and startup probes.
*   **`AddNatsConsumer<TMessage>(string stream, string consumer, Func<IJsMessageContext<TMessage>, Task> handler, Action<NatsConsumerOptions>? configure)`**
    *   Registers a background worker for a specific consumer.
*   **`AddNatsDlqAdvisoryListener(Action<DlqAdvisoryListenerOptions>? configure)`**
    *   Enables monitoring of NATS advisory events for delivery failures.

---

## 2. Messaging

### Core Messaging (`IMessageBus`)
For standard publish/subscribe patterns.
*   `Task PublishAsync<T>(string subject, T message, CancellationToken ct)`
*   `Task SubscribeAsync<T>(string subject, Func<IMessageContext<T>, Task> handler, string? queueGroup, CancellationToken ct)`
*   `Task<TResponse?> RequestAsync<TRequest, TResponse>(string subject, TRequest request, TimeSpan timeout, CancellationToken ct)`

### JetStream (`IJetStreamPublisher`, `IJetStreamConsumer`)
For durable, at-least-once messaging.
*   `Task PublishAsync<T>(string subject, T message, string? messageId, CancellationToken ct)`
    *   *Note: `messageId` is required for idempotency.*
*   `Task ConsumeAsync<T>(StreamName stream, SubjectName subject, Func<IJsMessageContext<T>, Task> handler, ...)`
    *   *Supports push-based consumption.*
*   `Task ConsumePullAsync<T>(StreamName stream, ConsumerName consumer, Func<IJsMessageContext<T>, Task> handler, ...)`
    *   *Supports pull-based consumption for better backpressure control.*

---

## 3. State Stores

### Key-Value Store (`IKeyValueStore`)
Abstraction over NATS KV buckets (with optional caching).
*   `Task PutAsync<T>(string key, T value, CancellationToken ct)`
*   `Task<T?> GetAsync<T>(string key, CancellationToken ct)`
*   `Task DeleteAsync(string key, CancellationToken ct)`
*   `Task WatchAsync<T>(string key, Func<KvChangeEvent<T>, Task> handler, CancellationToken ct)`

### Object Store (`IObjectStore`)
Abstraction over NATS Object Store buckets.
*   `Task<string> PutAsync(string key, Stream data, CancellationToken ct)`
*   `Task GetAsync(string key, Stream target, CancellationToken ct)`
*   `Task DeleteAsync(string key, CancellationToken ct)`
*   `Task<ObjectInfo?> GetInfoAsync(string key, bool showDeleted, CancellationToken ct)`
*   `Task<IEnumerable<ObjectInfo>> ListAsync(bool showDeleted, CancellationToken ct)`
*   `Task WatchAsync(Func<ObjectInfo, Task> handler, CancellationToken ct)`

---

## 4. Topology Management

### Manager (`ITopologyManager`)
For programmatic creation of NATS resources.
*   `Task EnsureStreamAsync(StreamSpec spec, CancellationToken ct)`
*   `Task EnsureConsumerAsync(ConsumerSpec spec, CancellationToken ct)`
*   `Task EnsureBucketAsync(BucketName name, StorageType storageType, CancellationToken ct)`
*   `Task EnsureObjectStoreAsync(BucketName name, StorageType storageType, CancellationToken ct)`

### Remediation (`IDlqRemediationService`)
For managing Dead Letter Queues.
*   `Task<DlqMessageEntry?> InspectAsync(string id, CancellationToken ct)`
*   `Task<IReadOnlyList<DlqMessageEntry>> ListAsync(..., CancellationToken ct)`
*   `Task<DlqRemediationResult> ReplayAsync(string id, CancellationToken ct)`
*   `Task<DlqRemediationResult> ArchiveAsync(string id, string? reason, CancellationToken ct)`
*   `Task<DlqRemediationResult> DeleteAsync(string id, CancellationToken ct)`

---

## 5. Configuration Options (DTOs)

These classes are used to configure the services via the `Action<T>` delegates in the DI extensions.

*   **`EnterpriseNatsOptions`**: Top-level configuration object.
*   **`NatsConfiguration`**: Core NATS connection settings (URL, Auth, TLS).
*   **`PayloadOffloadingOptions`**: Settings for Claim Check pattern (Thresholds, Bucket names).
*   **`BulkheadConfiguration`**: Settings for resilience pools and concurrency limits.
*   **`FusionCacheConfiguration`**: Settings for KV caching (TTL, Fail-safe durations).
*   **`TopologyStartupOptions`**: Settings for startup connection retries and timeouts.
*   **`NatsConsumerOptions`**: Advanced settings for individual consumers (Middleware, Poison Handlers).
*   **`StreamSpec` / `ConsumerSpec`**: Definitions for topology resources.

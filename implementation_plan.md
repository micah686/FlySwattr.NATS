# FlySwattr.NATS — Six Improvements Plan

## 1. Object Store Cleanup After Claim-Check Resolution

### Problem

The `OffloadingJetStreamConsumer` resolves claim-check objects from the Object Store but **never deletes them**. Over time, the `claim-checks` bucket will grow unbounded. The `OffloadingJetStreamPublisher` does have compensating cleanup on publish failure, but the happy-path (consumer acks → object is no longer needed) has no cleanup at all.

### Current State

- [OffloadingJetStreamPublisher.cs](file:///home/micah/RiderProjects/FlySwattr.NATS/FlySwattr.NATS.Core/Decorators/OffloadingJetStreamPublisher.cs): Uploads to object store, cleans up on publish failure only.
- [OffloadingJetStreamConsumer.cs](file:///home/micah/RiderProjects/FlySwattr.NATS/FlySwattr.NATS.Core/Decorators/OffloadingJetStreamConsumer.cs): Downloads and deserializes, but never calls `_objectStore.DeleteAsync(...)`.
- [NatsConsumerBackgroundService.cs](file:///home/micah/RiderProjects/FlySwattr.NATS/FlySwattr.NATS.Hosting/Services/NatsConsumerBackgroundService.cs): The production consumer host — where acks happen.
- The `ObjectStoreSpec` (topology) has no `MaxAge` set on the claim-checks bucket today.

### Proposed Design: Three-Layer Strategy

**Layer 1 — Delete-After-Ack (Primary):**

The consumer should delete the object from the store after successful ack. This is the tightest cleanup loop.

#### [MODIFY] [OffloadingJetStreamConsumer.cs](file:///home/micah/RiderProjects/FlySwattr.NATS/FlySwattr.NATS.Core/Decorators/OffloadingJetStreamConsumer.cs)
- Extend `OffloadingMessageContext<T>` to carry the resolved `objectKey` when a claim-check was resolved.
- Add a new `ClaimCheckObjectKey` property (nullable) on the context.

#### [NEW] ClaimCheckCleanupMiddleware.cs
Location: `FlySwattr.NATS.Core/Decorators/ClaimCheckCleanupMiddleware.cs`
- Implement `IConsumerMiddleware<T>` that:
  1. Calls `next()` (lets the handler + ack complete).
  2. If the context carries a `ClaimCheckObjectKey`, fire-and-forget `_objectStore.DeleteAsync(key)` with best-effort error swallowing / logging.
- This avoids blocking the consume path and avoids coupling cleanup to ack timing.

> [!IMPORTANT]
> The middleware approach keeps cleanup decoupled from the consumer decorator. If the delete fails, the object is still cleaned up by Layer 2 or 3.

**Layer 2 — Object Store TTL (Safety Net):**

#### [MODIFY] [TopologySpecs.cs](file:///home/micah/RiderProjects/FlySwattr.NATS/FlySwattr.NATS.Abstractions/Specs/TopologySpecs.cs)
- Add `MaxAge` property to `ObjectStoreSpec` (currently missing).

#### [MODIFY] Topology provisioning code
- When auto-creating the claim-checks bucket, set a default `MaxAge` of 24 hours (configurable via `EnterpriseNatsOptions`).

#### [MODIFY] [EnterpriseNatsOptions.cs](file:///home/micah/RiderProjects/FlySwattr.NATS/FlySwattr.NATS/Configuration/EnterpriseNatsOptions.cs)
- Add `ClaimCheckTtl` property (default: `TimeSpan.FromHours(24)`).

**Layer 3 — Background Sweep (Monitoring / Optional):**

#### [NEW] ClaimCheckCleanupService.cs
Location: `FlySwattr.NATS.Hosting/Services/ClaimCheckCleanupService.cs`
- A `BackgroundService` that periodically (`SweepInterval`, default 1 hour):
  1. Lists objects in the claim-checks bucket.
  2. Deletes objects older than the configured TTL.
  3. Emits telemetry (counter for objects cleaned, gauge for bucket size).
- This is opt-in via `EnterpriseNatsOptions.EnableClaimCheckCleanup`.
- Useful on NATS setups where Object Store `MaxAge` isn't supported or where proactive monitoring is desired.

### Design Decisions

> [!WARNING]
> **Delete-after-ack vs. delete-before-ack tradeoff**: If a consumer crashes after ack but before delete, the object leaks — but Layer 2 TTL catches it. If we delete before ack and the consumer restarts, the redelivered message would fail to resolve. Delete-after-ack is the safe default.

---

## 2. Schema Evolution Strategy

### Problem

The `MemoryPackSchemaEnvelopeSerializer` currently performs **strict** checks — `SchemaVersion` and `SchemaFingerprint` must match exactly — meaning any field change is a hard deserialization failure with no forward/backward compatibility.

### Current State

- [MemoryPackSchemaEnvelopeSerializer.cs](file:///home/micah/RiderProjects/FlySwattr.NATS/FlySwattr.NATS.Core/Serializers/MemoryPackSchemaEnvelopeSerializer.cs): Throws on version or fingerprint mismatch.
- [MemoryPackSchemaMetadata.cs](file:///home/micah/RiderProjects/FlySwattr.NATS/FlySwattr.NATS.Core/Serializers/MemoryPackSchemaMetadata.cs): Computes SHA256 fingerprint from all serializable members.
- [MessageSchemaAttribute.cs](file:///home/micah/RiderProjects/FlySwattr.NATS/FlySwattr.NATS.Abstractions/Attributes/MessageSchemaAttribute.cs): Simple `Version` int.
- Headers already include `Content-Type`, `X-Schema-Id`, `X-Schema-Version`, `X-Schema-Fingerprint`.

### Proposed Design

**Phase 1 — Make the envelope deserializer version-aware (not just strict-match):**

#### [MODIFY] [MemoryPackSchemaEnvelopeSerializer.cs](file:///home/micah/RiderProjects/FlySwattr.NATS/FlySwattr.NATS.Core/Serializers/MemoryPackSchemaEnvelopeSerializer.cs)
- Change version check from equality (`!=`) to compatibility range: allow `envelope.SchemaVersion <= descriptor.SchemaVersion` (newer reader, older writer — backward compat).
- When `envelope.SchemaVersion > descriptor.SchemaVersion`, throw a clear `SchemaVersionTooNewException` explaining the consumer needs to be upgraded.
- **Make the fingerprint check configurable** via a new `EnforceSchemaFingerprint` option (default: `true` — preserving current strict behavior). When enabled, mismatches throw as today. When disabled, mismatches emit a warning log instead of throwing. This lets teams opt into flexible evolution when they understand the MemoryPack positional rules.

#### [MODIFY] [MessageSchemaAttribute.cs](file:///home/micah/RiderProjects/FlySwattr.NATS/FlySwattr.NATS.Abstractions/Attributes/MessageSchemaAttribute.cs)
- Add optional `MinSupportedVersion` property (default: 1) to define how far back a type can accept messages from.

```csharp
[MessageSchema(version: 3, MinSupportedVersion = 2)]
```

This enables explicit backward-compatibility windows.

**Phase 2 — Content-Type version header:**

#### [MODIFY] [HybridNatsSerializer.cs](file:///home/micah/RiderProjects/FlySwattr.NATS/FlySwattr.NATS.Core/Serializers/HybridNatsSerializer.cs)
- Change `ContentTypeMemoryPack` to include a version parameter: `application/x-memorypack; v=1` to signal wire format version.
- Add a `TryParseContentTypeVersion` helper that consumers can use to negotiate.

**Phase 3 — Documentation of safe MemoryPack evolution patterns:**

#### [NEW] Markdown/schema-evolution-guide.md
Document the safe evolution rules for MemoryPack types:
1. **Safe**: Appending new nullable/optional fields at the end.
2. **Safe**: Adding `[MemoryPackIgnore]` fields.
3. **Unsafe**: Removing fields, reordering fields, changing field types.
4. **Required**: Bump `[MessageSchema(version: N)]` for any contract change.
5. **Recommended**: Use `MinSupportedVersion` to define support windows.
6. **Migration pattern**: Side-by-side versioned types (`OrderV1`, `OrderV2`) with a consumer that handles both.

> [!IMPORTANT]
> MemoryPack is positional — field order matters. This is fundamentally different from JSON/Protobuf. The guide must make this crystal clear.

---

## 3. Integration Test Coverage for Failure Modes

### Problem

The existing integration tests cover happy-path scenarios but don't test failure/recovery modes like connection loss, consumer deletion, or network partitions.

### Current Integration Test Suite

| Test File | What It Covers |
|-----------|---------------|
| [NatsTest.cs](file:///home/micah/RiderProjects/FlySwattr.NATS/Tests/IntegrationTests/NatsTest.cs) | Basic container connectivity |
| [PayloadOffloadingIntegrationTests.cs](file:///home/micah/RiderProjects/FlySwattr.NATS/Tests/IntegrationTests/Core/PayloadOffloadingIntegrationTests.cs) | Claim-check publish/consume round-trip |
| [DlqEndToEndIntegrationTests.cs](file:///home/micah/RiderProjects/FlySwattr.NATS/Tests/IntegrationTests/Core/DlqEndToEndIntegrationTests.cs) | DLQ advisory + remediation |
| [StoreWatchIntegrationTests.cs](file:///home/micah/RiderProjects/FlySwattr.NATS/Tests/IntegrationTests/Stores/StoreWatchIntegrationTests.cs) | KV/ObjStore watch |
| [NatsConsumerBackgroundServiceTests.cs](file:///home/micah/RiderProjects/FlySwattr.NATS/Tests/IntegrationTests/Hosting/NatsConsumerBackgroundServiceTests.cs) | Consumer background service |
| Various Core tests | Idempotency, queue groups, wildcards, scatter-gather, request-reply, load |

### Proposed Test Plan

#### [NEW] Tests/IntegrationTests/Chaos/ConnectionResilienceTests.cs
Tests using Testcontainers `nats:latest` with pause/unpause to simulate network issues:

1. **Publisher reconnect**: Publish → pause container → resume → publish again → verify both messages arrive.
2. **Consumer reconnect**: Start consumer → pause container → resume → verify consumer resumes processing.
3. **Consumer with backpressure during outage**: Fill the channel, pause NATS, resume → verify NAKed messages are redelivered.

#### [NEW] Tests/IntegrationTests/Chaos/ConsumerDeletionTests.cs

1. **Ephemeral consumer re-creation**: Delete the NATS consumer while `NatsConsumerBackgroundService` is running → verify it detects deletion and re-subscribes.
2. **Durable consumer config change**: Update consumer config (e.g., change filter subject) while consumer is active → verify the consumer picks up changes on its next loop iteration.

#### [NEW] Tests/IntegrationTests/Chaos/ClaimCheckFailureTests.cs

1. **Object Store unavailable during publish**: Publish a message that exceeds the threshold while the object store is in a degraded state → verify the publisher retries and eventually succeeds or fails cleanly.
2. **Object Store unavailable during consume**: Consumer receives a claim-check message but object store is temporarily unavailable → verify NAK and redelivery.
3. **Orphaned object cleanup**: Publish a large message, ack on consumer side, verify the object is eventually deleted (once Layer 1 cleanup is implemented).

#### [NEW] Tests/IntegrationTests/Chaos/SchemaEvolutionTests.cs

1. **Forward-compatible read**: Publish with schema V2 (extra field), consume with V1 reader → verify successful deserialization (once schema evolution changes land).
2. **Backward-compatible read**: Publish with V1, consume with V2 reader → verify defaults for new fields.
3. **Incompatible version rejection**: Publish with V3, consume with V1 reader where `MinSupportedVersion = 2` → verify clear error.

> [!NOTE]
> The Testcontainers `pause`/`unpause` approach is preferred over `stop`/`start` because it simulates network partition without losing container state. For NATS, use the Docker API: `await container.PauseAsync()` / `await container.ResumeAsync()`.

---

<!-- ## 4. Multi-Tenant Subject Prefixing — DEFERRED

This feature is too complex for the current iteration. It requires cross-cutting changes
to subjects, topology provisioning, and decorator ordering. Revisit after the other five
items are stable.

Key design notes for future reference:
- Core concept: ITenantContext ambient value + subject prefix decorators
- Decorator chain position: Core → Offloading → Tenant → Resilience
- Open question: shared streams with wildcard subjects vs. per-tenant streams
- SubjectName value object already supports dots, so no VO changes needed
-->

---

## 5. Batch Publishing API

### Problem

The current `IJetStreamPublisher.PublishAsync` publishes one message at a time. For high-throughput producers, this means one round-trip per message, which is a significant throughput bottleneck.

### Proposed Design

#### [MODIFY] [IMessaging.cs](file:///home/micah/RiderProjects/FlySwattr.NATS/FlySwattr.NATS.Abstractions/Interfaces/IMessaging.cs)
Add a batch publish method to `IJetStreamPublisher`:

```csharp
/// <summary>
/// Publishes a batch of messages to JetStream subjects.
/// Messages are published concurrently and all acks are awaited.
/// </summary>
Task PublishBatchAsync<T>(
    IReadOnlyList<BatchMessage<T>> messages,
    CancellationToken cancellationToken = default);
```

#### [NEW] FlySwattr.NATS.Abstractions/DTOs/BatchMessage.cs
```csharp
public record BatchMessage<T>(
    string Subject,
    T Message,
    string MessageId,
    MessageHeaders? Headers = null);
```

#### [MODIFY] [NatsJetStreamBus.cs](file:///home/micah/RiderProjects/FlySwattr.NATS/FlySwattr.NATS.Core/NatsJetStreamBus.cs)
Implement `PublishBatchAsync<T>`:
1. Validate all messages upfront (subjects, message IDs, headers).
2. Serialize all messages.
3. Publish all messages concurrently using `Task.WhenAll` on individual `_jsContext.PublishAsync` calls.
4. Collect all ack results and call `EnsureSuccess()` on each.
5. If any fail, throw an `AggregateException` with details of which messages failed.

> [!NOTE]
> NATS.Net v2 doesn't have a native batch publish API — the client sends individual publishes over a single TCP connection. However, publishing concurrently without awaiting each ack individually leverages NATS's pipelining behavior. The key optimization is **not awaiting each ack sequentially**.

**Error semantics**: If any messages in the batch fail, the method throws an `AggregateException` containing one inner exception per failed message. The exception message includes the index and message ID of each failure so callers can identify which messages need retry.

#### [MODIFY] [OffloadingJetStreamPublisher.cs](file:///home/micah/RiderProjects/FlySwattr.NATS/FlySwattr.NATS.Core/Decorators/OffloadingJetStreamPublisher.cs)
Forward `PublishBatchAsync` to inner, with per-message offloading decision:
- Messages exceeding threshold: upload to object store, then publish placeholder.
- Messages under threshold: pass through directly.
- All uploads can happen concurrently before the batch publish.

#### Resilience decorator
The resilience decorators also need to forward `PublishBatchAsync`. The simplest approach: wrap the entire batch in a single resilience execution, since partial failures in a batch are complex to retry individually.

---

## 6. Eager `NatsConnection.ConnectAsync()` at Startup

### Problem

`NatsConnection` is lazy — it connects on the first operation. For publisher-only services, this means the first publish absorbs connection latency (typically 50–200ms, or much more if NATS is slow to start). This is an operational concern for latency-sensitive services.

### Current State

- [ServiceCollectionExtensions.cs](file:///home/micah/RiderProjects/FlySwattr.NATS/FlySwattr.NATS.Core/Extensions/ServiceCollectionExtensions.cs#L41-L58): Creates `NatsConnection` as a singleton but never calls `ConnectAsync()`.
- The topology provisioning service in `FlySwattr.NATS.Topology` may implicitly trigger connection, but only if topology provisioning is enabled.

### Proposed Design

#### [NEW] FlySwattr.NATS.Hosting/Services/NatsConnectionWarmupService.cs
A simple `IHostedService` that eagerly connects:

```csharp
internal class NatsConnectionWarmupService : IHostedService
{
    private readonly NatsConnection _connection;
    private readonly ILogger<NatsConnectionWarmupService> _logger;
    private readonly TimeSpan _connectTimeout;

    public NatsConnectionWarmupService(
        NatsConnection connection,
        ILogger<NatsConnectionWarmupService> logger,
        TimeSpan? connectTimeout = null)
    {
        _connection = connection;
        _logger = logger;
        _connectTimeout = connectTimeout ?? TimeSpan.FromSeconds(30);
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Warming up NATS connection...");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(_connectTimeout);
        try
        {
            await _connection.ConnectAsync();
            _logger.LogInformation("NATS connection established. State: {State}", _connection.ConnectionState);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw; // App shutting down
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "NATS connection warmup failed. Connection will be retried lazily.");
            // Don't throw — allow the app to start and connect lazily
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
```

#### [MODIFY] [EnterpriseNatsOptions.cs](file:///home/micah/RiderProjects/FlySwattr.NATS/FlySwattr.NATS/Configuration/EnterpriseNatsOptions.cs)
- Add `bool EagerConnect { get; set; } = true;` (default enabled for the "Golden Path").
- Add `TimeSpan ConnectionWarmupTimeout { get; set; } = TimeSpan.FromSeconds(30);`

#### [MODIFY] [ServiceCollectionExtensions.cs (top-level)](file:///home/micah/RiderProjects/FlySwattr.NATS/FlySwattr.NATS/Extensions/ServiceCollectionExtensions.cs)
- When `EagerConnect` is true, register `NatsConnectionWarmupService` as an `IHostedService`.
- Register it **before** topology provisioning so the connection is ready before any JetStream operations.

> [!TIP]
> The warmup service is deliberatly non-fatal — if NATS is temporarily unavailable at startup (e.g., Kubernetes sidecar race), the app starts normally and the lazy connection kicks in. This matches the existing topology startup resilience philosophy.

---

## Implementation Priority

| Priority | Feature | Effort | Risk |
|----------|---------|--------|------|
| 1 | Eager ConnectAsync | Small | Low — standalone `IHostedService`, no breaking changes |
| 2 | Claim-Check Cleanup | Medium | Low — additive middleware + config changes |
| 3 | Schema Evolution | Medium | Medium — changes deserialization behavior |
| 4 | Batch Publishing | Medium | Low — additive API, decorators forward through |
| 5 | Integration Tests | Large | Low — purely additive test code |
| — | ~~Multi-Tenant~~ | ~~Large~~ | Deferred — too complex for current iteration |

## Resolved Decisions

- **Multi-Tenant Isolation**: Deferred to a future iteration.
- **Batch Publish Error Semantics**: Use `AggregateException` with per-message inner exceptions.
- **Schema Fingerprint Check**: Configurable via `EnforceSchemaFingerprint` option, **default enabled** (strict mode preserved).

## Verification Plan

### Automated Tests
- All new code should have unit tests (TUnit + NSubstitute).
- Chaos integration tests use `NatsContainerFixture` with `PauseAsync`/`ResumeAsync`.
- Run `dotnet run --project Tests/UnitTests/UnitTests.csproj` and `dotnet run --project Tests/IntegrationTests/IntegrationTests.csproj`.

### Manual Verification
- Schema evolution consumer compatibility tested with mismatched producer/consumer versions.
- Batch publish throughput benchmarked (messages/sec) against sequential publish.
- Eager connect verified with publisher-only service startup time measurement.

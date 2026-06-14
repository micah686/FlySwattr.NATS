# FlySwattr.NATS — Full Project Evaluation

> **Scope note.** The evaluation prompt describes "FrostStream", a distributed media-archiving system with WebAPI, DataBridge, Worker, Scheduler, ControlPlane, Postgres, Quartz.NET, and yt-dlp. **None of those components exist in this repository.** This repo contains only **FlySwattr.NATS**: an 8-package .NET 10 wrapper library over NATS.Net 2.7.0 (JetStream, KV, Object Store), plus unit/integration/benchmark test projects and samples. The library appears to be the messaging substrate that a system like FrostStream would consume. Sections of the requested analysis that target FrostStream services (database ownership, yt-dlp pipeline, Quartz scheduling, WebAPI design) are answered as **N/A — not present in this repository**, with notes on what the library implies for such consumers. Everything else is evaluated against the actual code.
>
> Evaluation basis: full source read of all 8 library projects (~15k lines of library code, ~39.5k including tests), build (`dotnet build`, 0 warnings with `TreatWarningsAsErrors`), 511/511 unit tests green, 92–94/95 integration tests green (flaky failures are Testcontainers/port-collision environmental issues, see Testing). Behavioral claims about NATS.Net were verified against a live `nats:2.10` server where noted.

---

# Executive Summary

FlySwattr.NATS is a genuinely ambitious, mostly well-executed enterprise wrapper for NATS.Net. The architecture is clean and layered: `Abstractions` (interfaces, Vogen value objects, DTOs) → `Core` (buses, serializers, stores, DLQ) → orthogonal add-ons (`Resilience`, `Caching`, `Topology`, `Hosting`, `DistributedLock`) → a "Golden Path" orchestration package (`FlySwattr.NATS`). The decorator chain (Core → Offloading → Resilience) is enforced at registration time with marker services, message IDs are *required* for JetStream publishes (correct idempotency posture), the MemoryPack schema-envelope strategy is a thoughtful answer to positional-serialization drift, and the DLQ pipeline (poison handler → DLQ stream + KV entry store → remediation service with audit hooks) is more complete than most in-house messaging layers ever get.

That said, the deep read found a cluster of **real correctness defects** that the (otherwise broad) test suite does not catch because they live exactly at the seams the tests mock out:

1. **`RespondAsync` on every JetStream message context always throws.** `NatsJSMsg<T>` does not implement `INatsMsg<T>`, so the `_msg is INatsMsg<T>` cast in both `JsMessageContext<T>` and `JsMessageContextWrapper<T>` can never succeed (verified against a live server). Related: `IJsMessageContext.ReplyTo` exposes the JetStream **ack subject** (`$JS.ACK.…`) as if it were a reply subject.
2. **DLQ replay of offloaded payloads is structurally broken in every supported configuration.** `IDlqRemediationService` is registered with `sp.GetService<IObjectStore>()` (unkeyed), but the library only ever registers *keyed* `IObjectStore` instances — so `_objectStore` is always `null` and `ReplayAsync` fails with "Unable to retrieve payload for replay" for any payload > 1 MB.
3. **`ResilientJetStreamConsumer` disposes the per-consumer semaphore immediately after the consumer *starts*** (the consume call returns once the background service is running), so any explicit-API consumer with `MaxConcurrency` set fails every subsequent message with `ObjectDisposedException`.
4. **Shared DLQ target streams get their subject list clobbered.** `EnsureDlqInfrastructureAsync` does `UpdateStreamAsync` with `Subjects = [thisPolicy.TargetSubject]`, so the second consumer pointing at the same DLQ stream silently removes the first consumer's DLQ subject → subsequent DLQ publishes fail.
5. **Deleted KV keys throw instead of returning null.** `NatsKVStore.GetEntryAsync` throws `NatsKVKeyDeletedException` for deleted keys (verified live); neither `NatsKeyValueStore.GetAsync` nor `NatsDlqStore` catches it.

These are all small, surgical fixes (patches are provided in `FABLE_CODE_REVIEW_FIXES/`, see the companion `FABLE_CODE_REVIEW.MD`). With them applied, the library is in solid shape for production use at the stated scale (10–50 msg/s, multiple workers, large payloads via claim-check).

---

# Overall Rating

| Category | Score | Rationale |
|---|---|---|
| **Architecture** | **8/10** | Clean package layering, decorator chain with order validation, value objects for names, options-first configuration. Docked for DI seams that drift between the two consumer-hosting paths and for `IOptions<T>`-is-always-resolvable being used as a feature flag. |
| **Reliability** | **6/10** | Excellent intent (bounded channels, NAK backoff, ack timeouts, shutdown drain, topology ready signal, zombie health checks), but the confirmed defects above — plus poison deserialization failures tearing down the consume enumerator in the raw path — cost real points. |
| **Maintainability** | **7/10** | Consistent style, source-generated logging, XML docs nearly everywhere. Duplicated wiring between `ConfiguredNatsConsumerHostedService` and `TopologyConsumerHostedService` has already diverged (redaction/failure options). Several docs lie about defaults. |
| **Testability** | **7/10** | 511 unit + 95 integration tests, real NATS via Testcontainers, chaos tests. But fakes mask the worst bugs (`RespondAsync` stubbed, DI resolution untested end-to-end), and per-test containers make integration runs flaky. |
| **Security** | **7/10** | Good: wildcard/subject validation, reserved-header enforcement, claim-check prefix pinning, traversal-safe object keys, exception sanitization, TLS verify force-on, inbound size caps. Gaps: insecure transport allowed by default, DLQ payload-ref headers not prefix-validated in remediation. |
| **Scalability** | **7/10** | Fine for 10–50 msg/s. Bounded-channel consumer with parallel workers is right. Claim-check hydration is serialized in the reader loop; several avoidable copies (`ToArray()` in publish paths, envelope double-buffering) only matter at much higher throughput. |
| **Developer Experience** | **8/10** | `AddEnterpriseNATSMessaging` golden path, topology-as-code with handler mapping, good README. Docked for default-value drift between README/XML docs and code (64 KB vs 1 MB threshold; "headers preserved by default" vs redact-all default). |

---

# Critical Issues

These can cause incorrect behavior, message/data loss, or broken core functionality. All are **confirmed against code** (file:line) and, where noted, against a live NATS server. Patches: see `FABLE_CODE_REVIEW_FIXES/`.

### C1. `RespondAsync` on JetStream contexts always throws `NotSupportedException`
- `FlySwattr.NATS.Core/NatsJetStreamBus.cs:710-726` (`JsMessageContext<T>.RespondAsync`) and `FlySwattr.NATS.Hosting/Services/NatsConsumerBackgroundService.cs:691-700` (`JsMessageContextWrapper<T>.RespondAsync`).
- Verified live: the runtime message type is `NatsJSMsg<T>` which implements `INatsJSMsg<T>` and non-generic `INatsMsg` only — **never** `INatsMsg<T>`. The cast guard is dead code; every call throws.
- Compounding it, `ReplyTo` (`NatsJetStreamBus.cs:695`, `NatsConsumerBackgroundService.cs:659`) returns the JetStream ack subject (verified: `$JS.ACK.VERIFY.ver-con.1.1.1.…`), so the `IsNullOrEmpty(ReplyTo)` guard always passes and callers are misled into thinking the message is a request. Naively "fixing" this by calling `ReplyAsync` would publish the response payload to the **ack subject**, which the server interprets as an ack-protocol message — worse than throwing.
- Impact: any consumer handler that calls `ctx.RespondAsync(...)` on a JetStream message fails 100% of the time; the trace tag `messaging.conversation_id` records ack subjects.

### C2. DLQ remediation cannot resolve the Object Store → replay of offloaded payloads always fails
- Registration: `FlySwattr.NATS.Core/Extensions/ServiceCollectionExtensions.cs:118-128` passes `sp.GetService<IObjectStore>()` into `NatsDlqRemediationService`. The library registers `IObjectStore` **only as keyed services** (`AddPayloadOffloading`, same file, lines 186-191) — there is no unkeyed registration anywhere (verified by repo-wide grep).
- Meanwhile `DefaultDlqPoisonHandler` (`FlySwattr.NATS.Hosting/Services/DefaultDlqPoisonHandler.cs:193-201`) *does* offload >1 MB DLQ payloads via the keyed store and records `PayloadEncoding = "objstore://dlq-payload/…"`.
- Result: `NatsDlqRemediationService.GetPayloadAsync` (`NatsDlqRemediationService.cs:389-417`) finds `_objectStore == null`, returns `null`, and `ReplayAsync` fails with "Unable to retrieve payload for replay" for every offloaded DLQ entry — in the golden path and every modular configuration.

### C3. `ResilientJetStreamConsumer` disposes the per-consumer semaphore while the consumer is running
- `FlySwattr.NATS.Resilience/Decorators/ResilientJetStreamConsumer.cs:106-114` and `:155-163`: the `finally` blocks call `_semaphoreManager.RemoveConsumerSemaphore(consumerKey)` (which **disposes** the `SemaphoreSlim`, `ConsumerSemaphoreManager.cs:81-88`).
- But `_inner.ConsumeAsync(...)` returns as soon as the background consumer **starts** (`NatsJetStreamBus.StartBackgroundConsumerAsync` → `BackgroundTaskManager.StartAsync` returns after `BackgroundService.StartAsync`). The wrapped handler (`WrapHandlerWithSemaphore`, lines 169-188) keeps the disposed semaphore captured; every subsequent message throws `ObjectDisposedException` in `semaphore.WaitAsync()` → NAK loop until MaxDeliver.
- Impact: `EnableResilience` + explicit `IJetStreamConsumer.Consume*Async` with `options.MaxConcurrency` set = consumer that fails every message. (Hosted consumers from `AddNatsConsumer`/`AddNatsTopologyWithConsumers` bypass this decorator and are unaffected.)

### C4. Shared DLQ target stream subjects are clobbered by provisioning
- `FlySwattr.NATS.Topology/Managers/NatsTopologyManager.cs:286-313` (`EnsureDlqInfrastructureAsync`): `CreateStreamAsync` with `Subjects = [policy.TargetSubject]`; on "already exists" it unconditionally `UpdateStreamAsync` with that **single-subject** config.
- If two consumers declare `DeadLetterPolicy` with the same `TargetStream` (the natural pattern: one `ORDERS-DLQ` for all ORDERS consumers) but different `TargetSubject`s, whichever provisions last removes the other's subject from the stream. The displaced consumer's next DLQ publish gets "no stream matches subject" → `DefaultDlqPoisonHandler` NAKs forever (`DefaultDlqPoisonHandler.cs:152-158`) until the server stops redelivering at MaxDeliver; the payload is then preserved only as an advisory-based KV entry **without payload** (`DlqStoreAdvisoryHandler.cs:29-42`). Practical effect: silent DLQ payload loss. It also resets MaxAge/retention on user-customized DLQ streams.

### C5. Deleted KV keys throw `NatsKVKeyDeletedException` through public APIs
- Verified live: `store.GetEntryAsync` on a deleted key throws `NatsKVKeyDeletedException` ("Key was deleted"), not `NatsKVKeyNotFoundException`.
- `FlySwattr.NATS.Core/Stores/NatsKeyValueStore.cs:103-133` (`GetAsync`) catches only `NatsKVKeyNotFoundException` → a *deleted* key surfaces as a logged error + thrown exception instead of `null`/default. Same gap in `NatsDlqStore.GetAsync` (`NatsDlqStore.cs:222-251`) and `UpdateStatusAsync` (`:321-377`); `NatsDlqStore.DeleteAsync` (`:380-409`) calls `GetAsync` first, so deleting an already-deleted DLQ entry throws instead of returning `false`.
- Impact: `IKeyValueStore.GetAsync` violates its own contract ("null if missing") for the entire lifetime of a delete tombstone; DLQ list/inspect flows can blow up after any entry deletion (KV `History = 1` shortens but does not eliminate the window; the delete marker is the latest revision).

---

# High Priority Improvements

### H1. "Payload offloading is configured" fail-fast misfires — modular hosting without offloading cannot start
- `FlySwattr.NATS.Hosting/Services/ConfiguredNatsConsumerHostedService.cs:45-63` and `TopologyConsumerHostedService.cs:135,170-177` treat `GetService<IOptions<PayloadOffloadingOptions>>() != null` as "offloading is configured". In any real host, `IOptions<T>` is *always* resolvable (open-generic options infrastructure), so the check degenerates to "is an object store resolvable?" — and since `AddFlySwattrNatsCore` registers no unkeyed `IObjectStore`, a user who composes `AddFlySwattrNatsCore()` + `AddNatsConsumer(...)` **without** `AddPayloadOffloading()` gets a startup `InvalidOperationException` telling them to configure offloading they never asked for. In the topology path the exception is swallowed per-consumer ("Continuing with remaining consumers") — consumers silently don't start.
- Fix: gate on the `NatsPayloadOffloadingMarker` (it exists for exactly this) or `PayloadOffloadingOptions.ObjectStoreServiceKey != null`.

### H2. Default in-process retry pipeline ignores its own options and retries permanent failures
- `ConfiguredNatsConsumerHostedService.cs:136-149` and `TopologyConsumerHostedService.cs:284-297` (duplicated code): `BuildDefaultResiliencePipeline` maps `MaxRetryAttempts`/`UseJitter` but **not** `InitialRetryDelay`/`MaxRetryDelay` — Polly's default 2 s exponential base applies regardless of configuration (`ConsumerResilienceOptions.cs:22-28` documents 1 s/30 s). It also `Handle<Exception>()`, so `MessageValidationException` (explicitly designed as a *permanent* failure that should go straight to DLQ — `MessageValidationException.cs:5-10`) is retried 3× in-process first, re-running the entire middleware pipeline. And because `IOptions<ConsumerResilienceOptions>` is always resolvable, this pipeline is **always on** for hosted consumers unless a custom key is supplied — ~14 s of hidden in-process delay per poison message before the poison handler ever sees it, eating into `AckWait` (default 30 s) and risking concurrent redelivery.
- Also note the circuit-breaker half of `ConsumerResilienceOptions` (failure ratio/throughput/break duration) is wired into options but **never used** to build anything.

### H3. Raw-path deserialization failures tear down the consume enumerator instead of routing to the poison handler
- `NatsConsumerBackgroundService.cs:310-335`: in the raw/offloading path (`UseRawConsumption()`, which is the golden-path default), claim-check hydration and `_serializer.Deserialize<T>` run inside `ReadContextsAsync` — i.e., inside the `await foreach` that feeds the channel. A poison payload (schema mismatch, fingerprint mismatch, missing claim-check object, oversized payload) throws out of the enumerator into `ExecuteAsync`'s catch (`:257-265`), which logs `LogPullConsumerLoopFailed`, sleeps 1 s, and **recreates the entire subscription**. The message is never NAK'd/TERM'd/DLQ'd by the library; it redelivers after `AckWait`, kills the enumerator again, and only lands (payload-less) in the KV DLQ store when the server's MaxDeliver advisory finally fires. Each cycle disrupts delivery for *all* messages on that consumer.
- Contrast: handler exceptions are correctly routed to `IPoisonMessageHandler` (`:467-482`). Deserialization failures deserve the same treatment (Term + DLQ-with-raw-payload is achievable in the raw path because the raw bytes are in hand).

### H4. DLQ payload offloads share the claim-check bucket and its TTL
- `DefaultDlqPoisonHandler.cs:196` writes `dlq-payload/...` objects into the same keyed store (= `claim-checks` bucket) used for claim-check offloading. That bucket is auto-provisioned with `MaxAge = ClaimCheckTtl` (default **24 h**, `TopologyProvisioningService.cs:214-235`, `EnterpriseNatsOptions.cs:166`), and the optional Layer-3 sweep (`ClaimCheckCleanupService.cs:62-88`) deletes **any** object older than TTL regardless of prefix.
- Net effect: a DLQ entry whose payload exceeded 1 MB is replayable for at most 24 h (after C2 is fixed); after that, `ReplayAsync` returns "Unable to retrieve payload". DLQ remediation routinely happens days later. DLQ payloads need their own bucket (or at minimum the sweep must be prefix-scoped and the TTL story documented).

### H5. `ConfiguredNatsConsumerHostedService` fetches the consumer before topology is ready
- `ConfiguredNatsConsumerHostedService.cs:41-42` calls `jsContext.GetConsumerAsync` in `StartAsync`, but the topology-ready wait happens later, inside the worker's `ExecuteAsync` (`NatsConsumerBackgroundService.cs:191-209`). Ordering only works because `TopologyProvisioningService` is a blocking `IHostedService` *if it was registered first*. With `AddNatsConsumer` called before `AddEnterpriseNATSMessaging`, or with `ServicesStartConcurrently`, startup throws "consumer not found". `TopologyConsumerHostedService` does this correctly (waits for the signal first, `TopologyConsumerHostedService.cs:51-56`).
- Related reliability note: when provisioning partially fails, `TopologyReadySignal.SignalFailed` causes consumers to log and **silently never start** (`NatsConsumerBackgroundService.cs:204-208`), and they unregister from health metrics — so the zombie health check can't see them. A failed-topology host runs indefinitely with missing consumers and a green `nats_consumer_health` check.

### H6. Over-broad transient-error classification retries permanent errors
- `NatsObjectStore.cs:98-116` and `ResilientJetStreamPublisher.cs:110-116`: the `ex is NatsException` fallback matches the **entire** NATS exception tree (verified: `NatsJSApiException → NatsJSException → NatsException`; `NatsObjNotFoundException → NatsObjException → NatsJSException → …`). So 400-class JS API errors and `NatsObjNotFoundException` are retried 3× with backoff: a missing claim-check object costs ~6 s of pointless retries per hydration attempt, repeated on every redelivery; permanently-failing publishes burn 3 retries before surfacing. The explicit `503 or 504` arm is unreachable-in-effect because the base-type check matches first in the boolean chain.

---

# Medium Priority Improvements

- **M1. Non-seekable stream retry can silently upload truncated objects.** `NatsObjectStore.PutAsync` (`NatsObjectStore.cs:118-153`) retries transient failures and only rewinds when `data.CanSeek`. A non-seekable stream partially consumed by attempt 1 gets its *remaining* bytes uploaded by attempt 2 — success is reported with corrupt content. Don't retry non-seekable puts.
- **M2. DLQ header redaction default contradicts docs and corrupts replays.** `DlqHeaderRedactionOptions.RedactHeaders` defaults to `true` with an empty allowlist → **all** header values stored as `[REDACTED]` (`DefaultDlqPoisonHandler.FilterHeaders`, `:288-307`), while `EnterpriseNatsOptions.cs:189-193` documents "By default, all headers are preserved verbatim." On replay, `CreateReplayHeaders` (`NatsDlqRemediationService.cs:527-542`) republishes those literal `[REDACTED]` values as live headers.
- **M3. `JetStreamConsumeOptions.MaxConcurrency` is a no-op in the explicit consume path.** `BasicNatsConsumerService.ExecuteAsync` (`NatsJetStreamBus.cs:558-665`) awaits the handler inline inside `await foreach`; processing is strictly sequential, so the semaphore never gates anything. Either dispatch handlers to workers (as the Hosting service does) or document that explicit-API consumption is sequential.
- **M4. `IMessageBus.RequestAsync` contract mismatch.** XML doc says "default if no response is received" (`IMessaging.cs:50`), but NATS.Net throws `NatsNoReplyException` on timeout. Pick one; today callers writing `if (result is null)` get an unhandled exception on the very path they tried to handle.
- **M5. Duplicated, already-diverged consumer wiring.** `ConfiguredNatsConsumerHostedService` builds `DefaultDlqPoisonHandler` manually and omits `DlqStoreFailureOptions`/`DlqHeaderRedactionOptions` (`ConfiguredNatsConsumerHostedService.cs:99-108`); `TopologyConsumerHostedService` uses `ActivatorUtilities` and picks both up (`TopologyConsumerHostedService.cs:152-156`). Same divergence risk in the two `BuildDefaultResiliencePipeline` copies. Extract a shared factory.
- **M6. Stream config drift checks are weaker for DLQ streams than for user streams.** `EnsureStreamAsync` has `StreamConfigChanged` + `ValidateStreamUpdate` guards (`NatsTopologyManager.cs:139-177`); `EnsureDlqInfrastructureAsync` bypasses both (see C4).
- **M7. Default `ConsumerSpec.MaxDeliver = -1`** (`TopologySpecs.cs:51`) means unlimited redeliveries: with the default poison handler, a poison message NAK-loops forever at 30 s intervals and `isRetryExhausted` never fires (`DefaultDlqPoisonHandler.cs:87`: `NumDelivered >= long.MaxValue`). The README example sets `MaxDeliver = 5`; the default should be a finite number or provisioning should warn when a DLQ policy exists with unlimited deliveries.
- **M8. Schema fingerprint measures CLR shape, not MemoryPack wire shape.** `MemoryPackSchemaMetadata.ComputeFingerprint` (`MemoryPackSchemaMetadata.cs:68-124`) hashes all fields+properties in metadata-token order but ignores `[MemoryPackIgnore]`, `[MemoryPackOrder]`, and constructor-selection semantics. Adding an ignored property breaks the fingerprint (false rejection of wire-compatible messages); attribute-driven reordering keeps it stable (false acceptance). Compute the fingerprint from MemoryPack's actual serializable-member model, or document the conservative behavior.
- **M9. Claim-check cleanup vs. multi-consumer streams.** `ClaimCheckCleanupMiddleware` deletes the offloaded object after the **first** consumer's ack. If a second durable consumer on the same subject hasn't processed the message yet, its hydration fails (`"was not found in object storage"`). The middleware is opt-in, but nothing warns about this; document it and recommend per-consumer-set TTL cleanup instead when fan-out exists.
- **M10. `NatsConnectionWarmupService` requires the concrete `NatsConnection`** (`NatsConnectionWarmupService.cs:29-37`) — fine in the golden path, breaks if a consumer swaps the connection registration for `INatsConnection`.
- **M11. Integration tests are flaky by construction.** Each test news up its own `NatsContainerFixture` (`Tests/IntegrationTests/...`), so a full run starts dozens of containers; observed failure: `rootlessport listen tcp 0.0.0.0:42671: bind: address already in use` (podman port race). CI papers over it with `--retry-failed-tests 3` (`.github/workflows/dotnet.yml:31-34`). Use a shared assembly-level fixture.
- **M12. `NatsConfiguration.MaxConcurrency` does nothing.** Configured, validated (`NatsConfigurationValidator.cs:56-58`), copied from `EnterpriseNatsOptions` (`ServiceCollectionExtensions.cs:130`), documented in the README ("Maximum concurrent operations") — and never read by any runtime path. Remove it or wire it.
- **M13. Insecure transport is the default.** `AllowInsecureTransport = true` (`NatsConfiguration.cs:55`) keeps `nats://` valid in production. The mechanism to harden exists (validator rejects plaintext when `false`); flip the recommendation in docs, and consider warning at startup when a non-localhost plaintext URL is used.

# Low Priority / Nice-to-Have

- `PublishRawAsync`/`PublishBytesAsync` copy payloads via `ToArray()` (`NatsJetStreamBus.cs:271,325`); a pooled or `ReadOnlyMemory`-aware `INatsSerialize` avoids it.
- `ExecuteHandlerWithResilienceAsync` rebuilds the middleware closure chain and allocates `_middlewares.Reverse()` per message (`NatsConsumerBackgroundService.cs:541-563`); precompute once per consumer.
- `MemoryPackSchemaEnvelopeSerializer.Serialize` double-buffers the payload (acknowledged in code comments) — a custom formatter could splice bytes without a wire change later.
- `NatsObjectStore.GetInfoAsync(showDeleted: true)` falls back to an O(n) `ListAsync` scan (`NatsObjectStore.cs:239-251`).
- `BackgroundTaskManager.DisposeAsync` awaits all tasks with no timeout (`BackgroundTaskManager.cs:61-69`); a hung handler can stall host shutdown beyond the host's own timeout.
- `HierarchicalResilienceBuilder.DisposeAsync` logs the pipeline count after clearing the dictionary (always 0).
- `PublishBatchAsync` leaks its `SemaphoreSlim` (no dispose) per call (`NatsJetStreamBus.cs:199-201`) — harmless but untidy.
- `AddFlySwattrNatsCaching` hardcodes `WithDefaultEntryOptions` (5 min/1 h/500 ms) instead of using `FusionCacheConfiguration` (`Caching/Extensions/ServiceCollectionExtensions.cs:55-62`); runtime per-entry options mask it, but it's misleading.
- Doc drift to clean up: README says offload threshold default is 64 KB (code default is 1 MB; 64 KB only via golden path, `PayloadOffloadingOptions.cs:26` vs `EnterpriseNatsOptions.cs:47-50`); `WIRE_COMPATIBILITY.md` lists Content-Type values (`application/x-memorypack-envelope`) that the code never emits (`HybridNatsSerializer.cs:22-23`).

---

# Boundary Violations

**N/A for the FrostStream rules as written** — there is no database, no DataBridge, and no service code in this repository, so "only DataBridge accesses the database" cannot be violated here.

Within the library's own intended boundaries:

- The package layering is honored: `Abstractions` has no project references; add-on packages depend only on `Abstractions` + `Core`; only the umbrella `FlySwattr.NATS` references everything. Decorator ordering is actively enforced (`ValidateDecoratorOrdering`, markers).
- `InternalsVisibleTo` grants (`FlySwattr.NATS.Core/InternalsVisibleTo.cs`) to test assemblies and `FlySwattr.NATS.Caching` are reasonable; note `Hosting` reaches Core internals like `IRawJetStreamPublisher` indirectly via DI of concrete `NatsJetStreamBus` (`Core/Extensions/ServiceCollectionExtensions.cs:124`) — acceptable, but it means `NatsJetStreamBus` is effectively a public seam.
- One genuine boundary smell: `Topology` takes a dependency on `Medallion.Threading.IDistributedLockProvider` directly (`NatsTopologyManager.cs:28-30`) rather than an abstraction owned by the library — it couples the topology package to the DistributedLock package's third-party choice (and means "no lock provider" silently degrades to unsynchronized provisioning with only a warning).

# Messaging Review

**Subjects & validation.** Publish subjects are validated (no wildcards, token rules) via `MessageSecurity.ValidatePublishSubject` + Vogen `SubjectName`; user header injection is blocked by reserved-header lists and `Nats-*` prefix rejection (`MessageSecurity.cs:261-298`). Claim-check references are prefix-pinned on the consumer and cleanup sides (`ValidateClaimCheckReference`). Good. Gap: `NatsDlqRemediationService.GetPayloadAsync` trusts `entry.PayloadEncoding`/`x-dlq-payload-ref` without the same prefix validation (`NatsDlqRemediationService.cs:393-411`) — operator-trust zone, but inconsistent hardening.

**Idempotency.** Requiring caller-supplied `Nats-Msg-Id` everywhere (publish, batch, raw) is the single best design decision in the library; retries inside `ResilientJetStreamPublisher` reuse the same ID, so server-side dedup holds. DLQ publishes (`dlq-{stream}-{consumer}-{seq}`) and idempotent replays (`replay-{entryId}`) are deterministic. Caveat: JetStream's dedup window (default 2 min) is not configured by topology provisioning — document that replays of the same entry more than 2 min apart will duplicate.

**Consumers.** The Hosting consumer (`NatsConsumerBackgroundService`) is the strongest component: bounded channel sized to parallelism, explicit backpressure logging, ack-with-timeout, NAK-drain on shutdown, in-progress heartbeats, zombie metrics. Issues: H3 (poison in the enumerator), C1 (`RespondAsync`), and the `MaxMsgs = MaxConcurrency` coupling (consume batch size doubles as worker count — fine at this scale, worth separating later). The `BasicNatsConsumerService` used by the explicit API is sequential (M3) and NAKs with a flat 5 s delay rather than the exponential backoff the poison handler uses — two retry personalities for the same library.

**Retry/backoff/DLQ.** Transient failures: NAK with exponential backoff capped at 30 s (`DefaultDlqPoisonHandler.cs:168-172`) — correct. Permanent (`MessageValidationException`) and exhausted (`NumDelivered >= MaxDeliver`): publish full-payload `DlqMessage` to the DLQ stream, persist a KV entry, notify, then `Term` — a genuinely good Store+Notify pattern, with explicit failure policies (`DlqStoreFailureOptions`). The advisory listener provides the fallback path for non-hosted consumers (metadata-only entries, clearly logged as such). Defects C2/C4/H4 undermine the remediation half of this story; fix those and the DLQ design is better than most.

**Request/reply.** Core NATS request/reply (`NatsMessageBus.RequestAsync`) is correct, with trace propagation; the timeout surfaces as `NatsNoReplyException` contrary to docs (M4). JetStream `RespondAsync` is broken (C1) — and conceptually, JetStream-stored messages don't carry the requester's reply subject, so the right fix is an honest `NotSupportedException` plus (if desired) a header-based reply convention as a feature.

**Reconnect/outage behavior.** Subscription loops (core bus, KV watch, object watch, advisory listener) all reconnect with backoff and survive handler exceptions. Topology provisioning has cold-start ping retry with a global timeout. `NatsConnection` reconnection is delegated to NATS.Net with `ReconnectWaitMin`/`MaxReconnectRetry` pass-through. Reasonable. Watch-based invalidation in `CachingKeyValueStore` is per-instance only — cross-node staleness up to `MemoryCacheDuration` is an accepted (documented) tradeoff.

**Schema/versioning.** The MemoryPack envelope (magic + schemaId + version + fingerprint + payload) with `[MessageSchema]`, min-supported version, and `SchemaVersionTooNewException` is a solid design; headers mirror schema metadata for DLQ inspection; `WireVersionCheckMiddleware` covers protocol-version gating with Log/Term/Nack actions. M8 (fingerprint vs. MemoryPack attributes) is the one real gap.

# Persistence Review

**N/A for Postgres/migrations/transactions** — no database exists in this repository. The library's durable state lives in NATS primitives:

- **KV (`NatsKeyValueStore`)**: thin, metric-instrumented wrapper; double-checked store-handle caching with invalidation on 503/504. C5 (deleted keys) is the correctness gap. `GetKeysAsync` streams; `WatchAsync` distinguishes Put/Del/Purge properly.
- **Object Store (`NatsObjectStore`)**: retry with linear backoff, seek-reset on retry (M1 for non-seekable), idempotent delete, status-check before list (avoids the empty-bucket hang). H6 mis-classification is the main issue.
- **DLQ KV store (`NatsDlqStore`)**: JSON entries (deliberately human-readable — good call, documented in-code), hierarchical keys `{stream}.{consumer}.{seq}` with wildcard filtering, optimistic concurrency via revisions surfaced as `StoreRevision`/`DlqEntryUpdateResult` (Updated/NotFound/Conflict) — textbook OCC, also documented in `Markdown/OptimisticConcurrency.md`. Entry-ID prefix validation prevents store/remediation drift. Bounded-parallel `ListAsync` (16-way) is sensible. C5 applies here too.
- **Distributed lock**: KV `Create`-as-acquire with TTL bucket per-TTL, OCC heartbeats at TTL/3 with revision tracking, tombstone release, `HandleLostToken` on revision theft — a careful implementation. The TTL-suffixed bucket name (`topology_locks_300000ms`) avoids MaxAge collisions across providers; mildly surprising operationally (bucket proliferation per distinct TTL) but correct.

**Consistency/observability**: every store operation records duration histograms with operation/bucket tags, and cardinality is runtime-configurable (`NatsTelemetry.Configure`). Good.

# Worker / Download Pipeline Review

**N/A** — no yt-dlp, media, or file-download code exists in this repository. The relevant analogue, claim-check hydration, is reviewed above (H3/H4/M9): bounded hydration (`MaxHydrationBytes`, pooled buffers with explicit return — `PooledWriteStream`, `NatsConsumerBackgroundService.cs:795-890`) is well done.

# Scheduler / BackgroundProcessor Review

**N/A** — no Quartz.NET or scheduling code exists here. The library's background services (`TopologyProvisioningService`, `DlqAdvisoryListenerService`, `ClaimCheckCleanupService`, consumer hosts) are reviewed under Reliability/High findings.

# API Design Review

**N/A for WebAPI/HTTP** — none exists. The *library* API surface is well-shaped: interfaces in `Abstractions`, options classes per package, value objects for identifiers (`StreamName`, `ConsumerName`, `SubjectName`, `BucketName`, `QueueGroup` — all regex-validated Vogen structs), and an obsoleted migration path (`JetStreamConsumeOptions.QueueGroup` → `DurableName`/`DeliverGroup`) handled with compile-time errors where appropriate (sync lock methods). `PUBLIC_API.md` is maintained. Main API debts: C1's misleading `ReplyTo`/`RespondAsync`, and `string`-typed `FilterSubject`/`TargetSubject` in specs where `SubjectName` exists (inconsistent strong-typing).

# Reliability and Operations

Covered substantially above. Summary of the good: graceful shutdown paths exist everywhere (drain + NAK on the consumer, bounded `WaitAsync` on subscriptions, `IAsyncDisposable` chains); health checks cover connection, JetStream account, and zombie consumers; startup has opt-in fail-fast (`NatsStartupCheck`), eager warmup (non-fatal), and topology cold-start retries with a global timeout. The notable gaps: H5 (failed topology → silently absent consumers invisible to health checks), `BackgroundTaskManager.DisposeAsync` unbounded wait, and the warmup service's concrete-type dependency (M10).

# Security

- **Transport**: TLS options pass through with `InsecureSkipVerify` force-disabled (`Core/Extensions/ServiceCollectionExtensions.cs:60`) — good. Plaintext allowed by default (M13), with an existing opt-in validator to forbid it.
- **Secrets**: `NatsAuthOpts` is passed through, never logged; no credentials in code or samples.
- **Injection surfaces**: subject/header/object-key validation is thorough (wildcard rejection, control-char rejection, `Nats-*` reservation, traversal-segment rejection, claim-check prefix pinning). DLQ remediation's payload refs are the one unvalidated path (operator-zone).
- **Information disclosure**: exception messages sanitized before DLQ/telemetry storage (`SanitizeExceptionMessage`, 256-char cap, control-char stripping, FluentValidation field-name-only summaries); DLQ header redaction exists (default behavior mismatch = M2).
- **DoS**: inbound and outbound payload caps (`MaxPayloadSize` enforced in serializer both directions, envelope inner-payload cap, hydration caps); bounded channels; bounded batch publish concurrency; bounded DLQ list parallelism. Unbounded redelivery with default `MaxDeliver = -1` (M7) is the residual risk.
- **Deserialization**: no polymorphic type resolution from message data; `X-ClaimCheck-Type` maps through an explicit alias registry with `RequireExplicitAliases` defaulting to true — no `Type.GetType` on attacker input. Good.

# Testing Plan

Current state: 511 unit tests (TUnit + NSubstitute + Shouldly), 95 integration tests against real NATS (Testcontainers), benchmarks, chaos tests (network drop, schema evolution, claim-check failure). That's a strong base. The failures found in this review share a theme: **the seams between DI wiring, NATS.Net's real types, and runtime behavior are tested with fakes that are more capable than reality** (e.g., test contexts stub `RespondAsync`; no test resolves `IDlqRemediationService` from the real container and replays an offloaded payload).

Priority-ordered additions (names indicative):

1. `JsMessageContext_RespondAsync_OnRealJetStreamMessage_*` — integration: consume a real JS message, assert current behavior, then the fixed contract (C1).
2. `DlqRemediation_ReplayOffloadedPayload_EndToEnd` — golden-path container test: force a >1 MB poison message through `DefaultDlqPoisonHandler`, then `ReplayAsync` and assert the consumer receives the original payload (C2 + H4).
3. `ResilientConsumer_WithMaxConcurrency_ProcessesMessagesAfterStart` — integration with `EnableResilience` + explicit consume + `MaxConcurrency` (C3).
4. `EnsureConsumer_TwoDlqPoliciesSameTargetStream_PreservesBothSubjects` — topology manager integration (C4).
5. `KeyValueStore_GetAsync_DeletedKey_ReturnsNull` / `DlqStore_GetAsync_DeletedEntry_ReturnsNull` / `DlqStore_DeleteAsync_AlreadyDeleted_ReturnsFalse` (C5).
6. `AddNatsConsumer_WithoutPayloadOffloading_StartsSuccessfully` — host-level DI test (H1).
7. `DefaultRetryPipeline_HonorsConfiguredDelays` and `DefaultRetryPipeline_DoesNotRetryValidationFailures` (H2).
8. `RawConsumer_PoisonPayload_IsTerminatedAndDlqd_WithoutLoopRestart` — publish garbage bytes onto an offloading consumer's subject; assert the consume loop survives and a DLQ entry with raw payload exists (H3).
9. `ObjectStore_PutAsync_NonSeekableStreamFailure_DoesNotRetruncate` (M1); `ObjectStore_GetAsync_Missing_NoRetryDelay` (H6).
10. `DlqReplay_RedactedHeaders_NotForwarded` (M2).
11. Test-infra: consolidate to one shared NATS container per assembly (fixes M11 flakiness; removes the need for CI retries).

# Refactoring Plan

**Phase 1 — safety/correctness (small, surgical; patches provided):** C1–C5, H1, H2, H6, M1, M2. None changes wire formats; only C1's `ReplyTo` adjustment is an observable behavioral change (ack subject no longer exposed as `ReplyTo`).

**Phase 2 — architecture cleanup:** unify the two consumer-host wiring paths behind one factory (M5); gate offloading via marker everywhere; move DLQ payload offloads to a dedicated bucket with its own retention (H4); make `EnsureDlqInfrastructureAsync` merge-and-validate like `EnsureStreamAsync` (C4 follow-through); strongly-type `FilterSubject`/`TargetSubject`.

**Phase 3 — reliability/observability:** route raw-path deserialization failures to the poison handler (H3); surface topology-failure state in `NatsConsumerHealthCheck` (H5); wire (or remove) the dormant circuit-breaker options in `ConsumerResilienceOptions`; bound `BackgroundTaskManager.DisposeAsync`; align `BasicNatsConsumerService` NAK backoff with the poison handler's policy.

**Phase 4 — performance and polish:** eliminate `ToArray()` copies in raw/pre-serialized publish; precompute middleware pipelines; parallelize claim-check hydration into workers; doc-default reconciliation (README/`WIRE_COMPATIBILITY.md`/XML docs); remove dead `NatsConfiguration.MaxConcurrency`.

# Suggested Architecture Adjustments

All within the existing direction — no replacement of NATS, the package layout, or the decorator approach is warranted:

1. **Honest JetStream reply semantics.** Either implement reply-over-JetStream via an explicit header convention (`X-Reply-Subject` stamped by a publishing helper, honored by `RespondAsync`), or remove `RespondAsync` from `IJsMessageContext` at the next major version. The current inherited-from-core shape promises something JetStream cannot deliver.
2. **A dedicated `fs-dlq-payloads` bucket** provisioned alongside the DLQ KV bucket, with retention matched to DLQ stream MaxAge (30 d), used by `DefaultDlqPoisonHandler` and `NatsDlqRemediationService`.
3. **One consumer-host composition root.** A single internal `ConsumerHostFactory` consumed by both `AddNatsConsumer` and `AddNatsTopologyWithConsumers`, so options resolution (object store keys, redaction, failure policies, resilience) cannot diverge again.
4. **Feature flags as markers, not options-presence.** The `IOptions<T>`-always-resolves pitfall bit this codebase twice (H1, H2); standardize on marker services for "is feature X registered".
5. **Topology drift reporting.** Provisioning already validates immutable consumer drift; add a dry-run/report mode (list of creates/updates/conflicts) so operators can run it as a check in CI/CD — cheap to add on the existing manager.

# Questions / Unknowns

1. **Is request/reply over JetStream an intended feature?** C1's fix direction (implement via headers vs. remove) depends on this.
2. **Is multi-consumer fan-out on offloaded subjects expected?** Determines how aggressive claim-check cleanup (M9) may be, and whether Layer-1 cleanup should be default-off in docs.
3. **Intended DLQ replayability horizon?** H4's fix (dedicated bucket + retention) needs a product answer: hours, days, or "until archived"?
4. **Is the `Samples/` tree maintained?** It was excluded from deep review (`NatsTester`, `Service1/2`) and from CI's `Github_CI.slnx`; if it's shipped as documentation, it should be smoke-built in CI.
5. **FrostStream linkage**: `EnterpriseNatsOptions` references "FrostStream parity" in a comment (`EnterpriseNatsOptions.cs:46-50`) — if FrostStream consumes this library, the C1/C2 fixes change behavior its services may have coded around (e.g., catching `NotSupportedException`); coordinate the upgrade.
6. **NuGet release intent**: packages are `GeneratePackageOnBuild` with version 1.0.0 and no README in most packages; if public NuGet release is imminent, the release-blocking items in `FABLE_CODE_REVIEW.MD` should land first.

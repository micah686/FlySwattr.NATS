# FlySwattr.NATS — Release-Readiness Code Review (Second Pass)

**Scope:** FlySwattr.NATS only (8 library projects, tests, samples). No external services or prior-project context were assumed; prior review notes (`Markdown/`, memory of earlier fix rounds) were used only as leads and re-verified against code.
**Method:** Full source read of all library projects; build + full unit and integration runs; targeted behavioral verification against a live `nats:2.10` server and reflection over `NATS.Net 2.7.0` for every claim that depends on NATS.Net semantics. Every confirmed finding below cites file/line against the current tree and ships with a patch in `FABLE_CODE_REVIEW_FIXES/` (verified: all apply cleanly; with all applied the solution builds with 0 warnings, 538/538 unit tests and 97/97 integration tests pass).

---

# Executive Summary

FlySwattr.NATS is **releasable with fixes** — specifically, not before the five Release Blockers below are applied. The library's overall engineering quality is high: enforced publish idempotency, a complete DLQ pipeline with audit hooks, schema-enveloped MemoryPack, validated decorator ordering, and broad test coverage. But the blockers are not edge cases; they sit on advertised, primary features (JetStream reply contexts, DLQ replay, resilience-wrapped consumers, shared DLQ streams) and all were invisible to the existing tests because fakes are more capable than the real NATS.Net types and no test exercises the DI graph end-to-end.

**Top 3 risks:**
1. **DLQ remediation cannot replay offloaded payloads in any supported configuration** — the remediation service resolves an unkeyed `IObjectStore` that the library never registers, and even after that fix, DLQ payloads live in the claim-check bucket whose TTL is 24 hours. The "break glass" tool fails exactly when it's needed.
2. **Shared DLQ target streams get their subject lists clobbered during provisioning** — the second consumer pointing at the same DLQ stream removes the first one's DLQ subject; subsequent poison messages cannot be published to the DLQ and their payloads are effectively lost (only a payload-less advisory KV entry remains).
3. **Resilience-wrapped consumers with `MaxConcurrency` fail every message** with `ObjectDisposedException` because the per-consumer semaphore is disposed the moment the consumer *starts* (the consume call returns at start, not at stop).

With the ten patches applied, remaining risk is concentrated in three design-level items (H-3/H-4/H-5) that should be scheduled but don't have one-line fixes.

# Overall Rating

| Category | Score | One-sentence justification |
|---|---|---|
| Correctness | **5/10** | Five confirmed blockers on primary paths (reply contexts, DLQ replay, resilient consume, DLQ provisioning, deleted-key reads); the surrounding logic is otherwise careful. → ~8/10 with patches applied. |
| Security | **7/10** | Strong input validation (subjects, headers, claim-check prefixes, traversal-safe keys), sanitized errors, forced TLS verification; docked for plaintext-transport default and unvalidated DLQ payload refs in the operator zone. |
| Reliability | **6/10** | Excellent shutdown/backpressure/health design, but poison payloads in the default (raw) consume path restart the whole subscription, and failed topology leaves consumers silently absent. |
| Maintainability | **7/10** | Consistent, documented, well-factored; the two consumer-host wiring paths have already diverged and several docs contradict the code's actual defaults. |
| Testability | **7/10** | 500+ unit and ~95 integration tests with real NATS and chaos coverage, but the worst bugs lived precisely in what was faked; two tests asserted buggy behavior as correct. |
| Operations readiness | **7/10** | Health checks (connection, JetStream, zombie consumers), OTel tracing/metrics with cardinality controls, cold-start provisioning retries; degraded-but-green states (H-5) are the gap. |
| Release confidence | **6/10** | Releasable after blockers; the patch set is small, surgical, and fully test-verified — confidence rises to 8/10 with patches 001–010 applied and H-3/H-4/H-5 scheduled. |

---

# Release Blockers

## [Release Blocker] `RespondAsync` on every JetStream message context always throws; `ReplyTo` exposes the ack subject

- **Status:** Confirmed (verified against live NATS 2.10 and by reflection over NATS.Net 2.7.0)
- **Files:** `FlySwattr.NATS.Core/NatsJetStreamBus.cs:680-727` (`JsMessageContext<T>`), `FlySwattr.NATS.Hosting/Services/NatsConsumerBackgroundService.cs:643-700` (`JsMessageContextWrapper<T>`)
- **Risk:** Any handler calling `ctx.RespondAsync(...)` fails 100% of the time with `NotSupportedException`. Worse, `ctx.ReplyTo` returns the JetStream ack subject (verified: `$JS.ACK.VERIFY.ver-con.1.1.1.…`), so code branching on "is this a request?" is always misled, and the `messaging.conversation_id` trace tag records ack subjects.
- **Evidence:** `NatsJSMsg<T>` implements `INatsJSMsg<T>` and non-generic `INatsMsg` — **not** `INatsMsg<T>` (verified: `boxed is INatsMsg<string> == False`). The guard `if (_msg is INatsMsg<T> natsMsg)` is therefore dead code; the `else` throw is unconditional in practice. A "fix" that calls `ReplyAsync` would publish the response payload onto the ack subject, which the server parses as an ack-protocol message — strictly worse than throwing.
- **Recommended fix:** Make the contract honest: `ReplyTo` returns `null` when the underlying reply subject is a `$JS.ACK.` ack inbox; `RespondAsync` throws a deterministic `NotSupportedException` explaining that JetStream-stored messages don't carry the requester's reply subject and pointing at `IMessageBus.RequestAsync` / header-based reply propagation. (Implementing real reply-over-JetStream via an `X-Reply-Subject` header convention is a feature decision — see Questions.)
- **Patch:** `001-fix-jetstream-respondasync-and-replyto.patch`
- **Regression tests:** `JsMessageContextReplySemanticsTests` (6 tests: ack subject → null ReplyTo, passthrough of genuine reply subjects, RespondAsync throws on both context types).
- **Release impact:** Blocks release — the API surface advertises request/reply on JetStream contexts (`IJsMessageContext<T>` inherits `RespondAsync`) and it cannot ever have worked.

## [Release Blocker] DLQ remediation resolves a never-registered object store → replay of offloaded payloads always fails

- **Status:** Confirmed
- **Files:** `FlySwattr.NATS.Core/Extensions/ServiceCollectionExtensions.cs:118-128` (registration passes `sp.GetService<IObjectStore>()`), `FlySwattr.NATS.Core/Services/NatsDlqRemediationService.cs:389-417` (`GetPayloadAsync`), `FlySwattr.NATS.Hosting/Services/DefaultDlqPoisonHandler.cs:193-201` (offload writer)
- **Risk:** Every DLQ entry whose payload exceeded 1 MB (`PayloadEncoding = "objstore://dlq-payload/…"`) fails `ReplayAsync` with "Unable to retrieve payload for replay" — in the golden path and all modular configurations.
- **Evidence:** Repo-wide grep confirms `IObjectStore` is registered **only** as keyed services (`AddPayloadOffloading`, lines 186-191; the golden-path cleanup service also resolves keyed). The unkeyed `GetService<IObjectStore>()` in the remediation registration is always `null`, while `DefaultDlqPoisonHandler` offloads through the keyed store, producing entries the remediation service can never hydrate.
- **Recommended fix:** Resolve the keyed store via `PayloadOffloadingOptions.ObjectStoreServiceKey` (set by `AddPayloadOffloading`), falling back to unkeyed.
- **Patch:** `003-resolve-dlq-remediation-object-store.patch`
- **Regression tests:** `DlqRemediationObjectStoreResolutionTests.GoldenPath_DlqRemediationService_ResolvesClaimCheckObjectStore` (builds the real golden-path container and asserts the resolved dependency).
- **Release impact:** Blocks release. Note: even with this patch, replayability of offloaded payloads is bounded by the claim-check bucket TTL (see H-4).

## [Release Blocker] `ResilientJetStreamConsumer` disposes the per-consumer semaphore while the consumer is running

- **Status:** Confirmed
- **Files:** `FlySwattr.NATS.Resilience/Decorators/ResilientJetStreamConsumer.cs:94-115` and `:143-164` (finally blocks), `ConsumerSemaphoreManager.cs:81-88` (`RemoveConsumerSemaphore` disposes)
- **Risk:** With `EnableResilience` + explicit `IJetStreamConsumer.Consume*Async` + `options.MaxConcurrency` set, every message after startup throws `ObjectDisposedException` from `semaphore.WaitAsync()` inside the wrapped handler → NAK loop until MaxDeliver. The consumer is effectively dead from message one.
- **Evidence:** `_inner.ConsumeAsync` returns once the background service **starts** (`NatsJetStreamBus.StartBackgroundConsumerAsync` → `BackgroundTaskManager.StartAsync` returns after `BackgroundService.StartAsync`). The `finally` then runs immediately and disposes the semaphore captured by `WrapHandlerWithSemaphore` (lines 169-188), which every subsequent message uses. An existing unit test (`ConsumeAsync_ShouldCleanupSemaphore_WhenConsumerCompletes`) asserted this disposal as desired behavior — a test pinning the bug.
- **Recommended fix:** Remove the premature cleanup; the semaphore manager already owns disposal at shutdown, and the pipeline cache evicts stale entries on its own timer. (Hosted consumers from `AddNatsConsumer`/`AddNatsTopologyWithConsumers` bypass this decorator and were unaffected.)
- **Patch:** `005-keep-consumer-semaphore-alive.patch` (includes the corrected legacy test)
- **Regression tests:** `ResilientConsumerSemaphoreLifetimeTests` (handler still functional after the consume call returns, both push and pull paths).
- **Release impact:** Blocks release — it breaks a documented combination of two flagship features (resilience + per-consumer concurrency limits).

## [Release Blocker] DLQ provisioning clobbers shared DLQ target streams' subjects

- **Status:** Confirmed (reproduced against live NATS, then verified fixed)
- **Files:** `FlySwattr.NATS.Topology/Managers/NatsTopologyManager.cs:286-313` (`EnsureDlqInfrastructureAsync`)
- **Risk:** The natural pattern — several consumers of one stream sharing one DLQ target stream with per-consumer subjects — silently breaks: whichever consumer provisions last replaces the stream's subject list with only its own subject. The displaced consumer's DLQ publishes then fail ("no stream matches subject"); `DefaultDlqPoisonHandler` NAKs (`DefaultDlqPoisonHandler.cs:152-158`) until the server stops redelivering, leaving only a payload-less advisory KV entry. Practical effect: DLQ payload loss. It also resets a user-customized DLQ stream's retention/MaxAge to the hardcoded defaults.
- **Evidence:** On "already exists" the code runs `UpdateStreamAsync(dlqStreamConfig)` where `dlqStreamConfig.Subjects = [policy.TargetSubject]` — wholesale config replacement. (Separate but related: the README's example topology — source stream `orders.>` with DLQ subject `orders.dlq` — cannot provision at all, because NATS rejects cross-stream subject overlap and the resulting 400 is not an "already exists" error; see Medium M-5.)
- **Recommended fix:** On exists, fetch the stream and **merge** the policy subject into the existing subject set (wildcard-aware no-op when already covered), leaving every other setting untouched.
- **Patch:** `006-merge-shared-dlq-stream-subjects.patch`
- **Regression tests:** `DlqSharedTargetStreamTests` — two integration tests run against live NATS: two policies sharing a target stream keep both subjects (and both subjects accept publishes); a user-customized wildcard DLQ stream is left untouched, including its MaxAge.
- **Release impact:** Blocks release — silent loss of DLQ'd payloads under a configuration the API actively encourages.

## [Release Blocker] Deleted KV keys throw `NatsKVKeyDeletedException` through public contracts

- **Status:** Confirmed (verified live: `GetEntryAsync` on a deleted key throws `NatsKVKeyDeletedException`, message "Key was deleted")
- **Files:** `FlySwattr.NATS.Core/Stores/NatsKeyValueStore.cs:103-133` (`GetAsync` catches only `NatsKVKeyNotFoundException`), `FlySwattr.NATS.Core/Stores/NatsDlqStore.cs:222-251` (`GetAsync`), `:321-377` (`UpdateStatusAsync`), `:380-409` (`DeleteAsync` via `GetAsync`)
- **Risk:** `IKeyValueStore.GetAsync` violates its documented "null if missing" contract for any key whose latest revision is a delete/purge tombstone — callers crash instead of treating the key as absent. In the DLQ store, inspecting or re-deleting a deleted entry throws instead of returning null/false; remediation flows over recently-deleted entries blow up.
- **Evidence:** `NatsKVKeyDeletedException` is a sibling of `NatsKVKeyNotFoundException` (both `: NatsKVException`), so the existing catch never matches. The caching layer (`CachingKeyValueStore.GetAsync`) passes the exception through and FusionCache fail-safe can then serve stale values for an actually-deleted key.
- **Recommended fix:** Catch `NatsKVKeyDeletedException` alongside `NatsKVKeyNotFoundException` in the three sites; deleted ⇒ default/null/NotFound.
- **Patch:** `002-handle-deleted-kv-keys.patch`
- **Regression tests:** `DeletedKvKeyHandlingTests` (4 tests across both stores).
- **Release impact:** Blocks release — a first-class read API throws on a routine state (every deleted key has a tombstone window; the DLQ bucket uses `History = 1` but the delete marker *is* the latest revision).

# High Priority Fixes

## [High] "Payload offloading is configured" fail-fast fires for hosts that never configured offloading

- **Status:** Confirmed
- **Files:** `FlySwattr.NATS.Hosting/Services/ConfiguredNatsConsumerHostedService.cs:45-63`, `FlySwattr.NATS.Hosting/Services/TopologyConsumerHostedService.cs:135,168-177`
- **Risk:** Modular composition (`AddFlySwattrNatsCore()` + `AddNatsConsumer(...)` without `AddPayloadOffloading()`) throws at startup demanding an object store the user never asked for. In the topology path the per-consumer catch swallows it — consumers **silently never start**.
- **Evidence:** The gate is `GetService<IOptions<PayloadOffloadingOptions>>() != null`, but the options infrastructure materializes `IOptions<T>` for any `T` in every real host (anything that calls `AddLogging`/`AddOptions`), so the check degenerates to "no object store resolvable" — and `AddFlySwattrNatsCore` registers no unkeyed `IObjectStore`.
- **Recommended fix:** Gate on `NatsPayloadOffloadingMarker` (registered exactly by `AddPayloadOffloading`); the marker type exists for precisely this purpose.
- **Patch:** `004-gate-offloading-failfast-on-marker.patch`
- **Regression tests:** `OffloadingConfiguredGateTests` (options resolve but gate stays closed; marker opens it).
- **Release impact:** Should be fixed before release — it makes the advertised modular registration model unusable for non-offloading hosts.

## [High] Default consumer retry pipeline ignores its own options and retries permanent failures

- **Status:** Confirmed
- **Files:** `FlySwattr.NATS.Hosting/Services/ConfiguredNatsConsumerHostedService.cs:136-149`, `FlySwattr.NATS.Hosting/Services/TopologyConsumerHostedService.cs:284-297` (duplicated `BuildDefaultResiliencePipeline`), `FlySwattr.NATS.Core/Configuration/ConsumerResilienceOptions.cs:16-34`
- **Risk:** This pipeline is effectively **always on** for hosted consumers (same `IOptions<T>`-always-resolves trap), and: (a) `InitialRetryDelay`/`MaxRetryDelay` are never wired, so Polly's 2 s default exponential base applies — ~14 s of hidden in-process delay per failing message at the default 3 attempts, eating into the 30 s `AckWait` and risking concurrent redelivery; (b) `Handle<Exception>()` retries `MessageValidationException`, the exception type whose entire documented purpose (`MessageValidationException.cs:5-10`) is "route to DLQ without retries" — the full middleware pipeline (validation included) re-runs three times before the poison handler sees it.
- **Recommended fix:** Wire `Delay`/`MaxDelay` from options; exclude `MessageValidationException` and `OperationCanceledException` from `ShouldHandle`.
- **Patch:** `007-honor-consumer-retry-options.patch`
- **Regression tests:** `ConsumerRetryPipelineTests` (5 tests, both hosted-service variants: attempt counts, validation not retried, configured delays actually used).
- **Release impact:** Fix before release; silent multi-second per-message stalls under failure are an operational trap. (Note: the circuit-breaker fields of `ConsumerResilienceOptions` are configured and documented but never used to build anything — flagged under Medium M-6.)

## [High] Over-broad transient classification retries permanent errors (missing objects, 400-class API errors)

- **Status:** Confirmed (exception hierarchy verified by reflection: `NatsObjNotFoundException → NatsObjException → NatsJSException → NatsException`; `NatsJSApiException → NatsJSException → NatsException`)
- **Files:** `FlySwattr.NATS.Core/Stores/NatsObjectStore.cs:98-116` (`IsTransient`), `FlySwattr.NATS.Resilience/Decorators/ResilientJetStreamPublisher.cs:110-116` (`IsTransient`)
- **Risk:** In the object store, `NatsObjNotFoundException` falls through to the `ex is NatsException` arm → a missing claim-check object costs 1+2+3 s of retries per hydration attempt, repeated on each redelivery. In the publisher, the boolean chain `… or NatsException || (ex is NatsJSApiException …)` matches 400-class JS API errors on the broad term before the 503/504 gate is relevant → permanently failing publishes burn 3 retries (and pollute the circuit breaker) before surfacing.
- **Evidence:** An existing unit test documented the wrong behavior as intended: `DeleteAsync_ShouldSucceed_WhenKeyNotFound` asserted **4 calls** with the comment "NatsObjectStore retries on NatsException, and NatsObjNotFoundException inherits from it."
- **Recommended fix:** Order-sensitive classification: JS API errors transient only on 503/504; connectivity-shaped JetStream exceptions (`NatsJSConnectionException`, `NatsJSTimeoutException`, `NatsJSApiNoResponseException`, `NatsJSPublishNoResponseException` — the last is the lost-PubAck case where retrying with the same message ID is exactly right) transient; all remaining `NatsJSException` domain errors permanent; then network exceptions and core `NatsException` as before.
- **Patch:** `008-narrow-transient-error-classification.patch` (includes the corrected legacy test)
- **Regression tests:** `TransientErrorClassificationTests` (missing object not retried; 400 publish not retried; 503 publish retried with same message ID).
- **Release impact:** Fix before release — latency amplification on the poison path and retry-storms on permanent errors.

## [High] Raw-path deserialization failures tear down the consume subscription instead of poisoning the message — *design note, not patched*

- **Status:** Confirmed
- **Files:** `FlySwattr.NATS.Hosting/Services/NatsConsumerBackgroundService.cs:310-335` (`ReadContextsAsync`/`ResolveRawContextAsync`), `:228-266` (outer loop catch)
- **Risk:** In the golden-path raw consumption mode, claim-check hydration and payload deserialization run inside the consume enumerator. A poison payload (schema/fingerprint mismatch, missing claim-check object, oversize) throws into the outer catch, which logs, sleeps 1 s, and **recreates the entire subscription**. The message is never NAK'd/Term'd/DLQ'd by the library — it redelivers after `AckWait` and kills the enumerator again, disrupting all message flow on that consumer until the server's MaxDeliver advisory finally fires (which persists only a payload-less KV entry).
- **Recommended fix (manual):** Catch per message inside `ReadContextsAsync`; on hydration/deserialization failure, `Term` the raw message and write a DLQ entry with the raw bytes (they are in hand in this path), routed through `IPoisonMessageHandler` or a raw-payload equivalent. Not patched blindly: it requires choosing the DLQ payload representation for messages that failed deserialization and which typed context (if any) the poison handler receives.
- **Patch:** manual fix recommended (design note in `FABLE_CODE_REVIEW_FIXES/README.md`)
- **Regression tests (to add with the fix):** `RawConsumer_PoisonPayload_IsTerminatedAndDlqd_WithoutLoopRestart`; `RawConsumer_MissingClaimCheckObject_DoesNotRestartSubscription`.
- **Release impact:** Should be consciously accepted or fixed before release; with patches 003/008 applied the blast radius shrinks (no 6 s retry amplification), but subscription churn per poison delivery remains.

## [High] DLQ payload offloads share the claim-check bucket and inherit its 24 h TTL — *design note, not patched*

- **Status:** Confirmed
- **Files:** `FlySwattr.NATS.Hosting/Services/DefaultDlqPoisonHandler.cs:193-201` (writes `dlq-payload/...` into the claim-check store), `FlySwattr.NATS.Topology/Services/TopologyProvisioningService.cs:214-235` (bucket auto-created with `MaxAge = ClaimCheckTtl`), `FlySwattr.NATS/Configuration/EnterpriseNatsOptions.cs:161-179` (`ClaimCheckTtl` default 24 h), `FlySwattr.NATS.Hosting/Services/ClaimCheckCleanupService.cs:62-88` (optional sweep deletes **any** object older than TTL, prefix-blind)
- **Risk:** DLQ entries with >1 MB payloads are replayable for at most 24 h; remediation typically happens later. The DLQ stream itself retains messages 30 days — the payload store contradicts it.
- **Recommended fix (manual):** Provision a dedicated `fs-dlq-payloads` object store bucket with DLQ-aligned retention and point `DefaultDlqPoisonHandler`/remediation at it; independently, scope `ClaimCheckCleanupService.SweepAsync` to `PayloadOffloadingOptions.ObjectKeyPrefix`.
- **Patch:** manual fix recommended
- **Release impact:** Decide before release (it's a data-retention promise); acceptable short-term if documented.

## [High] `AddNatsConsumer` hosted service fetches the consumer before the topology-ready signal — *design note, not patched*

- **Status:** Confirmed
- **Files:** `FlySwattr.NATS.Hosting/Services/ConfiguredNatsConsumerHostedService.cs:39-42` (`GetConsumerAsync` in `StartAsync`), vs. `NatsConsumerBackgroundService.cs:190-209` (signal wait happens later, in the worker); contrast `TopologyConsumerHostedService.cs:47-56` (waits first — correct)
- **Risk:** Works only when `TopologyProvisioningService` happens to be registered first and hosted services start sequentially. Registration-order changes or `ServicesStartConcurrently` produce "consumer not found" at startup. Related: when provisioning partially fails, `SignalFailed` causes consumers to log and silently never start while **unregistering from health metrics** (`NatsConsumerBackgroundService.cs:204-208,292-294`) — the host runs degraded with a green `nats_consumer_health`.
- **Recommended fix (manual):** Move `GetConsumerAsync` behind an `ITopologyReadySignal` wait in `ConfiguredNatsConsumerHostedService.StartAsync`; surface failed-topology consumers as unhealthy in `NatsConsumerHealthCheck` instead of unregistering.
- **Patch:** manual fix recommended
- **Release impact:** Should be fixed before release or documented as a hard registration-order requirement.

# Medium Priority Fixes

## [Medium] Non-seekable stream Put retry silently uploads truncated objects
- **Status:** Confirmed · **Files:** `FlySwattr.NATS.Core/Stores/NatsObjectStore.cs:118-153`
- **Risk:** A transient failure mid-upload on a non-seekable stream leaves the stream partially consumed; the retry uploads only the remaining bytes and **reports success** with corrupted content. Internal callers use `MemoryStream` (safe); `IObjectStore` is public API.
- **Fix/Patch:** Non-seekable uploads get exactly one attempt — `009-no-retry-nonseekable-put.patch` · **Tests:** `NonSeekablePutRetryTests` (2) · **Release impact:** before release (silent corruption path on a public API).

## [Medium] DLQ replay republishes redacted header markers as live header values
- **Status:** Confirmed · **Files:** `FlySwattr.NATS.Core/Configuration/DlqHeaderRedactionOptions.cs:9-21` (redaction **on** by default, empty allowlist → all values stored as `[REDACTED]` by `DefaultDlqPoisonHandler.FilterHeaders:288-307`), `FlySwattr.NATS.Core/Services/NatsDlqRemediationService.cs:527-542` (`CreateReplayHeaders` forwards them), `FlySwattr.NATS/Configuration/EnterpriseNatsOptions.cs:189-194` (XML doc claims "preserved verbatim" by default — false)
- **Risk:** Replayed messages carry e.g. `X-Tenant: [REDACTED]` as a real header; consumers keying on business headers misbehave after a replay.
- **Fix/Patch:** Drop header values equal to the configured `RedactedValue` on replay; correct the misleading XML doc — `010-skip-redacted-headers-on-replay.patch` · **Tests:** `DlqReplayRedactedHeaderTests` (2) · **Release impact:** before release (correctness of the remediation feature); the default-redact-everything *policy itself* deserves a product decision (allowlist correlation IDs by default?).

## [Medium] `JetStreamConsumeOptions.MaxConcurrency` is a no-op on the explicit consume path
- **Status:** Confirmed · **Files:** `FlySwattr.NATS.Core/NatsJetStreamBus.cs:558-665` (`BasicNatsConsumerService` awaits the handler inline; the semaphore can never contend)
- **Risk:** Code does not do what the option promises; users tuning `MaxConcurrency` on `IJetStreamConsumer.ConsumeAsync` get strictly sequential processing regardless. (The Hosting consumer implements real parallelism correctly.)
- **Recommended fix:** Either dispatch to a worker pool like `NatsConsumerBackgroundService`, or document the explicit API as sequential and obsolete the option there. Not patched: a concurrency rewrite of the basic loop should not ship as a drive-by. **Patch:** manual fix recommended · **Release impact:** before release as a doc fix; code fix can follow.

## [Medium] `IMessageBus.RequestAsync` contract mismatch (doc says null on timeout; code throws)
- **Status:** Confirmed · **Files:** `FlySwattr.NATS.Abstractions/Interfaces/IMessaging.cs:43-51` (XML: "or default if no response"), `FlySwattr.NATS.Core/NatsMessageBus.cs:326-346` (NATS.Net throws `NatsNoReplyException` on timeout / `NatsNoRespondersException` with no responders)
- **Risk:** Callers writing `if (response is null)` per the docs get unhandled exceptions on the exact path they handled.
- **Recommended fix:** Fix the XML doc to document the thrown exceptions (preferred — throwing is the better behavior); one-line doc change, bundled naturally with the next API-doc pass. **Patch:** manual fix recommended · **Release impact:** before release (docs only).

## [Medium] README example topology cannot provision (DLQ subject overlaps source stream wildcard)
- **Status:** Confirmed (reproduced live: `CreateStreamAsync` → 400 "subjects overlap with an existing stream", which `IsResourceExistsError` does not match → consumer provisioning fails)
- **Files:** `README.MD:96-126` (stream `orders.>` + `TargetSubject = "orders.dlq"`), `FlySwattr.NATS.Topology/Managers/NatsTopologyManager.cs:71-75,286-313`
- **Risk:** First-run experience: copying the README's quick-start topology yields a failed DLQ provisioning on a fresh server.
- **Recommended fix:** Change the README example to a non-overlapping DLQ subject (e.g. source `orders.created.>` or DLQ subject `dlq.orders`); optionally have `EnsureDlqInfrastructureAsync` surface overlap errors with a targeted message. **Patch:** manual fix recommended (docs) · **Release impact:** before release (docs).

## [Medium] `ConsumerResilienceOptions` circuit-breaker settings are configured but never used
- **Status:** Confirmed · **Files:** `FlySwattr.NATS.Core/Configuration/ConsumerResilienceOptions.cs:36-54`, golden-path copy-through at `FlySwattr.NATS/Extensions/ServiceCollectionExtensions.cs:99-108`; no consumer of the three CB properties exists (repo-wide grep)
- **Risk:** Operators configure breaker thresholds that silently do nothing. Same family: `NatsConfiguration.MaxConcurrency` (`NatsConfiguration.cs:14`) is configured, validated, README-documented — and never read.
- **Recommended fix:** Wire a circuit breaker into the default consumer pipeline or delete the dead options (and `MaxConcurrency`) before the public API ossifies. **Patch:** manual fix recommended · **Release impact:** before release if the options ship in a 1.0 public surface.

## [Medium] Integration tests are flaky by construction (per-test containers)
- **Status:** Confirmed (observed: 3 failures run 1, 1 failure run 2 — `DockerApiException: rootlessport listen tcp 0.0.0.0:42671: bind: address already in use` — 0 failures run 3; CI masks with `--retry-failed-tests 3` in `.github/workflows/dotnet.yml:31-34`)
- **Files:** `Tests/IntegrationTests/**` (each test news a `NatsContainerFixture`)
- **Risk:** Red-herring CI failures; retries can also mask genuine intermittent regressions in reconnect-sensitive code.
- **Recommended fix:** Shared assembly-level NATS container fixture (TUnit `ClassDataSource`/assembly hooks) with per-test stream/bucket namespacing; drop the CI retry flag afterwards. **Patch:** manual fix recommended · **Release impact:** can be scheduled after release.

# False Positives / Rejected Prior Findings

Re-verified and **not** issues (stated briefly so they aren't re-litigated):

1. **Publish idempotency under retry** — `ResilientJetStreamPublisher` reuses the caller's message ID across retries; correct, and now pinned by `TransientErrorClassificationTests.Publisher_ServiceUnavailable_IsRetried`.
2. **Subject/header injection** — `MessageSecurity` validation (wildcard rejection in publish subjects, control-char and `Nats-*` header rejection, claim-check prefix pinning, traversal-safe object keys) is solid; the offloading consumer and cleanup middleware both enforce the configured prefix. No injection path found.
3. **`InsecureSkipVerify` hardening** — Core forcibly re-disables TLS skip-verify (`Core/Extensions/ServiceCollectionExtensions.cs:60`); not a finding, a strength (plaintext-by-default URL scheme remains a config-policy note, not a defect).
4. **Double-ack hazards** — handler-acked messages are protected by the `_acknowledged` guard in `JsMessageContextWrapper`/`HydratedMessageContext`; the worker's post-pipeline ack is a safe no-op, and `WireVersionCheckMiddleware`'s Term/Nack actions compose correctly with it.
5. **Claim-check compensation on publish failure** — `OffloadingJetStreamPublisher` deletes orphaned objects only when the corresponding publish was never confirmed (`pendingCleanup` bookkeeping in the batch path is correct); no live-payload deletion path found.
6. **DLQ store optimistic concurrency** — revision-checked status transitions with Conflict/NotFound outcomes and revert-to-Pending compensation in `ReplayAsync` are correctly implemented.
7. **MemoryPack envelope inbound size caps** — both the wire-size cap and the inner-payload cap are enforced (`HybridNatsSerializer.Deserialize`, `MemoryPackSchemaEnvelopeSerializer.Deserialize(maxPayloadSize)`); no unbounded-allocation path through the typed consume pipeline.
8. **Prior-round fixes hold** — earlier remediation themes (payload size enforcement, DLQ header redaction storage, NAK retry resilience, claim-check prefix requirement — commits `6f0243e`, `a047642`, `74ef474`) are present and covered by existing tests; no regressions found in those areas.

# NATS / JetStream Review

Condensed to what matters for release (full architectural review in `NATS_WRITEUP.MD`):

- **Ack/retry lifecycle (hosted path):** correct and careful — bounded channel sized to parallelism, ack-with-timeout (abandoned intentionally on shutdown), NAK-drain of queued messages at shutdown, in-progress heartbeats with linked cancellation, exponential NAK backoff capped at 30 s in the poison handler, DLQ publish + KV persist + Term on exhaustion with deterministic DLQ message IDs (`dlq-{stream}-{consumer}-{seq}`). The two retry personalities (flat 5 s NAK in `BasicNatsConsumerService` vs. exponential in the poison handler) should converge eventually.
- **Idempotency:** required message IDs end-to-end (publish, batch, raw, replay) is the standout design decision; replay modes (Idempotent/NewEvent) are explicit. Caveat to document: JetStream's dedup window (typically 2 min) bounds replay de-duplication; provisioning does not set `DuplicateWindow`.
- **Redelivery/poison:** handler exceptions route correctly (validation ⇒ DLQ immediately; transient ⇒ NAK/backoff; exhausted ⇒ DLQ+Term). The gap is pre-handler failures in the raw path (H-3). With `ConsumerSpec.MaxDeliver` defaulting to `-1`, poison messages NAK forever; the poison handler maps it to `long.MaxValue` (`NatsConsumerBackgroundService.cs:472-474`) — recommend a finite default or a provisioning warning when a DLQ policy coexists with unlimited deliveries.
- **Request/reply:** core NATS path is correct with W3C trace propagation; JetStream-context reply is Blocker 1.
- **Reconnect/outage:** subscription and watch loops resubscribe with (jittered) backoff; topology provisioning has cold-start ping retries under a global timeout; advisory listener reconnects on error. Advisory subscription is core NATS — advisories during listener downtime are lost (acceptable; the inline poison handler is the primary writer, and the code logs the limitation).

# Security Review

- **Credentials/TLS:** `NatsAuthOpts`/`NatsTlsOpts` pass-through, never logged; `InsecureSkipVerify` force-disabled; TLS cert/key/CA file existence validated at startup. `AllowInsecureTransport` defaults `true` (dev-friendly); the enforcement path exists — production guidance should flip it (`NatsConfiguration.cs:48-55`).
- **Injection surfaces:** publish subjects reject wildcards and invalid tokens (Vogen `SubjectName` + `MessageSecurity.ValidatePublishSubject`); user headers can't override reserved transport headers or any `Nats-*` header; claim-check references are prefix-pinned at hydration and cleanup. One inconsistency: `NatsDlqRemediationService.GetPayloadAsync` trusts `entry.PayloadEncoding`/`x-dlq-payload-ref` without `ValidateClaimCheckReference` (`NatsDlqRemediationService.cs:393-411`) — operator-zone data (KV write access required), but it should be validated against the DLQ payload prefix for defense-in-depth. Schedule with H-4's dedicated-bucket work.
- **Deserialization:** no polymorphic type resolution from attacker-controlled data — `X-ClaimCheck-Type` resolves through an explicit alias registry with `RequireExplicitAliases = true` by default; MemoryPack envelopes are schema-checked (id, version window, fingerprint) before payload deserialization.
- **Information disclosure:** exception messages sanitized (256-char cap, control chars stripped, FluentValidation reduced to field names) before DLQ entries/telemetry; DLQ header redaction is on by default (its replay echo is Medium-2). Logging avoids payload contents throughout.
- **DoS:** outbound and inbound payload caps, inner-envelope cap, hydration size/budget caps with pooled buffers, bounded channels, bounded batch concurrency, bounded DLQ list parallelism. Residual: unlimited `MaxDeliver` default (above) and the H-3 subscription-churn amplification.

# Reliability Review

- **Cancellation:** consistently propagated (consume loops, workers, heartbeats, drain, provisioning, locks); `OperationCanceledException` handled distinctly from failures nearly everywhere; Polly pipelines exclude cancellation after patch 007.
- **Shutdown:** consumer drains and NAKs queued messages, workers get a 30 s bounded wait, `NatsMessageBus` and `BackgroundTaskManager` dispose orderly. One residual: `BackgroundTaskManager.DisposeAsync` awaits all tasks unbounded (`BackgroundTaskManager.cs:61-69`) — a hung handler can stall disposal; add a timeout in the next pass.
- **Health/readiness:** connection + JetStream account checks; zombie-consumer detection driven by loop-iteration heartbeats; opt-in hard startup check. Gap: topology-failure → consumers absent but health green (H-5).
- **Failure behavior verified:** NATS-down at startup (cold-start retry with global timeout); bucket/stream missing (DLQ store failure policies: Propagate/LogAndContinue/FallbackToLogOnly are coherent and tested); reconnects (watch loops invalidate cached handles on 503/504); slow consumers (bounded channel + `MaxAckPending` passthrough).

# Test Plan

Highest-value additions, in order (1–8 ship inside the patches; 9–12 to add):

1. ✔ `JsMessageContextReplySemanticsTests` — JS ReplyTo/RespondAsync contract (patch 001).
2. ✔ `DeletedKvKeyHandlingTests` — deleted-key semantics across KV + DLQ stores (patch 002).
3. ✔ `DlqRemediationObjectStoreResolutionTests` — golden-path DI resolves the claim-check store for remediation (patch 003).
4. ✔ `OffloadingConfiguredGateTests` — marker-based feature gating (patch 004).
5. ✔ `ResilientConsumerSemaphoreLifetimeTests` (+ corrected legacy test) — handler usable after consume start (patch 005).
6. ✔ `DlqSharedTargetStreamTests` — live-NATS subject merge for shared/customized DLQ streams (patch 006).
7. ✔ `ConsumerRetryPipelineTests` — retry counts, validation exclusion, configured delays (patch 007).
8. ✔ `TransientErrorClassificationTests` / `NonSeekablePutRetryTests` / `DlqReplayRedactedHeaderTests` (patches 008–010).
9. `DlqRemediation_ReplayOffloadedPayload_EndToEnd` — integration: >1 MB poison message through `DefaultDlqPoisonHandler`, then `ReplayAsync`, consumer receives original payload (locks Blocker 3 + H-4 end-to-end).
10. `RawConsumer_PoisonPayload_IsTerminatedAndDlqd_WithoutLoopRestart` — with the H-3 fix.
11. `AddNatsConsumer_RegisteredBeforeTopology_StartsAfterReadySignal` — with the H-5 fix.
12. Test-infra: single shared NATS container per assembly; then remove `--retry-failed-tests 3` from CI so flakiness is visible again.

# Patch Index

| # | Patch file | Severity | Finding | Changes | Tests | Apply risk |
|---|-----------|----------|---------|---------|-------|------------|
| 001 | `001-fix-jetstream-respondasync-and-replyto.patch` | Blocker | JS `RespondAsync` dead cast; `ReplyTo` = ack subject | 2 context classes (Core + Hosting) | ✔ 6 | Low (honest-contract behavioral change) |
| 002 | `002-handle-deleted-kv-keys.patch` | Blocker | `NatsKVKeyDeletedException` escapes public contracts | KV store + DLQ store catches | ✔ 4 | Very low |
| 003 | `003-resolve-dlq-remediation-object-store.patch` | Blocker | Remediation can't resolve object store | Core DI registration | ✔ 1 | Very low |
| 004 | `004-gate-offloading-failfast-on-marker.patch` | High | Offloading fail-fast misfires | 2 hosted services + helper | ✔ 2 | Low |
| 005 | `005-keep-consumer-semaphore-alive.patch` | Blocker | Semaphore disposed at consumer start | Resilience decorator (+1 corrected test) | ✔ 3 | Low |
| 006 | `006-merge-shared-dlq-stream-subjects.patch` | Blocker | DLQ stream subject clobbering | Topology manager merge logic | ✔ 2 (live NATS) | Low |
| 007 | `007-honor-consumer-retry-options.patch` | High | Retry options ignored; validation retried | 2 hosted services' pipeline builders | ✔ 5 | Low |
| 008 | `008-narrow-transient-error-classification.patch` | High | Permanent errors retried | ObjectStore + publisher classification (+1 corrected test) | ✔ 4 | Low-medium |
| 009 | `009-no-retry-nonseekable-put.patch` | Medium | Truncated-upload corruption on retry | ObjectStore PutAsync | ✔ 2 | Very low |
| 010 | `010-skip-redacted-headers-on-replay.patch` | Medium | `[REDACTED]` republished as live headers | Remediation service + options doc | ✔ 2 | Very low |

# Recommended Fix Order

1. **Release blockers:** patches 001, 002, 003, 005, 006 (independent; numeric order fine).
2. **High-priority correctness/security:** patches 004, 007, 008; then the H-3 raw-path poison handling as a designed change.
3. **Tests that lock behavior:** already bundled per patch; add the end-to-end offloaded-replay integration test (Test Plan #9) once 003 + the H-4 bucket decision land.
4. **Operational hardening:** H-4 dedicated DLQ payload bucket (+ prefix-scoped sweep, + payload-ref validation), H-5 startup ordering + health visibility, `BackgroundTaskManager` disposal timeout, finite `MaxDeliver` default.
5. **Medium cleanup:** patches 009, 010 (can ship with wave 1 — they're tiny), README topology example, `RequestAsync` doc, dead options removal (`ConsumerResilienceOptions` CB fields, `NatsConfiguration.MaxConcurrency`), shared test container.

# Commands Run

| Command | Result |
|---|---|
| `dotnet build FlySwattr.NATS.slnx --configuration Release` (baseline) | Success, **0 warnings / 0 errors** (`TreatWarningsAsErrors` on) |
| `dotnet test --project Tests/UnitTests … Release` (baseline) | **511/511 passed** |
| `dotnet test --project Tests/IntegrationTests … Release` (baseline, 3 runs) | 92/95 → 95/95 → 94/95; all failures environmental (Testcontainers/podman port collisions, e.g. `rootlessport … address already in use`), none reproducible — see Medium "flaky tests" |
| Live verification (`nats:2.10` container): KV deleted-key `GetEntryAsync`; JS `ReplyTo`/`INatsMsg<T>` cast | Confirmed `NatsKVKeyDeletedException` and `$JS.ACK.…` ReplyTo / cast-always-false |
| Reflection over NATS.Net 2.7.0 | Confirmed exception hierarchy and `INatsJSMsg<T>` interface set |
| `git apply --check` each of the 10 patches against clean tree | All apply cleanly |
| All 10 patches applied → `dotnet build` + full test suites | Build 0 warnings; **538/538 unit**, **97/97 integration** passed |

# Questions / Unknowns

1. **Is request/reply over JetStream an intended feature?** Patch 001 makes the current impossibility explicit; if the feature is wanted, it needs a header-based reply-subject convention (publisher helper + `RespondAsync` honoring `X-Reply-Subject`) — a small design, but an API commitment.
2. **What is the intended DLQ replayability horizon?** H-4's fix (dedicated payload bucket) needs a retention number; the DLQ stream says 30 days, the payload store currently says 24 hours.
3. **Should DLQ header redaction default to redact-everything?** Patch 010 stops the replay corruption either way, but the default makes stored DLQ entries nearly headerless for diagnostics; a default allowlist (`X-Correlation-`, `traceparent`) may serve operators better.
4. **Downstream consumers (e.g. the FrostStream services referenced in code comments):** if any existing consumer catches `NotSupportedException` from `RespondAsync` or reads the ack subject from `ReplyTo`, coordinate the patch-001 behavioral change with their upgrade.

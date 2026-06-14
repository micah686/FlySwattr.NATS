# FlySwattr.NATS — Release-Readiness Fix Patches

Companion to `FABLE_CODE_REVIEW.MD`. Each patch is an independent, focused fix in standard
unified-diff format. Apply from the repository root with:

```bash
git apply FABLE_CODE_REVIEW_FIXES/<patch-file>
```

All ten patches were verified together against this tree:
- each applies cleanly to a clean checkout (`git apply --check`),
- with **all ten applied**: full solution builds with 0 warnings (`TreatWarningsAsErrors` on),
- **538/538 unit tests pass** (511 pre-existing + 27 added by these patches),
- **97/97 integration tests pass** against a real NATS 2.10 container (95 pre-existing + 2 added).

## Recommended application order

Apply in numeric order. All patches are hunk-independent (001–010 touch disjoint regions even
where they share files: 004/007 modify different methods of the two consumer hosted services;
008/009 modify different methods of `NatsObjectStore`), so they also apply individually in any
order — numeric order is simply severity-first.

| # | File | Severity | Finding | What it changes | Tests | Risk of applying |
|---|------|----------|---------|-----------------|-------|------------------|
| 001 | `001-fix-jetstream-respondasync-and-replyto.patch` | Release Blocker | `RespondAsync` on JetStream contexts always throws via a dead `INatsMsg<T>` cast; `ReplyTo` exposes the `$JS.ACK.` ack subject | Both JS message contexts: `ReplyTo` returns `null` for ack subjects; `RespondAsync` throws a deterministic, documented `NotSupportedException` (replying to the ack subject would be interpreted as an ack-protocol message) | ✔ 6 unit tests | Low. **Behavioral change:** `ReplyTo` no longer leaks the ack subject (it was never a usable reply target); `RespondAsync` already always threw, now with an actionable message. |
| 002 | `002-handle-deleted-kv-keys.patch` | Release Blocker | `NatsKVKeyDeletedException` (thrown for delete/purge tombstones; verified live) escapes `IKeyValueStore.GetAsync` and the DLQ store | Catches the exception: KV `GetAsync` → default, DLQ `GetAsync` → null, DLQ `UpdateStatusAsync` → `NotFound`; `DeleteAsync` of a deleted entry now returns `false` | ✔ 4 unit tests | Very low — converts an exception into the documented contract. |
| 003 | `003-resolve-dlq-remediation-object-store.patch` | Release Blocker | `IDlqRemediationService` resolves an **unkeyed** `IObjectStore` that is never registered → replay of offloaded (>1 MB) DLQ payloads always fails | Core DI: remediation resolves the keyed claim-check store via `PayloadOffloadingOptions.ObjectStoreServiceKey`, falling back to unkeyed | ✔ 1 DI test (asserts the resolved store is non-null in the golden path) | Very low — only changes a `null` dependency to the store the poison handler already writes to. |
| 004 | `004-gate-offloading-failfast-on-marker.patch` | High | "Offloading is configured" check uses `IOptions<PayloadOffloadingOptions>` presence, which is **always** true in a real host → modular hosts without `AddPayloadOffloading` fail at startup (or consumers silently don't start) | Both consumer hosted services gate the fail-fast on `NatsPayloadOffloadingMarker`; adds `IsPayloadOffloadingConfigured` helper | ✔ 2 unit tests | Low. Hosts that never called `AddPayloadOffloading` no longer require an `IObjectStore`. |
| 005 | `005-keep-consumer-semaphore-alive.patch` | Release Blocker | `ResilientJetStreamConsumer` disposes the per-consumer semaphore in `finally` right after the consume call returns — but the call returns at consumer **start**, so every later message throws `ObjectDisposedException` | Removes the premature cleanup (manager owns disposal at shutdown; pipeline cache self-evicts); updates the one existing test that pinned the buggy cleanup | ✔ 2 new unit tests + 1 corrected test | Low. Semaphores now live for process lifetime (bounded by distinct consumer keys — same lifetime as every other singleton here). |
| 006 | `006-merge-shared-dlq-stream-subjects.patch` | Release Blocker | `EnsureDlqInfrastructureAsync` replaces an existing DLQ stream's config with a single-subject config — the second consumer sharing a DLQ target stream clobbers the first one's subject (DLQ publishes then fail → payload effectively lost) | On "stream exists", merge this policy's subject into the existing subject set (wildcard-aware) and leave all other settings untouched | ✔ 2 integration tests (verified against live NATS) | Low. Existing DLQ streams are no longer forcibly reset to `MaxAge=30d`/single subject; pure superset behavior. |
| 007 | `007-honor-consumer-retry-options.patch` | High | Default in-process retry pipeline ignores `InitialRetryDelay`/`MaxRetryDelay` (Polly's 2 s default base → ~14 s hidden delay) and retries `MessageValidationException`, which is designed to go straight to the DLQ | Both `BuildDefaultResiliencePipeline` copies: wire `Delay`/`MaxDelay` from options; exclude `MessageValidationException` and `OperationCanceledException` from retry | ✔ 5 unit tests | Low. Consumers with default options now retry with the *documented* 1 s base instead of Polly's 2 s; validation failures reach the poison handler immediately (the designed behavior). |
| 008 | `008-narrow-transient-error-classification.patch` | High | `IsTransient` falls through to `ex is NatsException`, which matches the whole NATS exception tree — missing objects and 400-class JS API errors get 3 retries with backoff (~6 s per missing claim-check hydration) | Reworks classification in `NatsObjectStore` and `ResilientJetStreamPublisher`: JS API errors only on 503/504; connectivity-shaped JS exceptions (publish/API no-response, timeout, connection) retryable; remaining JS domain errors permanent; corrects one existing test that pinned the 4-attempt NotFound behavior | ✔ 3 new unit tests + 1 corrected test | Low-medium. Errors previously mis-retried now surface immediately; genuinely transient classes are enumerated explicitly. |
| 009 | `009-no-retry-nonseekable-put.patch` | Medium | `PutAsync` retry with a non-seekable stream re-uploads only the remaining bytes → silently truncated object reported as success | Non-seekable uploads get exactly one attempt; seekable retry behavior (with rewind) unchanged | ✔ 2 unit tests | Very low — removes a silent-corruption path; failures now propagate instead of corrupting. |
| 010 | `010-skip-redacted-headers-on-replay.patch` | Medium | DLQ header redaction is ON by default with an empty allowlist; replay republishes the literal `[REDACTED]` marker as live header values (and the `EnterpriseNatsOptions` XML doc claims headers are "preserved verbatim" by default) | `CreateReplayHeaders` drops values equal to the configured `RedactedValue` (new optional `IOptions<DlqHeaderRedactionOptions>` ctor param, defaulting safely); corrects the misleading XML doc | ✔ 2 unit tests | Very low. Note: if you customize `RedactedValue`, also pass the options to the remediation service in your wiring (the default DI registration uses the default marker). |

## Not patched (design notes — see FABLE_CODE_REVIEW.MD)

These are real findings where a blind patch would be riskier than a considered change:

- **Raw-path poison deserialization tears down the consume enumerator** (`NatsConsumerBackgroundService.ReadContextsAsync`): the right fix moves hydration/deserialization failures into a per-message Term + DLQ path, which needs a deliberate decision about what payload to persist (raw bytes are available) and which context to hand the poison handler. See finding H-3.
- **DLQ payload offloads share the claim-check bucket and its 24 h TTL**: needs a dedicated `fs-dlq-payloads` bucket with DLQ-aligned retention — a provisioning/product decision, not a one-liner. See finding H-4.
- **`ConfiguredNatsConsumerHostedService` fetches the consumer before the topology-ready signal**: correct fix is to move `GetConsumerAsync` behind the ready-signal wait (mirroring `TopologyConsumerHostedService`), which changes startup sequencing for hosts that relied on eager failure. See finding H-5.

# Wire Compatibility & Mixed-Client Ecosystems

This document describes FlySwattr.NATS's wire-format versioning, cross-language header contract,
and upgrade migration strategies.

---

## 1. Protocol Version Header

Every message published by FlySwattr.NATS includes a **protocol version header**:

| Header | Type | Example | Description |
|--------|------|---------|-------------|
| `X-FlySwattr-Version` | integer string | `"1"` | Protocol version of the publishing library |

### Configuration

```csharp
services.Configure<WireCompatibilityOptions>(opts =>
{
    opts.ProtocolVersion = 1;           // stamped on outgoing messages
    opts.MinAcceptedVersion = 1;        // reject messages below v1
    opts.MaxAcceptedVersion = 2;        // warn on messages above v2
    opts.MismatchAction = VersionMismatchAction.LogWarning; // or Terminate / Nack
});
```

### Version Numbering

| Version | Wire Format Changes |
|---------|-------------------|
| 1 | Initial release. Type aliases use `type.Name` (short name). MemoryPack serialization with schema envelope. |

Increment the protocol version when introducing breaking wire-format changes such as:
- New required headers that older consumers do not understand.
- Changes to claim-check reference format.
- Serialization format changes.

---

## 2. Custom Header Reference (Cross-Language)

All headers set by FlySwattr.NATS are listed below for non-.NET consumers that need to
interpret or produce compatible messages.

### Standard Headers

| Header | Set By | Type | Required | Description |
|--------|--------|------|----------|-------------|
| `Content-Type` | Publisher | string | Yes | Serialization format. Values: `application/x-memorypack`, `application/x-memorypack-envelope` |
| `Nats-Msg-Id` | Publisher | string | Yes | Application-level de-duplication ID (JetStream) |
| `traceparent` | Publisher | string | No | W3C Trace Context propagation |
| `tracestate` | Publisher | string | No | W3C Trace Context vendor state |
| `X-FlySwattr-Version` | Publisher | integer string | Yes (v1+) | Protocol version number |

### Claim-Check Headers (Payload Offloading)

Present when a message payload exceeded the configured threshold and was offloaded to NATS Object Store.

| Header | Set By | Type | Required | Description |
|--------|--------|------|----------|-------------|
| `X-ClaimCheck-Ref` | OffloadingPublisher | string | Conditional | Object store reference. Format: `objstore://{key}` |
| `X-ClaimCheck-Type` | OffloadingPublisher | string | Conditional | Type alias of the original message for deserialization |

**Non-.NET consumers:** When `X-ClaimCheck-Ref` is present, the NATS message body is **empty**.
Download the actual payload from the NATS Object Store bucket (default: `claim-checks`)
using the key from the header value (strip the `objstore://` prefix).

### MemoryPack Schema Headers

Present when using `MemoryPackSchemaEnvelopeSerializer` (default).

| Header | Set By | Type | Required | Description |
|--------|--------|------|----------|-------------|
| `X-MP-Schema-Fingerprint` | Publisher | string | No | SHA-256 fingerprint of the MemoryPack schema for type validation |
| `X-MP-Schema-Version` | Publisher | string | No | Schema version for evolution tracking |

**Non-.NET consumers:** These headers are informational for .NET-to-.NET schema validation.
If you use a different serializer for your language, you can safely ignore them.

### DLQ Headers

| Header | Set By | Type | Description |
|--------|--------|------|-------------|
| `x-dlq-payload-ref` | DLQ Handler | string | Object store reference for large DLQ payloads |

---

## 3. Migration: Pre-Alias to Alias Deployments

### Background

Older versions of FlySwattr.NATS used `AssemblyQualifiedName` (e.g., `MyApp.OrderCreated, MyApp, Version=1.0.0.0, ...`)
as the type identifier in claim-check and DLQ headers. The current version uses **type aliases**
(default: `type.Name`, e.g., `OrderCreated`).

### Migration Steps

#### Step 1: Drain In-Flight Messages

Before upgrading, ensure all in-flight claim-check messages from the old format have been consumed:

```bash
# Check for pending messages in claim-check streams
nats consumer info <stream> <consumer> --server <nats-url>
```

Wait until `Num Pending` reaches 0 for all consumers that process claim-check messages.

#### Step 2: Register Explicit Aliases

For maximum safety, register explicit aliases for all message types before upgrading:

```csharp
services.AddMessageTypeAlias<OrderCreated>("OrderCreated");
services.AddMessageTypeAlias<PaymentProcessed>("PaymentProcessed");

// In production, enable strict mode:
services.Configure<MessageTypeAliasOptions>(opts =>
{
    opts.RequireExplicitAliases = true;
});
```

#### Step 3: Upgrade Consumers First

Deploy the new version to **consumers first**. The `MessageTypeAliasRegistry.Resolve()` method
will attempt to resolve both old (`AssemblyQualifiedName`) and new (short alias) formats:
- If an explicit alias is registered, it takes priority.
- If no alias is found, it returns `null`, and replay falls back to raw-bytes publishing.

#### Step 4: Upgrade Publishers

Once all consumers are upgraded, deploy the new version to publishers. New messages will use
the short alias format.

#### Step 5: Verify

Monitor logs for `DLQ replay: cannot resolve type alias` warnings. These indicate messages
from the old format that could not be resolved via alias lookup.

---

## 4. Mixed-Version Cluster Deployment

When running multiple versions of FlySwattr.NATS in the same NATS cluster:

### Rolling Upgrade Strategy

1. **Set `MaxAcceptedVersion`** on existing consumers to the current version before upgrading.
   This generates warning logs if a newer publisher sends a higher version.

2. **Upgrade consumers first** with both `MinAcceptedVersion = 1` and `MaxAcceptedVersion = 2`.

3. **Upgrade publishers** to emit the new version (e.g., `ProtocolVersion = 2`).

4. **Remove version bounds** from consumers once all instances are upgraded.

### Handling Version Mismatches

| Scenario | Default Behavior | Configuration |
|----------|-----------------|---------------|
| Message has no version header | Log info, process normally | Always allowed (backward compat) |
| Message version < MinAcceptedVersion | Depends on `MismatchAction` | `LogWarning` / `Terminate` / `Nack` |
| Message version > MaxAcceptedVersion | Log warning, process normally | Advisory only |

### Using `Nack` for Mixed Clusters

If your cluster has consumers at different versions, use `MismatchAction = Nack`:

```csharp
services.Configure<WireCompatibilityOptions>(opts =>
{
    opts.MinAcceptedVersion = 2;
    opts.MismatchAction = VersionMismatchAction.Nack;
});
```

This causes the old-format message to be redelivered to another consumer instance that
may be running the compatible version.

---

## 5. Non-.NET Consumer Implementation Guide

To implement a FlySwattr.NATS-compatible consumer in another language:

### Basic Message Consumption

1. **Subscribe** to the JetStream stream/consumer.
2. **Read headers** from the NATS message.
3. **Check `X-ClaimCheck-Ref`**: If present, download payload from Object Store.
4. **Deserialize** the payload using the format indicated by `Content-Type`.
5. **ACK** or **NAK** the message.

### Producing Messages

1. Set `Content-Type` to your serialization format.
2. Set `Nats-Msg-Id` to a stable, business-key-derived ID.
3. Set `X-FlySwattr-Version` to your protocol version (use `1` for compatibility).
4. Do **not** set reserved headers (`traceparent`, `tracestate`, `X-ClaimCheck-Ref`, `X-ClaimCheck-Type`).

### Serialization Compatibility

- **MemoryPack** is .NET-specific. Non-.NET consumers should use JSON or another cross-platform format.
- Register a custom `Content-Type` (e.g., `application/json`) and configure the
  `HybridNatsSerializer` to handle it on the .NET side.
- Alternatively, use raw byte publishing (`PublishRawAsync`) with a shared schema (e.g., Protobuf).

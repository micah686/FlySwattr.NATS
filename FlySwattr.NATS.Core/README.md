# FlySwattr.NATS.Core

**The engine room of the FlySwattr framework.**

This library provides the foundational implementations of the NATS messaging patterns, serialization logic, and state store wrappers. It serves as the base upon which resilience and other advanced features are layered.

## 🛠 Key Capabilities

### 1. Robust Messaging Implementation
Implements `IJetStreamPublisher` and `IJetStreamConsumer` using the modern `NATS.Net` v2 client.
*   **Application-Level Idempotency:** Enforces the use of `MessageId` for publishing to ensure exactly-once processing behavior across retries.
*   **Structured Logging:** Deep integration with `Microsoft.Extensions.Logging` for tracing message flows.

### 2. Hybrid Serialization
Optimized for both performance and compatibility.
*   **MemoryPack (Fast Path):** Uses `MemoryPack` for high-performance, zero-allocation binary serialization for internal communication.
*   **System.Text.Json (Fallback):** Automatically falls back to standard JSON for external interoperability or types not decorated with `[MemoryPackable]`.

### 3. Automatic Claim Check (Payload Offloading)
Solves the "1MB NATS Payload Limit" problem transparently.
*   **Publisher:** Automatically detects messages exceeding the threshold (configurable), uploads the payload to NATS Object Store, and publishes a lightweight reference message.
*   **Consumer:** Automatically detects reference messages, downloads the payload from Object Store, and presents the original message to your handler.
*   **Zero Code Change:** Your application logic doesn't need to know if the message was 1KB or 100MB.

### 4. DLQ Management
Provides comprehensive Dead Letter Queue tools:
*   `NatsDlqRemediationService`: APIs to Inspect, Replay, Edit, and Archive poison messages.
*   `DlqPolicyRegistry`: Configuration for routing failed messages.

## 📦 Usage

Usually configured via `FlySwattr.NATS` meta-package, but can be added manually:

```csharp
services.AddFlySwattrNatsCore(options =>
{
    options.Url = "nats://localhost:4222";
    options.MaxConcurrency = 100;
});

// Add Claim Check support
services.AddPayloadOffloading(opts =>
{
    opts.ThresholdBytes = 1024 * 1024; // 1MB
});
```

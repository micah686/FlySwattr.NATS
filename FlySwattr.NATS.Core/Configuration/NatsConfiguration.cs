using NATS.Client.Core;

namespace FlySwattr.NATS.Core.Configuration;

public class NatsConfiguration
{
    public string Url { get; set; } = "nats://localhost:4222";
    public NatsAuthOpts? NatsAuth { get; set; }
    public NatsTlsOpts? TlsOpts { get; set; }
    public TimeSpan? ReconnectWait { get; set; }
    public int? MaxReconnect { get; set; }

    // Concurrency Tuning
    public int MaxConcurrency { get; set; } = 100;

    /// <summary>
    /// Maximum payload size in bytes for the hybrid serializer.
    /// Messages larger than this will fail serialization.
    /// Default: 10MB (10 * 1024 * 1024 bytes).
    /// </summary>
    /// <remarks>
    /// This is independent of <see cref="PayloadOffloadingOptions.ThresholdBytes"/> which controls
    /// when payloads are offloaded to Object Store. MaxPayloadSize should be larger than or equal
    /// to the offloading threshold.
    /// </remarks>
    public int MaxPayloadSize { get; set; } = 10 * 1024 * 1024; // 10MB

    /// <summary>
    /// Enforce strict schema fingerprint matching during MemoryPack deserialization.
    /// When enabled (default), fingerprint mismatches throw an exception.
    /// When disabled, fingerprint mismatches emit a warning log instead.
    /// </summary>
    public bool EnforceSchemaFingerprint { get; set; } = true;

    /// <summary>
    /// Enable privacy-focused sanitization for exception messages captured in DLQ metadata and logs.
    /// Default: true.
    /// </summary>
    public bool SanitizeExceptionMessages { get; set; } = true;

    /// <summary>
    /// Maximum number of messages published concurrently within a single <c>PublishBatchAsync</c> call.
    /// Set to 0 to disable the cap (all messages publish concurrently).
    /// Default: 32.
    /// </summary>
    public int BatchPublishMaxConcurrency { get; set; } = 32;

    /// <summary>
    /// When <c>false</c>, reject NATS URLs whose scheme is not <c>tls</c> or <c>wss</c> at
    /// startup validation. Set to <c>false</c> in production to prevent accidentally shipping
    /// plaintext <c>nats://</c> or <c>ws://</c> connections.
    /// Default: <c>true</c> to preserve backward compatibility with the <c>nats://localhost:4222</c>
    /// default URL used in development and tests.
    /// </summary>
    public bool AllowInsecureTransport { get; set; } = true;
}

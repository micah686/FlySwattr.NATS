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
}

namespace FlySwattr.NATS.Core.Configuration;

/// <summary>
/// Configuration options for automatic large payload offloading (Claim Check pattern).
/// When a message payload exceeds the threshold, it is automatically offloaded to an
/// IObjectStore and replaced with a reference header.
/// </summary>
public class PayloadOffloadingOptions
{
    /// <summary>
    /// Header name used to store the claim check reference.
    /// Default: "X-ClaimCheck-Ref"
    /// </summary>
    public string ClaimCheckHeaderName { get; set; } = "X-ClaimCheck-Ref";

    /// <summary>
    /// Header name used to store the original message type for deserialization.
    /// Default: "X-ClaimCheck-Type"
    /// </summary>
    public string ClaimCheckTypeHeaderName { get; set; } = "X-ClaimCheck-Type";

    /// <summary>
    /// Payload size threshold in bytes. Messages larger than this will be offloaded.
    /// Default: 1MB (NATS default max payload size).
    /// </summary>
    public int ThresholdBytes { get; set; } = 1024 * 1024; // 1MB - NATS default max payload

    /// <summary>
    /// Optional keyed service key for resolving IObjectStore.
    /// If null, the default IObjectStore registration is used.
    /// </summary>
    public string? ObjectStoreServiceKey { get; set; }

    /// <summary>
    /// Prefix for object keys stored in IObjectStore.
    /// Default: "claimcheck"
    /// </summary>
    public string ObjectKeyPrefix { get; set; } = "claimcheck";
}

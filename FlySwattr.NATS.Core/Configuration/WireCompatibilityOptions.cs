namespace FlySwattr.NATS.Core.Configuration;

/// <summary>
/// Configuration options for wire-format versioning and cross-version compatibility.
/// Every published message is stamped with a protocol version header so consumers
/// can detect and handle version mismatches during rolling upgrades or in
/// mixed-client ecosystems.
/// </summary>
public class WireCompatibilityOptions
{
    /// <summary>
    /// The current protocol version stamped on every outgoing message.
    /// Increment this when making wire-format breaking changes
    /// (e.g., new required headers, serialization format changes).
    /// Default: 1
    /// </summary>
    public int ProtocolVersion { get; set; } = 1;

    /// <summary>
    /// Header name used to convey the protocol version.
    /// Default: "X-FlySwattr-Version"
    /// </summary>
    public string VersionHeaderName { get; set; } = "X-FlySwattr-Version";

    /// <summary>
    /// Minimum protocol version this consumer will accept.
    /// Messages with a version below this value trigger <see cref="MismatchAction"/>.
    /// Null means no minimum is enforced.
    /// </summary>
    public int? MinAcceptedVersion { get; set; }

    /// <summary>
    /// Maximum protocol version this consumer will accept without a warning.
    /// Messages with a version above this value trigger a warning log (the message
    /// is still processed). Null means no maximum is enforced.
    /// </summary>
    public int? MaxAcceptedVersion { get; set; }

    /// <summary>
    /// Action taken when a message's protocol version is below <see cref="MinAcceptedVersion"/>.
    /// Default: <see cref="VersionMismatchAction.LogWarning"/>
    /// </summary>
    public VersionMismatchAction MismatchAction { get; set; } = VersionMismatchAction.LogWarning;
}

/// <summary>
/// Defines what happens when a consumer receives a message with an incompatible protocol version.
/// </summary>
public enum VersionMismatchAction
{
    /// <summary>
    /// Log a warning and continue processing the message normally.
    /// Use during gradual rollouts where older publishers are still active.
    /// </summary>
    LogWarning,

    /// <summary>
    /// Terminate the message (do not redeliver).
    /// Use when the version gap is known to be unrecoverable.
    /// </summary>
    Terminate,

    /// <summary>
    /// NAK the message so the server can redeliver it to a compatible consumer.
    /// Use in mixed-version clusters where another consumer instance may handle it.
    /// </summary>
    Nack
}

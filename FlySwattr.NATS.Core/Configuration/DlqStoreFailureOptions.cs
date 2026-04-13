namespace FlySwattr.NATS.Core.Configuration;

/// <summary>
/// Configuration for DLQ store operational failure handling.
/// Defines explicit policies for what happens when the DLQ infrastructure
/// (KV bucket, object store) is unavailable.
/// </summary>
public class DlqStoreFailureOptions
{
    /// <summary>
    /// Policy applied when the DLQ KV bucket cannot be initialized (created or accessed).
    /// Default: <see cref="DlqStoreFailurePolicy.LogAndContinue"/> — matches current behavior
    /// where initialization failure is logged and individual operations fail on their own.
    /// </summary>
    public DlqStoreFailurePolicy InitializationFailurePolicy { get; set; }
        = DlqStoreFailurePolicy.LogAndContinue;

    /// <summary>
    /// Policy applied when an individual DLQ store operation (Store, Get, Update, Delete) fails
    /// due to infrastructure unavailability.
    /// Default: <see cref="DlqStoreFailurePolicy.Propagate"/> — the exception is re-thrown
    /// to the caller, which typically results in a NAK + retry.
    /// </summary>
    public DlqStoreFailurePolicy OperationFailurePolicy { get; set; }
        = DlqStoreFailurePolicy.Propagate;

    /// <summary>
    /// Policy applied when the object store is unavailable during poison-message handling
    /// and the serialized payload exceeds the inline size limit (1MB).
    /// Default: <see cref="LargePayloadFallbackPolicy.TruncateAndLog"/> — the payload
    /// is dropped and replaced with a truncation marker.
    /// </summary>
    public LargePayloadFallbackPolicy LargePayloadFallback { get; set; }
        = LargePayloadFallbackPolicy.TruncateAndLog;
}

/// <summary>
/// Defines what happens when DLQ store operations fail due to infrastructure unavailability.
/// </summary>
public enum DlqStoreFailurePolicy
{
    /// <summary>
    /// Re-throw the exception to the caller.
    /// For initialization: the first DLQ operation will throw.
    /// For operations: the caller (e.g., poison handler) will receive the exception,
    /// typically causing a NAK with backoff retry.
    /// </summary>
    Propagate,

    /// <summary>
    /// Log a warning/error and return a default result.
    /// For initialization: set <c>_isInitialized = true</c> and let individual operations fail.
    /// For operations: log the full entry details and return null/false/default.
    /// </summary>
    LogAndContinue,

    /// <summary>
    /// Fall back to logging the full DLQ entry details at Error level instead of storing.
    /// The DLQ entry data is preserved in structured logs for manual recovery.
    /// This is useful when you prefer observability over persistence during outages.
    /// </summary>
    FallbackToLogOnly
}

/// <summary>
/// Defines what happens when a poison message's payload exceeds the inline size limit
/// and the object store is unavailable for offloading.
/// </summary>
public enum LargePayloadFallbackPolicy
{
    /// <summary>
    /// Store an empty payload with a truncation marker in the encoding field.
    /// The payload data is lost but the DLQ entry metadata is preserved.
    /// This is the current default behavior.
    /// </summary>
    TruncateAndLog,

    /// <summary>
    /// Fail the entire DLQ operation, causing the message to be NAKed for later retry.
    /// Use this when payload preservation is critical and you prefer to retry
    /// when the object store recovers.
    /// </summary>
    FailDlq,

    /// <summary>
    /// Store the full payload inline regardless of size.
    /// WARNING: This may exceed NATS message size limits and cause publish failures.
    /// Only use when your NATS server is configured with a higher max_payload.
    /// </summary>
    ForceInline
}

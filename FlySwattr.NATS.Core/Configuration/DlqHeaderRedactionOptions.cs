namespace FlySwattr.NATS.Core.Configuration;

/// <summary>
/// Controls whether and how original message headers are redacted before being persisted to the DLQ store.
/// </summary>
public class DlqHeaderRedactionOptions
{
    /// <summary>
    /// Enable header redaction. When true (default), only headers whose names match
    /// <see cref="AllowedHeaderPrefixes"/> retain their values; all others have their
    /// values replaced with <see cref="RedactedValue"/>.
    /// Set to false to preserve all headers verbatim.
    /// </summary>
    public bool RedactHeaders { get; set; } = true;

    /// <summary>
    /// Case-insensitive set of header name prefixes that are allowed through when
    /// <see cref="RedactHeaders"/> is true. Headers whose names start with any of these
    /// prefixes retain their original values; all others are redacted.
    /// Example: <c>["X-Correlation-", "traceparent", "tracestate"]</c>
    /// </summary>
    public HashSet<string> AllowedHeaderPrefixes { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// The replacement value used for redacted header values. Default: <c>[REDACTED]</c>.
    /// </summary>
    public string RedactedValue { get; set; } = "[REDACTED]";
}

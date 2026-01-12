// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Abstractions.Exceptions;

/// <summary>
/// Exception thrown when message validation fails.
/// This exception is treated specially by the consumer pipeline:
/// messages that fail validation are routed directly to DLQ without
/// retries, as validation failures are considered permanent/non-transient.
/// </summary>
public class MessageValidationException : Exception
{
    /// <summary>
    /// The collection of validation error messages.
    /// </summary>
    public IReadOnlyList<string> Errors { get; }

    /// <summary>
    /// The subject of the message that failed validation.
    /// </summary>
    public string Subject { get; }

    public MessageValidationException(string subject, IEnumerable<string> errors)
        : base($"Validation failed for message on subject '{subject}': {string.Join("; ", errors)}")
    {
        Subject = subject;
        Errors = errors.ToList().AsReadOnly();
    }

    public MessageValidationException(string subject, IEnumerable<string> errors, Exception innerException)
        : base($"Validation failed for message on subject '{subject}': {string.Join("; ", errors)}", innerException)
    {
        Subject = subject;
        Errors = errors.ToList().AsReadOnly();
    }
}

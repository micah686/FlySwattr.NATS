// ReSharper disable CheckNamespace
// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Abstractions;

/// <summary>
/// Represents a single message in a batch publish operation.
/// </summary>
/// <typeparam name="T">The type of the message payload.</typeparam>
/// <param name="Subject">The subject to publish to.</param>
/// <param name="Message">The message payload.</param>
/// <param name="MessageId">A stable, business-key-derived message ID for JetStream de-duplication.</param>
/// <param name="Headers">Optional headers to include with the message.</param>
public record BatchMessage<T>(
    string Subject,
    T Message,
    string MessageId,
    MessageHeaders? Headers = null);

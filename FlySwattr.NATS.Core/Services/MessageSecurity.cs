using System.Text;
using FlySwattr.NATS.Abstractions;

namespace FlySwattr.NATS.Core.Services;

public static class MessageSecurity
{
    private const int MaxStoredErrorLength = 256;

    internal static readonly StringComparer HeaderComparer = StringComparer.OrdinalIgnoreCase;

    public static void ValidatePublishSubject(string subject)
    {
        SubjectName.From(subject);
    }

    public static void RejectReservedHeaders(
        MessageHeaders? headers,
        IEnumerable<string> reservedHeaderNames,
        string paramName = "headers")
    {
        if (headers == null || headers.Headers.Count == 0)
        {
            return;
        }

        var reserved = new HashSet<string>(reservedHeaderNames, HeaderComparer);
        var collisions = headers.Headers.Keys.Where(reserved.Contains).OrderBy(static x => x, HeaderComparer).ToArray();
        if (collisions.Length == 0)
        {
            return;
        }

        throw new ArgumentException(
            $"The following headers are reserved for internal use and cannot be overridden: {string.Join(", ", collisions)}",
            paramName);
    }

    public static string ValidateObjectStoreKey(string objectKey, string paramName = "objectKey")
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(objectKey, paramName);

        if (objectKey.StartsWith('/') || objectKey.EndsWith('/'))
        {
            throw new ArgumentException("Object store key cannot start or end with '/'.", paramName);
        }

        if (objectKey.Contains('\\', StringComparison.Ordinal) ||
            objectKey.Contains('\0') ||
            objectKey.Any(char.IsControl))
        {
            throw new ArgumentException("Object store key contains invalid characters.", paramName);
        }

        var segments = objectKey.Split('/', StringSplitOptions.None);
        if (segments.Any(static segment => segment.Length == 0 || segment == "." || segment == ".."))
        {
            throw new ArgumentException("Object store key contains invalid path traversal segments.", paramName);
        }

        return objectKey;
    }

    public static string SanitizeExceptionMessage(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        var builder = new StringBuilder(exception.Message.Length);
        foreach (var character in exception.Message)
        {
            builder.Append(char.IsControl(character) ? ' ' : character);
        }

        var normalized = string.Join(" ", builder.ToString().Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));
        var payload = string.IsNullOrWhiteSpace(normalized)
            ? exception.GetType().Name
            : $"{exception.GetType().Name}: {normalized}";

        return payload.Length <= MaxStoredErrorLength
            ? payload
            : $"{payload[..(MaxStoredErrorLength - 3)]}...";
    }
}

using System.Text;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Abstractions.Exceptions;
using NATS.Client.Core;

namespace FlySwattr.NATS.Core.Services;

public static class MessageSecurity
{
    private const int MaxStoredErrorLength = 256;
    private const string NatsHeaderPrefix = "Nats-";

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

    public static NatsHeaders BuildValidatedHeaders(
        MessageHeaders? headers,
        IEnumerable<string>? reservedHeaderNames = null,
        IEnumerable<string>? allowlistedNatsHeaders = null,
        string paramName = "headers")
    {
        if (reservedHeaderNames != null)
        {
            RejectReservedHeaders(headers, reservedHeaderNames, paramName);
        }

        var natsHeaders = new NatsHeaders();
        if (headers == null || headers.Headers.Count == 0)
        {
            return natsHeaders;
        }

        var allowlisted = allowlistedNatsHeaders is null
            ? null
            : new HashSet<string>(allowlistedNatsHeaders, HeaderComparer);

        foreach (var (key, value) in headers.Headers)
        {
            ValidateHeader(key, value, allowlisted, paramName);
            natsHeaders.Add(key, value);
        }

        return natsHeaders;
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

    public static string SanitizeExceptionMessage(Exception exception, bool enablePrivacySanitization = true)
    {
        ArgumentNullException.ThrowIfNull(exception);

        if (enablePrivacySanitization)
        {
            if (exception is MessageValidationException messageValidationException)
            {
                var propertyNames = messageValidationException.Errors
                    .Select(ExtractValidationPropertyName)
                    .Where(static x => !string.IsNullOrWhiteSpace(x))
                    .Distinct(StringComparer.Ordinal)
                    .ToArray();

                var details = propertyNames.Length > 0
                    ? $"Fields={string.Join(", ", propertyNames)}"
                    : $"{messageValidationException.Errors.Count} error(s)";

                return BuildPayload(exception.GetType().Name,
                    $"Validation failed for message on subject '{messageValidationException.Subject}'. {details}");
            }

            if (TryBuildFluentValidationSummary(exception, out var fluentValidationSummary))
            {
                return BuildPayload(exception.GetType().Name, fluentValidationSummary);
            }
        }

        return BuildPayload(exception.GetType().Name, exception.Message);
    }

    private static bool TryBuildFluentValidationSummary(Exception exception, out string summary)
    {
        summary = string.Empty;

        if (!string.Equals(exception.GetType().FullName, "FluentValidation.ValidationException", StringComparison.Ordinal))
        {
            return false;
        }

        var errorsProperty = exception.GetType().GetProperty("Errors");
        if (errorsProperty?.GetValue(exception) is not System.Collections.IEnumerable errors)
        {
            summary = "Validation failed";
            return true;
        }

        var fields = new HashSet<string>(StringComparer.Ordinal);
        foreach (var error in errors)
        {
            var propertyName = error?.GetType().GetProperty("PropertyName")?.GetValue(error) as string;
            if (!string.IsNullOrWhiteSpace(propertyName))
            {
                fields.Add(propertyName);
            }
        }

        summary = fields.Count > 0
            ? $"Validation failed. Fields={string.Join(", ", fields)}"
            : "Validation failed";

        return true;
    }

    private static string ExtractValidationPropertyName(string error)
    {
        if (string.IsNullOrWhiteSpace(error))
        {
            return string.Empty;
        }

        var colonIndex = error.IndexOf(':');
        return colonIndex <= 0 ? string.Empty : error[..colonIndex].Trim();
    }

    private static string BuildPayload(string exceptionTypeName, string message)
    {
        var builder = new StringBuilder(message.Length);
        foreach (var character in message)
        {
            builder.Append(char.IsControl(character) ? ' ' : character);
        }

        var normalized = string.Join(" ", builder.ToString().Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));
        var payload = string.IsNullOrWhiteSpace(normalized)
            ? exceptionTypeName
            : $"{exceptionTypeName}: {normalized}";

        return payload.Length <= MaxStoredErrorLength
            ? payload
            : $"{payload[..(MaxStoredErrorLength - 3)]}...";
    }

    private static void ValidateHeader(
        string key,
        string value,
        HashSet<string>? allowlistedNatsHeaders,
        string paramName)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            throw new ArgumentException("Header key cannot be null or whitespace", paramName);
        }

        if (key.Contains(':') || key.Any(char.IsControl))
        {
            throw new ArgumentException(
                $"Invalid header key '{key}'. Keys cannot contain colons or control characters.",
                paramName);
        }

        if (value == null)
        {
            throw new ArgumentException($"Header value for '{key}' cannot be null", paramName);
        }

        if (value.Any(char.IsControl))
        {
            throw new ArgumentException(
                $"Invalid header value for '{key}'. Values cannot contain control characters.",
                paramName);
        }

        if (key.StartsWith(NatsHeaderPrefix, StringComparison.OrdinalIgnoreCase) &&
            (allowlistedNatsHeaders == null || !allowlistedNatsHeaders.Contains(key)))
        {
            throw new ArgumentException(
                $"Header '{key}' is reserved for NATS internals and cannot be set by callers.",
                paramName);
        }
    }
}

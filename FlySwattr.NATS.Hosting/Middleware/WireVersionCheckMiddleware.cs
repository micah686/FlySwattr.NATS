using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace FlySwattr.NATS.Hosting.Middleware;

/// <summary>
/// Middleware that checks the protocol version header on incoming messages.
/// Enables safe rolling upgrades in mixed-version clusters by detecting and
/// handling version mismatches before the message reaches the handler.
/// </summary>
/// <typeparam name="T">The type of message being processed.</typeparam>
internal partial class WireVersionCheckMiddleware<T> : IConsumerMiddleware<T>
{
    private readonly WireCompatibilityOptions _options;
    private readonly ILogger _logger;

    public WireVersionCheckMiddleware(
        IOptions<WireCompatibilityOptions> options,
        ILogger<WireVersionCheckMiddleware<T>> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task InvokeAsync(IJsMessageContext<T> context, Func<Task> next, CancellationToken ct)
    {
        if (!context.Headers.Headers.TryGetValue(_options.VersionHeaderName, out var versionString))
        {
            // No version header — likely from a pre-versioning deployment
            LogNoVersionHeader(context.Subject, context.Sequence);
            await next();
            return;
        }

        if (!int.TryParse(versionString, out var messageVersion))
        {
            LogInvalidVersionHeader(context.Subject, context.Sequence, versionString);
            await next();
            return;
        }

        // Check minimum version
        if (_options.MinAcceptedVersion.HasValue && messageVersion < _options.MinAcceptedVersion.Value)
        {
            LogVersionBelowMinimum(context.Subject, context.Sequence, messageVersion, _options.MinAcceptedVersion.Value);

            switch (_options.MismatchAction)
            {
                case VersionMismatchAction.Terminate:
                    await context.TermAsync(ct);
                    return;

                case VersionMismatchAction.Nack:
                    await context.NackAsync(TimeSpan.FromSeconds(10), ct);
                    return;

                case VersionMismatchAction.LogWarning:
                default:
                    // Continue processing with warning already logged
                    break;
            }
        }

        // Check maximum version (advisory only)
        if (_options.MaxAcceptedVersion.HasValue && messageVersion > _options.MaxAcceptedVersion.Value)
        {
            LogVersionAboveMaximum(context.Subject, context.Sequence, messageVersion, _options.MaxAcceptedVersion.Value);
        }

        await next();
    }

    [LoggerMessage(Level = LogLevel.Information,
        Message = "Message on {Subject} (Seq: {Sequence}) has no protocol version header. " +
                  "This is expected during migration from pre-versioning deployments.")]
    private partial void LogNoVersionHeader(string subject, ulong sequence);

    [LoggerMessage(Level = LogLevel.Warning,
        Message = "Message on {Subject} (Seq: {Sequence}) has invalid version header value '{VersionValue}'. Processing anyway.")]
    private partial void LogInvalidVersionHeader(string subject, ulong sequence, string versionValue);

    [LoggerMessage(Level = LogLevel.Warning,
        Message = "Message on {Subject} (Seq: {Sequence}) has protocol version {MessageVersion} " +
                  "which is below the minimum accepted version {MinVersion}.")]
    private partial void LogVersionBelowMinimum(string subject, ulong sequence, int messageVersion, int minVersion);

    [LoggerMessage(Level = LogLevel.Warning,
        Message = "Message on {Subject} (Seq: {Sequence}) has protocol version {MessageVersion} " +
                  "which is above the maximum known version {MaxVersion}. " +
                  "This consumer may not fully understand the message format.")]
    private partial void LogVersionAboveMaximum(string subject, ulong sequence, int messageVersion, int maxVersion);
}

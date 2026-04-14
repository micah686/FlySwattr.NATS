using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace FlySwattr.NATS.Core.Decorators;

/// <summary>
/// Consumer middleware that deletes claim-check objects from the Object Store
/// after the message is successfully acknowledged. This is Layer 1 of the
/// three-layer cleanup strategy — the tightest cleanup loop.
/// </summary>
/// <remarks>
/// <para>
/// Cleanup is registered as a post-ack callback via <see cref="IPostAckLifecycle"/> so the
/// object store delete never happens before the NATS server has confirmed the ack.
/// Works with any context type that implements <see cref="IPostAckLifecycle"/> — both
/// <c>OffloadingMessageContext</c> (explicit consumer path) and
/// <c>HydratedMessageContext</c> (Hosting consumer path) support this interface.
/// </para>
/// <para>
/// Claim-check presence is detected via the standardized header
/// (<see cref="PayloadOffloadingOptions.ClaimCheckHeaderName"/>) rather than a concrete
/// context subtype, so this middleware is not coupled to any specific consumer stack.
/// </para>
/// <para>
/// If the delete fails, it is logged but swallowed — Layer 2 (Object Store TTL)
/// or Layer 3 (background sweep) will eventually clean up the orphaned object.
/// </para>
/// </remarks>
public sealed partial class ClaimCheckCleanupMiddleware<T> : IConsumerMiddleware<T>
{
    private readonly IObjectStore _objectStore;
    private readonly string _claimCheckHeaderName;
    private readonly ILogger<ClaimCheckCleanupMiddleware<T>> _logger;

    public ClaimCheckCleanupMiddleware(
        IObjectStore objectStore,
        IOptions<PayloadOffloadingOptions> options,
        ILogger<ClaimCheckCleanupMiddleware<T>> logger)
    {
        _objectStore = objectStore;
        _claimCheckHeaderName = options.Value.ClaimCheckHeaderName;
        _logger = logger;
    }

    public async Task InvokeAsync(IJsMessageContext<T> context, Func<Task> next, CancellationToken ct)
    {
        // Register cleanup as a post-ack callback so the object is only deleted after
        // the server has confirmed the ack. Detection is header-based so it works with
        // any context type (OffloadingMessageContext, HydratedMessageContext, etc.).
        if (context is IPostAckLifecycle lifecycle &&
            context.Headers.Headers.TryGetValue(_claimCheckHeaderName, out var claimCheckRef))
        {
            var objectKey = ExtractObjectKey(claimCheckRef);
            lifecycle.RegisterAfterAckCallback(async cleanupCt =>
            {
                try
                {
                    await _objectStore.DeleteAsync(objectKey, cleanupCt);
                    LogClaimCheckDeleted(objectKey);
                }
                catch (Exception ex)
                {
                    // Best-effort: swallow and let Layer 2 TTL or Layer 3 sweep handle it
                    LogClaimCheckDeleteFailed(objectKey, ex);
                }
            });
        }

        await next();
    }

    private static string ExtractObjectKey(string reference)
    {
        const string prefix = "objstore://";
        return reference.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)
            ? reference[prefix.Length..]
            : reference;
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Deleted claim-check object {ObjectKey} after successful ack")]
    private partial void LogClaimCheckDeleted(string objectKey);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to delete claim-check object {ObjectKey}. Object Store TTL will clean it up.")]
    private partial void LogClaimCheckDeleteFailed(string objectKey, Exception exception);
}

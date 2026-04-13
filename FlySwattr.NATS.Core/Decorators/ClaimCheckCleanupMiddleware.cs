using FlySwattr.NATS.Abstractions;
using Microsoft.Extensions.Logging;

namespace FlySwattr.NATS.Core.Decorators;

/// <summary>
/// Consumer middleware that deletes claim-check objects from the Object Store
/// after the handler (and ack) completes successfully. This is Layer 1 of the
/// three-layer cleanup strategy — the tightest cleanup loop.
/// </summary>
/// <remarks>
/// If the delete fails, it is logged but swallowed — Layer 2 (Object Store TTL)
/// or Layer 3 (background sweep) will eventually clean up the orphaned object.
/// </remarks>
public sealed partial class ClaimCheckCleanupMiddleware<T> : IConsumerMiddleware<T>
{
    private readonly IObjectStore _objectStore;
    private readonly ILogger<ClaimCheckCleanupMiddleware<T>> _logger;

    public ClaimCheckCleanupMiddleware(
        IObjectStore objectStore,
        ILogger<ClaimCheckCleanupMiddleware<T>> logger)
    {
        _objectStore = objectStore;
        _logger = logger;
    }

    public async Task InvokeAsync(IJsMessageContext<T> context, Func<Task> next, CancellationToken ct)
    {
        await next();

        // After handler + ack completes, clean up the claim-check object if one was resolved
        if (context is OffloadingMessageContext<T> { ClaimCheckObjectKey: { } objectKey })
        {
            try
            {
                await _objectStore.DeleteAsync(objectKey, ct);
                LogClaimCheckDeleted(objectKey);
            }
            catch (Exception ex)
            {
                // Best-effort: swallow and let Layer 2 TTL or Layer 3 sweep handle it
                LogClaimCheckDeleteFailed(objectKey, ex);
            }
        }
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Deleted claim-check object {ObjectKey} after successful processing")]
    private partial void LogClaimCheckDeleted(string objectKey);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to delete claim-check object {ObjectKey}. Object Store TTL will clean it up.")]
    private partial void LogClaimCheckDeleteFailed(string objectKey, Exception exception);
}

using System.Diagnostics;
using FlySwattr.NATS.Abstractions;
using Microsoft.Extensions.Logging;

namespace FlySwattr.NATS.Hosting.Middleware;

/// <summary>
/// Middleware that provides standardized logging for message processing.
/// Logs message handling start, completion, and duration for observability.
/// </summary>
/// <typeparam name="T">The type of message being processed.</typeparam>
internal partial class LoggingMiddleware<T> : IConsumerMiddleware<T>
{
    private readonly ILogger _logger;

    public LoggingMiddleware(ILogger<LoggingMiddleware<T>> logger)
    {
        _logger = logger;
    }

    public async Task InvokeAsync(IJsMessageContext<T> context, Func<Task> next, CancellationToken ct)
    {
        var messageType = typeof(T).Name;
        LogHandlingMessage(context.Subject, messageType, context.Sequence);

        var stopwatch = Stopwatch.StartNew();
        try
        {
            await next();
            stopwatch.Stop();
            LogHandledMessage(context.Subject, messageType, context.Sequence, stopwatch.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            LogMessageFailed(context.Subject, messageType, context.Sequence, stopwatch.ElapsedMilliseconds, ex);
            throw;
        }
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Handling message on {Subject} (Type: {MessageType}, Seq: {Sequence})")]
    private partial void LogHandlingMessage(string subject, string messageType, ulong sequence);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Handled message on {Subject} (Type: {MessageType}, Seq: {Sequence}) in {ElapsedMs}ms")]
    private partial void LogHandledMessage(string subject, string messageType, ulong sequence, long elapsedMs);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Message failed on {Subject} (Type: {MessageType}, Seq: {Sequence}) after {ElapsedMs}ms")]
    private partial void LogMessageFailed(string subject, string messageType, ulong sequence, long elapsedMs, Exception exception);
}

using FlySwattr.NATS.Core.Telemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

namespace FlySwattr.NATS.Extensions;

/// <summary>
/// Extension methods for integrating FlySwattr.NATS with OpenTelemetry.
/// </summary>
public static class OpenTelemetryExtensions
{
    /// <summary>
    /// Adds FlySwattr.NATS tracing instrumentation to the TracerProviderBuilder.
    /// </summary>
    /// <remarks>
    /// This registers the FlySwattr.NATS ActivitySource for distributed tracing.
    /// Trace spans are created for:
    /// <list type="bullet">
    ///   <item><description>Publishing messages (Core and JetStream)</description></item>
    ///   <item><description>Receiving/processing messages</description></item>
    ///   <item><description>Request/Reply operations</description></item>
    /// </list>
    /// Trace context is automatically propagated via message headers (traceparent/tracestate).
    /// </remarks>
    /// <param name="builder">The TracerProviderBuilder to configure.</param>
    /// <returns>The TracerProviderBuilder for chaining.</returns>
    /// <example>
    /// <code>
    /// builder.Services.AddOpenTelemetry()
    ///     .WithTracing(tracing => tracing
    ///         .AddFlySwattrNatsInstrumentation()
    ///         .AddOtlpExporter());
    /// </code>
    /// </example>
    public static TracerProviderBuilder AddFlySwattrNatsInstrumentation(this TracerProviderBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.AddSource(NatsTelemetry.ActivitySourceName);
    }

    /// <summary>
    /// Adds FlySwattr.NATS metrics instrumentation to the MeterProviderBuilder.
    /// </summary>
    /// <remarks>
    /// This registers the FlySwattr.NATS Meter for metrics collection.
    /// Metrics include:
    /// <list type="bullet">
    ///   <item><description><c>flyswattr.nats.messages.published</c> - Counter of published messages</description></item>
    ///   <item><description><c>flyswattr.nats.messages.received</c> - Counter of received messages</description></item>
    ///   <item><description><c>flyswattr.nats.messages.failed</c> - Counter of message processing failures</description></item>
    ///   <item><description><c>flyswattr.nats.message.processing.duration</c> - Histogram of handler execution time</description></item>
    ///   <item><description><c>flyswattr.nats.publish.duration</c> - Histogram of publish operation time</description></item>
    ///   <item><description><c>flyswattr.nats.kv.operation.duration</c> - Histogram of KV store operation time</description></item>
    ///   <item><description><c>flyswattr.nats.objectstore.operation.duration</c> - Histogram of Object Store operation time</description></item>
    /// </list>
    /// </remarks>
    /// <param name="builder">The MeterProviderBuilder to configure.</param>
    /// <returns>The MeterProviderBuilder for chaining.</returns>
    /// <example>
    /// <code>
    /// builder.Services.AddOpenTelemetry()
    ///     .WithMetrics(metrics => metrics
    ///         .AddFlySwattrNatsInstrumentation()
    ///         .AddOtlpExporter());
    /// </code>
    /// </example>
    public static MeterProviderBuilder AddFlySwattrNatsInstrumentation(this MeterProviderBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.AddMeter(NatsTelemetry.MeterName);
    }
}

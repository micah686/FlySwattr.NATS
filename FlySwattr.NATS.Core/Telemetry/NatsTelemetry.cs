using FlySwattr.NATS.Abstractions;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using NATS.Client.Core;

namespace FlySwattr.NATS.Core.Telemetry;

/// <summary>
/// OpenTelemetry instrumentation for FlySwattr.NATS messaging operations.
/// Provides both tracing (via ActivitySource) and metrics (via Meter).
/// </summary>
public static class NatsTelemetry
{
    // ==================== Tracing ====================
    
    /// <summary>
    /// The name of the ActivitySource for FlySwattr.NATS tracing.
    /// Use this with TracerProviderBuilder.AddSource() for OpenTelemetry integration.
    /// </summary>
    public const string ActivitySourceName = "FlySwattr.NATS";
    
    /// <summary>
    /// The ActivitySource for creating distributed tracing spans.
    /// </summary>
    public static readonly ActivitySource ActivitySource = new(ActivitySourceName);

    // ==================== Metrics ====================
    
    /// <summary>
    /// The name of the Meter for FlySwattr.NATS metrics.
    /// Use this with MeterProviderBuilder.AddMeter() for OpenTelemetry integration.
    /// </summary>
    public const string MeterName = "FlySwattr.NATS";
    
    /// <summary>
    /// The Meter for recording FlySwattr.NATS metrics.
    /// </summary>
    public static readonly Meter Meter = new(MeterName, "1.0.0");

    // ---- Counters ----
    
    /// <summary>
    /// Counter for messages published (Core and JetStream).
    /// Tags: messaging.destination.name, messaging.nats.stream (JetStream only)
    /// </summary>
    public static readonly Counter<long> MessagesPublished = Meter.CreateCounter<long>(
        "flyswattr.nats.messages.published",
        unit: "{message}",
        description: "Number of messages published to NATS");

    /// <summary>
    /// Counter for messages received/processed.
    /// Tags: messaging.destination.name, messaging.nats.stream, messaging.nats.consumer
    /// </summary>
    public static readonly Counter<long> MessagesReceived = Meter.CreateCounter<long>(
        "flyswattr.nats.messages.received",
        unit: "{message}",
        description: "Number of messages received from NATS");

    /// <summary>
    /// Counter for message processing failures.
    /// Tags: messaging.destination.name, error.type
    /// </summary>
    public static readonly Counter<long> MessagesFailed = Meter.CreateCounter<long>(
        "flyswattr.nats.messages.failed",
        unit: "{message}",
        description: "Number of message processing failures");

    // ---- Histograms ----
    
    /// <summary>
    /// Histogram for message handler execution duration.
    /// Tags: messaging.destination.name, messaging.nats.stream
    /// </summary>
    public static readonly Histogram<double> MessageProcessingDuration = Meter.CreateHistogram<double>(
        "flyswattr.nats.message.processing.duration",
        unit: "ms",
        description: "Duration of message handler execution");

    /// <summary>
    /// Histogram for publish operation duration.
    /// Tags: messaging.destination.name
    /// </summary>
    public static readonly Histogram<double> PublishDuration = Meter.CreateHistogram<double>(
        "flyswattr.nats.publish.duration",
        unit: "ms",
        description: "Duration of publish operations");

    /// <summary>
    /// Histogram for KV store operation duration.
    /// Tags: operation (get, put, delete, watch), bucket
    /// </summary>
    public static readonly Histogram<double> KvOperationDuration = Meter.CreateHistogram<double>(
        "flyswattr.nats.kv.operation.duration",
        unit: "ms",
        description: "Duration of Key-Value store operations");

    /// <summary>
    /// Histogram for Object Store operation duration.
    /// Tags: operation (get, put, delete, list), bucket
    /// </summary>
    public static readonly Histogram<double> ObjectStoreOperationDuration = Meter.CreateHistogram<double>(
        "flyswattr.nats.objectstore.operation.duration",
        unit: "ms",
        description: "Duration of Object Store operations");

    // ==================== Semantic Conventions ====================

    // Semantic Conventions for Messaging
    public const string MessagingSystem = "messaging.system";
    public const string MessagingSystemName = "nats";
    public const string MessagingDestinationName = "messaging.destination.name";
    public const string MessagingOperation = "messaging.operation";
    public const string MessagingMessageId = "messaging.message.id";
    public const string MessagingMessagePayloadSizeBytes = "messaging.message.body.size";
    public const string MessagingConversationId = "messaging.conversation_id";
    
    // NATS Specific
    public const string NatsStream = "messaging.nats.stream";
    public const string NatsConsumer = "messaging.nats.consumer";
    public const string NatsQueueGroup = "messaging.nats.queue_group";
    public const string NatsSubject = "messaging.nats.subject";

    public static void InjectTraceContext(Activity? activity, NatsHeaders headers)
    {
        if (activity?.Id == null) return;
        
        headers["traceparent"] = activity.Id;
        if (activity.TraceStateString != null)
        {
            headers["tracestate"] = activity.TraceStateString;
        }
    }

    public static ActivityContext ExtractTraceContext(NatsHeaders? headers)
    {
        if (headers == null) return default;
        
        if (headers.TryGetValue("traceparent", out var traceparentValues) && 
            traceparentValues.ToString() is { } traceparent)
        {
            headers.TryGetValue("tracestate", out var tracestateValues);
            var tracestate = tracestateValues.ToString();
            
            if (ActivityContext.TryParse(traceparent, tracestate, out var context))
            {
                return context;
            }
        }
        
        return default;
    }

    public static ActivityContext ExtractTraceContext(MessageHeaders? headers)
    {
        if (headers == null) return default;

        if (headers.Headers.TryGetValue("traceparent", out var traceparent))
        {
            headers.Headers.TryGetValue("tracestate", out var tracestate);
            
            if (ActivityContext.TryParse(traceparent, tracestate, out var context))
            {
                return context;
            }
        }
        
        return default;
    }
}

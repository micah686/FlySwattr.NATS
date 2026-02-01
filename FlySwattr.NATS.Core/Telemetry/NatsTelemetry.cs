using FlySwattr.NATS.Abstractions;
using System.Diagnostics;
using NATS.Client.Core;

namespace FlySwattr.NATS.Core.Telemetry;

public static class NatsTelemetry
{
    public const string ActivitySourceName = "FlySwattr.NATS";
    public static readonly ActivitySource ActivitySource = new(ActivitySourceName);

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

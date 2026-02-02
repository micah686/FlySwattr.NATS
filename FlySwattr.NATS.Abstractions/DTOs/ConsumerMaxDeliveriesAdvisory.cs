using System.Text.Json.Serialization;

// ReSharper disable CheckNamespace
// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Abstractions;

/// <summary>
/// Represents a NATS JetStream advisory event for when a consumer 
/// exceeds its maximum delivery attempts (MaxDeliver) for a message.
/// Schema type: io.nats.jetstream.advisory.v1.max_deliver
/// Subject: $JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.{stream}.{consumer}
/// </summary>
/// <param name="Type">The schema type identifier (io.nats.jetstream.advisory.v1.max_deliver).</param>
/// <param name="Id">Unique identifier for this advisory event.</param>
/// <param name="Timestamp">When the advisory was generated.</param>
/// <param name="Stream">The name of the JetStream stream.</param>
/// <param name="Consumer">The name of the consumer that exceeded MaxDeliver.</param>
/// <param name="StreamSeq">The stream sequence number of the message that failed.</param>
/// <param name="Deliveries">The number of delivery attempts made.</param>
/// <param name="Domain">Optional JetStream domain for multi-tenant setups.</param>
public record ConsumerMaxDeliveriesAdvisory(
    [property: JsonPropertyName("type")] string Type,
    [property: JsonPropertyName("id")] string Id,
    [property: JsonPropertyName("timestamp")] DateTimeOffset Timestamp,
    [property: JsonPropertyName("stream")] string Stream,
    [property: JsonPropertyName("consumer")] string Consumer,
    [property: JsonPropertyName("stream_seq")] ulong StreamSeq,
    [property: JsonPropertyName("deliveries")] int Deliveries,
    [property: JsonPropertyName("domain")] string? Domain = null,
    [property: JsonPropertyName("subject")] string? Subject = null
);

using FlySwattr.NATS.Abstractions;

namespace Shared;

/// <summary>
/// Defines the JetStream topology for the Orders domain.
/// Provisions ORDERS_STREAM and a durable consumer with DLQ policy.
/// </summary>
public class OrdersTopology : ITopologySource
{
    public IEnumerable<StreamSpec> GetStreams()
    {
        yield return new StreamSpec
        {
            Name = StreamName.From("ORDERS_STREAM"),
            Subjects = ["orders.>"],
            StorageType = StorageType.File,
            RetentionPolicy = StreamRetention.Limits,
            MaxAge = TimeSpan.FromDays(7)
        };
        
        // DLQ stream for failed messages
        yield return new StreamSpec
        {
            Name = StreamName.From("DLQ_STREAM"),
            Subjects = ["dlq.>"],
            StorageType = StorageType.File,
            RetentionPolicy = StreamRetention.Limits,
            MaxAge = TimeSpan.FromDays(30)
        };
    }

    public IEnumerable<ConsumerSpec> GetConsumers()
    {
        yield return new ConsumerSpec
        {
            StreamName = StreamName.From("ORDERS_STREAM"),
            DurableName = ConsumerName.From("orders-consumer"),
            Description = "Primary consumer for order events with DLQ support",
            FilterSubject = "orders.>",
            AckPolicy = AckPolicy.Explicit,
            AckWait = TimeSpan.FromSeconds(30),
            MaxDeliver = 3,
            Backoff = [TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(10)],
            DeadLetterPolicy = new DeadLetterPolicy
            {
                SourceStream = "ORDERS_STREAM",
                SourceConsumer = "orders-consumer",
                TargetStream = StreamName.From("DLQ_STREAM"),
                TargetSubject = "dlq.orders"
            }
        };
    }
}

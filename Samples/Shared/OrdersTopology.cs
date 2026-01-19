using FlySwattr.NATS.Abstractions;

namespace Shared;

/// <summary>
/// Defines the NATS infrastructure topology for the sample applications.
/// This demonstrates the "Infrastructure as Code" pattern where all streams,
/// consumers, buckets, and object stores are declaratively defined.
/// </summary>
public class OrdersTopology : ITopologySource
{
    /// <summary>
    /// Stream name for order events.
    /// </summary>
    public static readonly StreamName OrdersStreamName = StreamName.From("orders-stream");

    /// <summary>
    /// Stream name for work tasks (queue group demo).
    /// </summary>
    public static readonly StreamName TasksStreamName = StreamName.From("tasks-stream");

    /// <summary>
    /// Consumer name for order processing.
    /// </summary>
    public static readonly ConsumerName OrderProcessorConsumer = ConsumerName.From("order-processor");

    /// <summary>
    /// Consumer name for work task processing.
    /// </summary>
    public static readonly ConsumerName TaskProcessorConsumer = ConsumerName.From("task-processor");

    /// <summary>
    /// KV bucket name for application configuration.
    /// </summary>
    public static readonly BucketName ConfigBucket = BucketName.From("sample-config");

    /// <summary>
    /// Object store bucket name for documents.
    /// </summary>
    public static readonly BucketName DocumentsBucket = BucketName.From("sample-documents");

    /// <summary>
    /// DLQ stream name for failed order messages.
    /// </summary>
    public static readonly StreamName OrdersDlqStream = StreamName.From("orders-dlq");

    public IEnumerable<StreamSpec> GetStreams()
    {
        yield return new StreamSpec
        {
            Name = OrdersStreamName,
            Subjects = ["orders.>"],
            MaxAge = TimeSpan.FromDays(7),
            StorageType = StorageType.File,
            RetentionPolicy = StreamRetention.Limits,
            Replicas = 1
        };

        yield return new StreamSpec
        {
            Name = TasksStreamName,
            Subjects = ["tasks.>"],
            MaxAge = TimeSpan.FromDays(1),
            StorageType = StorageType.File,
            RetentionPolicy = StreamRetention.WorkQueue,
            Replicas = 1
        };
    }

    public IEnumerable<ConsumerSpec> GetConsumers()
    {
        yield return new ConsumerSpec
        {
            StreamName = OrdersStreamName,
            DurableName = OrderProcessorConsumer,
            Description = "Processes order events with DLQ support",
            FilterSubject = "orders.created",
            AckPolicy = AckPolicy.Explicit,
            DeliverPolicy = DeliverPolicy.All,
            AckWait = TimeSpan.FromSeconds(30),
            MaxDeliver = 3,
            Backoff = [TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(10)],
            DeadLetterPolicy = new DeadLetterPolicy
            {
                SourceStream = OrdersStreamName.Value,
                SourceConsumer = OrderProcessorConsumer.Value,
                TargetStream = OrdersDlqStream,
                TargetSubject = "dlq.orders.failed"
            }
        };

        yield return new ConsumerSpec
        {
            StreamName = TasksStreamName,
            DurableName = TaskProcessorConsumer,
            Description = "Processes work tasks in a queue group",
            FilterSubject = "tasks.work",
            AckPolicy = AckPolicy.Explicit,
            DeliverPolicy = DeliverPolicy.All,
            AckWait = TimeSpan.FromSeconds(60),
            MaxDeliver = 5
        };
    }

    public IEnumerable<BucketSpec> GetBuckets()
    {
        yield return new BucketSpec
        {
            Name = ConfigBucket,
            StorageType = StorageType.File,
            History = 5,
            Description = "Sample application configuration"
        };
    }

    public IEnumerable<ObjectStoreSpec> GetObjectStores()
    {
        yield return new ObjectStoreSpec
        {
            Name = DocumentsBucket,
            StorageType = StorageType.File,
            Description = "Sample document storage"
        };
    }
}

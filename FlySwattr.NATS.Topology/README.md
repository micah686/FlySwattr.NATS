# FlySwattr.NATS.Topology

**Declarative "Infrastructure-as-Code" for NATS JetStream.**

Stop manually running CLI commands to set up your streams. This library allows you to define your Streams, Consumers, and Buckets in code, and ensures they exist exactly as specified when your application starts.

## 🌟 Key Features

### 1. Declarative Definitions
Define your infrastructure requirements by implementing `ITopologySource`.

```csharp
public class OrderTopology : ITopologySource
{
    public IEnumerable<StreamSpec> GetStreams()
    {
        yield return new StreamSpec
        {
            Name = StreamName.From("ORDERS"),
            Subjects = new[] { "orders.>" },
            StorageType = StorageType.File,
            Replicas = 3
        };
    }

    public IEnumerable<ConsumerSpec> GetConsumers()
    {
        yield return new ConsumerSpec
        {
            StreamName = StreamName.From("ORDERS"),
            DurableName = ConsumerName.From("order-processor"),
            AckPolicy = AckPolicy.Explicit
        };
    }
}
```

### 2. Cold Start Protection
In containerized environments (Kubernetes), your app might start before NATS is ready. This library includes a startup guard that:
*   Waits for NATS connection with exponential backoff.
*   Blocks application startup until essential topology is provisioned.
*   Prevents `CrashLoopBackOff` scenarios caused by missing streams.

### 3. Safe Migrations
*   **Idempotent:** Safe to run on every startup.
*   **Immutable Property Checks:** Detects if you try to change an immutable property (like changing Stream Storage from File to Memory) and fails fast with a clear error, protecting you from data loss.

## 📦 Usage

```csharp
// Register your topology definitions
services.AddNatsTopologySource<OrderTopology>();
services.AddNatsTopologySource<PaymentTopology>();

// Enable the startup provisioning service
services.AddNatsTopologyProvisioning(options =>
{
    options.MaxRetryAttempts = 10;
    options.TotalStartupTimeout = TimeSpan.FromMinutes(2);
});
```

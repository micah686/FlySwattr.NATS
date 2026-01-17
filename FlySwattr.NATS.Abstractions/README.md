# FlySwattr.NATS.Abstractions

**The contract layer for the FlySwattr NATS ecosystem.**

This library contains the pure interfaces, DTOs, and exceptions that define the behavior of the system. It has minimal dependencies, allowing you to decouple your domain logic from concrete NATS implementations.

## 📦 What's Inside?

### Core Interfaces
*   `IMessageBus`: High-level abstraction for standard Pub/Sub and Request/Reply.
*   `IJetStreamPublisher` / `IJetStreamConsumer`: Contracts for durable, stream-based messaging.
*   `IKeyValueStore` / `IObjectStore`: Abstractions for NATS state stores.
*   `ITopologyManager`: Contract for managing Streams and Consumers declaratively.

### Message Context
*   `IJsMessageContext<T>`: Provides access to message metadata (Headers, Sequence, Timestamp) and acknowledgement methods (`AckAsync`, `NackAsync`) without tying handlers to the NATS client SDK directly.

### Usage Example

Inject these interfaces into your services to keep them testable and decoupled:

```csharp
public class OrderProcessor
{
    private readonly IJetStreamPublisher _publisher;
    private readonly IKeyValueStore _kvStore;

    public OrderProcessor(IJetStreamPublisher publisher, IKeyValueStore kvStore)
    {
        _publisher = publisher;
        _kvStore = kvStore;
    }

    public async Task ProcessOrder(Order order)
    {
        // 1. Update state
        await _kvStore.PutAsync($"order:{order.Id}", "Processing");

        // 2. Publish event (with idempotency key)
        await _publisher.PublishAsync("orders.processed", order, $"cmd-{order.Id}");
    }
}
```

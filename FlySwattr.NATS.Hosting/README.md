# FlySwattr.NATS.Hosting

**The runtime environment for NATS consumers.**

This library provides the background services, health checks, and middleware pipelines needed to run NATS consumers reliably in a .NET Host.

## 🌟 Key Features

### 1. NatsConsumerBackgroundService
A production-ready `BackgroundService` that manages the consume loop:
*   **Parallelism:** Configurable concurrency (`MaxDegreeOfParallelism`).
*   **Backpressure:** Uses bounded channels to prevent memory explosion if the handler is slow.
*   **Middleware:** Supports a "Russian Doll" pipeline for cross-cutting concerns like Logging and Validation.

### 2. Observability & Health
*   **Zombie Detection:** Monitors consumers to ensure they are actively processing messages. If a consumer loop hangs (deadlock), the Health Check reports `Unhealthy`.
*   **Startup Probes:** Ensures NATS is reachable before traffic starts.

### 3. Control Plane Monitoring
*   **DLQ Advisory Listener:** Listens to NATS system events (`$JS.EVENT.ADVISORY...`) to detect when messages exceed `MaxDeliver`. This provides visibility into poison messages that the application might have missed.

## 📦 Usage

Registering a worker:

```csharp
services.AddNatsConsumer<OrderCreatedEvent>(
    streamName: "ORDERS",
    consumerName: "order-processor",
    handler: async context => 
    {
        _logger.LogInformation("Processing order {Id}", context.Message.OrderId);
        await context.AckAsync();
    },
    options => 
    {
        options.MaxConcurrency = 10;
        options.EnableLoggingMiddleware = true;
    });
```

Enabling health checks:

```csharp
// Program.cs
services.AddFlySwattrNatsHosting();

// ...
app.MapHealthChecks("/health/ready"); // Includes Zombie Consumer check
```

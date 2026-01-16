using ConsoleApp2;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Extensions;
using FlySwattr.NATS.Hosting.Extensions;
using FlySwattr.NATS.Resilience.Extensions;
using FlySwattr.NATS.Topology.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

// ---- The Worker (Resilient Consumer) ----
// This application demonstrates:
// - JetStream Consumer (pull-based)
// - Queue Groups for load balancing
// - Resilience patterns (retry, circuit breaker)
// - Request/Reply responder
// - Distributed Lock contention

Console.WriteLine("🔧 Starting ConsoleApp2: The Worker");

var builder = Host.CreateApplicationBuilder(args);

// Configure logging
builder.Logging.SetMinimumLevel(LogLevel.Information);
builder.Logging.AddFilter("FlySwattr", LogLevel.Debug);
builder.Logging.AddFilter("NATS", LogLevel.Warning);

// 1. Add NATS Core
builder.Services.AddFlySwattrNatsCore(opts =>
{
    opts.Url = "nats://localhost:4222";
});

// 2. Add Topology (for signal, not provisioning - Commander does that)
builder.Services.AddFlySwattrNatsTopology();

// 3. Add Resilience (circuit breaker, bulkhead, retry)
builder.Services.AddFlySwattrNatsResilience(opts =>
{
    opts.NamedPools["default"] = 50;
    opts.NamedPools["critical"] = 10;
});

// 4. Add Hosting services (health checks, startup validation)
builder.Services.AddFlySwattrNatsHosting();

// 5. Register OrderProcessor as a singleton
builder.Services.AddSingleton<OrderProcessor>();

// 6. Register the JetStream consumer
builder.Services.AddNatsConsumer<OrderCreated>(
    streamName: "orders",
    consumerName: "worker-consumer",
    handler: async context =>
    {
        // Resolve processor from DI
        var processor = context.Message as object; // Workaround: get from scope
        
        // For now, inline the processing logic
        var order = context.Message;
        Console.WriteLine($"📥 Processing Order {order.OrderId} (${order.Amount}) - Delivery #{context.NumDelivered}");
        
        // Simulate work
        await Task.Delay(Random.Shared.Next(300, 700));
        
        // Simulate occasional failures for resilience demo
        if (context.NumDelivered == 1 && Random.Shared.NextDouble() < 0.15)
        {
            Console.WriteLine($"⚠️ Simulated failure processing {order.OrderId}");
            throw new InvalidOperationException($"Simulated processing failure for {order.OrderId}");
        }
        
        await context.AckAsync();
        Console.WriteLine($"✅ Processed Order {order.OrderId}");
    },
    configureOptions: opts =>
    {
        opts.MaxConcurrency = 5;
        opts.EnableLoggingMiddleware = true;
        opts.EnableValidationMiddleware = false;
    });

// 7. Add the Request/Reply responder service
builder.Services.AddHostedService<ResponderService>();

var host = builder.Build();

Console.WriteLine("📋 Worker services configured:");
Console.WriteLine("   - NATS Core (connection, JetStream)");
Console.WriteLine("   - Resilience (circuit breaker, retry)");
Console.WriteLine("   - JetStream Consumer (orders stream)");
Console.WriteLine("   - Request/Reply Responder");
Console.WriteLine();
Console.WriteLine("Press Ctrl+C to exit.\n");

await host.RunAsync();
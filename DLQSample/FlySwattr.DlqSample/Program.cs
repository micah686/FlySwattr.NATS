using FlySwattr.DlqSample;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core;
using FlySwattr.NATS.Core.Configuration;
using FlySwattr.NATS.Extensions;
using FlySwattr.NATS.Hosting.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream.Models;

// 1. Setup Host
var builder = Host.CreateApplicationBuilder(args);

// Configure Logging
builder.Logging.ClearProviders();
builder.Logging.AddConsole();
builder.Logging.SetMinimumLevel(LogLevel.Information);

// Configure NATS (Golden Path)
builder.Services.AddEnterpriseNATSMessaging(config =>
{
    config.Core.Url = "nats://localhost:4222";
    config.Core.MaxPayloadSize = 1024 * 1024; // 1MB
});

// Replace default Logging notification service with our Storage one so we can list it later
builder.Services.AddSingleton<IDlqNotificationService, DlqStorageNotificationService>();

// Register DLQ Policy for our stream
builder.Services.AddSingleton<IHostedService, PolicyRegistrationService>();

// Register the Consumer with a handler that fails for specific messages
builder.Services.AddNatsConsumer<UserCreatedEvent>(
    streamName: "EVENTS",
    consumerName: "user-processor",
    handler: async context =>
    {
        var user = context.Message;
        Console.WriteLine($"[Consumer] Processing User: {user.Username} ({user.UserId})");

        if (user.Username == "PoisonIvy")
        {
            Console.WriteLine($"[Consumer] ☠️ POISON MESSAGE DETECTED! Throwing exception for {user.Username}...");
            throw new InvalidOperationException("I am allergic to Ivy!");
        }

        await context.AckAsync();
        Console.WriteLine($"[Consumer] User {user.Username} processed successfully.");
    },
    options =>
    {
        options.MaxConcurrency = 1;
        options.DlqPolicy = new DeadLetterPolicy
        {
            SourceStream = "EVENTS",
            SourceConsumer = "user-processor",
            TargetStream = StreamName.From("DLQ"),
            TargetSubject = "events.dlq"
        };
    }
);

var host = builder.Build();

// 2. Provision Topology (Manually for this simple sample)
var publisher = host.Services.GetRequiredService<IJetStreamPublisher>();
var dlqStore = host.Services.GetRequiredService<IDlqStore>();
// We need raw context to create stream if it doesn't exist
var natsContext = host.Services.GetRequiredService<NATS.Client.JetStream.INatsJSContext>();

Console.WriteLine("\n--- Setting up Topology ---");
try
{
    // Check if stream exists first to avoid error or update it
    try 
    {
        await natsContext.GetStreamAsync("EVENTS");
        Console.WriteLine("Stream 'EVENTS' verified.");
    }
    catch
    {
        await natsContext.CreateStreamAsync(new StreamConfig("EVENTS", ["events.>"]));
        Console.WriteLine("Stream 'EVENTS' created.");
    }

    // Ensure consumer exists with MaxDeliver=3
    // Note: In a real app, TopologyManager handles this.
    try
    {
        await natsContext.CreateOrUpdateConsumerAsync("EVENTS", new ConsumerConfig("user-processor")
        {
             DurableName = "user-processor",
             AckPolicy = ConsumerConfigAckPolicy.Explicit,
             MaxDeliver = 3, // Fail fast for demo
             FilterSubject = "events.created"
        });
        Console.WriteLine("Consumer 'user-processor' created/updated with MaxDeliver=3.");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Warning creating consumer: {ex.Message}");
    }
    
    // Create DLQ stream too, just in case
    try
    {
        await natsContext.GetStreamAsync("DLQ");
        Console.WriteLine("Stream 'DLQ' verified.");
    }
    catch
    {
        await natsContext.CreateStreamAsync(new StreamConfig("DLQ", ["events.dlq"]));
        Console.WriteLine("Stream 'DLQ' created.");
    }
}
catch (Exception ex)
{
    Console.WriteLine($"Topology setup warning: {ex.Message}");
}

// 3. Start the Host (Background services start consuming)
Console.WriteLine("\n--- Starting Consumer Host ---");
var hostTask = host.RunAsync();

// Give it a moment to start
await Task.Delay(2000);

// 4. Send Messages
Console.WriteLine("\n--- Publishing Messages ---");

// Good Message
var goodId = $"u1-{Guid.NewGuid():N}";
var goodUser = new UserCreatedEvent(goodId, "BruceWayne", "bruce@wayne.com");
await publisher.PublishAsync("events.created", goodUser, goodUser.UserId);
Console.WriteLine($"Published Good User: {goodUser.Username}");

// Poison Message
var poisonId = $"u2-{Guid.NewGuid():N}";
var poisonUser = new UserCreatedEvent(poisonId, "PoisonIvy", "ivy@gotham.com");
await publisher.PublishAsync("events.created", poisonUser, poisonUser.UserId);
Console.WriteLine($"Published Poison User: {poisonUser.Username}");

// 5. Wait for Processing and DLQ
Console.WriteLine("\n--- Waiting for Processing & DLQ (10s) ---");
// Allow time for: Receive -> Fail -> Retry (x3) -> DLQ -> Store Notification
await Task.Delay(10000); 

// 6. Inspect DLQ Store
Console.WriteLine("\n--- Inspecting DLQ Store ---");
var dlqEntries = await dlqStore.ListAsync(limit: 10);

if (dlqEntries.Count == 0)
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.WriteLine("No messages found in DLQ Store!");
    Console.ResetColor();
}
else
{
    Console.ForegroundColor = ConsoleColor.Green;
    Console.WriteLine($"Found {dlqEntries.Count} messages in DLQ Store:");
    Console.ResetColor();

    foreach (var entry in dlqEntries)
    {
        Console.WriteLine($" - ID: {entry.Id}");
        Console.WriteLine($"   Stream/Consumer: {entry.OriginalStream}/{entry.OriginalConsumer}");
        Console.WriteLine($"   Reason: {entry.ErrorReason}");
        Console.WriteLine($"   Stored At: {entry.StoredAt}");
        Console.WriteLine("   -----------------------------");
    }
}

Console.WriteLine("\n--- Stopping Host ---");
await host.StopAsync();


// Helper to register policy since we aren't using the full TopologyManager here
public class PolicyRegistrationService : IHostedService
{
    private readonly IDlqPolicyRegistry _registry;

    public PolicyRegistrationService(IDlqPolicyRegistry registry)
    {
        _registry = registry;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _registry.Register("EVENTS", "user-processor", new DeadLetterPolicy
        {
            SourceStream = "EVENTS",
            SourceConsumer = "user-processor",
            TargetStream = StreamName.From("DLQ"),
            TargetSubject = "events.dlq"
        });
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

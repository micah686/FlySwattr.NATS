using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Extensions;
using FlySwattr.NATS.Topology.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shared;
using Spectre.Console;

/// <summary>
/// NatsSubscriber - A companion application for testing pub/sub, request/reply, and DLQ scenarios.
///
/// This application demonstrates:
/// - Core NATS subscriptions (fire-and-forget)
/// - Request/Reply pattern (responding to queries)
/// - JetStream consumption with acknowledgment
/// - Simulated failures for DLQ testing
///
/// Usage:
///   NatsSubscriber                    - Run with default settings
///   NatsSubscriber --simulate-failures - Run with simulated failures for DLQ testing
///   NatsSubscriber --subject orders.>  - Subscribe to a specific subject pattern
/// </summary>

var cmdArgs = Environment.GetCommandLineArgs().Skip(1).ToArray();
var simulateFailures = cmdArgs.Contains("--simulate-failures") || cmdArgs.Contains("-f");
var subjectArg = GetArgValue(cmdArgs, "--subject", "-s") ?? "orders.>";

// Build the host with FlySwattr.NATS services
var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder(args);

// Configure logging
builder.Logging.ClearProviders();
builder.Logging.AddConsole();
builder.Logging.SetMinimumLevel(LogLevel.Warning);

// Get NATS URL from environment or default
var natsUrl = Environment.GetEnvironmentVariable("NATS_URL") ?? "nats://localhost:4222";

// Add FlySwattr.NATS with the "batteries included" configuration
builder.Services.AddEnterpriseNATSMessaging(opts =>
{
    opts.Core.Url = natsUrl;
    opts.EnableTopologyProvisioning = true;
    opts.EnableResilience = true;
    opts.EnableCaching = true;
    opts.EnablePayloadOffloading = true;
    opts.EnableDistributedLock = true;
});

// Register the sample topology
builder.Services.AddNatsTopologySource<OrdersTopology>();

var host = builder.Build();

// Start the host to connect to NATS and provision topology
await host.StartAsync();

var services = host.Services;
var messageBus = services.GetRequiredService<IMessageBus>();
var jetStreamConsumer = services.GetRequiredService<IJetStreamConsumer>();

// Display header
AnsiConsole.Write(new FigletText("Subscriber").Color(Color.Green));
AnsiConsole.MarkupLine("[grey]FlySwattr.NATS Subscriber Application[/]");
AnsiConsole.MarkupLine($"[grey]Subject pattern: {subjectArg}[/]");
if (simulateFailures)
{
    AnsiConsole.MarkupLine("[red]Failure simulation mode ENABLED[/]");
    AnsiConsole.MarkupLine("[grey]Messages containing 'POISON' will fail processing.[/]");
}
AnsiConsole.WriteLine();

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
    AnsiConsole.MarkupLine("\n[yellow]Shutting down...[/]");
};

var messageCount = 0;
var failedCount = 0;

// Start Core NATS subscription for basic pub/sub
var coreSubTask = messageBus.SubscribeAsync<OrderCreatedEvent>(
    subjectArg,
    async context =>
    {
        Interlocked.Increment(ref messageCount);
        var order = context.Message;

        AnsiConsole.MarkupLine($"[green] (Core) Message #{messageCount}[/]");
        AnsiConsole.MarkupLine($"  Subject: [cyan]{context.Subject}[/]");
        AnsiConsole.MarkupLine($"  Order ID: {order.OrderId}");
        AnsiConsole.MarkupLine($"  Customer: {order.CustomerId}");
        AnsiConsole.MarkupLine($"  Amount: ${order.Amount:F2}");

        // Display headers if present
        if (context.Headers.Headers.Count > 0)
        {
            AnsiConsole.MarkupLine("  Headers:");
            foreach (var header in context.Headers.Headers)
            {
                AnsiConsole.MarkupLine($"    {header.Key}: [grey]{header.Value}[/]");
            }
        }

        await Task.CompletedTask;
    },
    queueGroup: null, // No queue group for broadcast
    cancellationToken: cts.Token);

// Start Request/Reply handler for order queries
var requestReplyTask = messageBus.SubscribeAsync<OrderQuery>(
    "orders.query",
    async context =>
    {
        var query = context.Message;
        AnsiConsole.MarkupLine($"[blue][Request/Reply] Query for order: {query.OrderId}[/]");

        // Simulate looking up the order
        var response = new OrderQueryResponse(
            OrderId: query.OrderId,
            CustomerId: $"CUST-{Random.Shared.Next(1000, 9999)}",
            Amount: Math.Round((decimal)(Random.Shared.NextDouble() * 1000), 2),
            CreatedAt: DateTime.UtcNow.AddDays(-Random.Shared.Next(1, 30)),
            Status: Random.Shared.Next(0, 3) switch
            {
                0 => "Pending",
                1 => "Shipped",
                _ => "Delivered"
            });

        await context.RespondAsync(response);
        AnsiConsole.MarkupLine($"  [green]Responded with order status: {response.Status}[/]");
    },
    queueGroup: "order-query-handlers",
    cancellationToken: cts.Token);

// Start JetStream consumer for durable processing (wrapped to handle missing streams)
var jetStreamTask = StartJetStreamConsumerSafe(
    jetStreamConsumer, simulateFailures, cts.Token,
    () => Interlocked.Increment(ref failedCount));

async Task StartJetStreamConsumerSafe(
    IJetStreamConsumer consumer,
    bool simulateFails,
    CancellationToken ct,
    Action incrementFailedCount)
{
    try
    {
        await consumer.ConsumePullAsync<OrderCreatedEvent>(
            OrdersTopology.OrdersStreamName,
            OrdersTopology.OrderProcessorConsumer,
            async context =>
            {
                var order = context.Message;
                var shouldFail = simulateFails && order.OrderId.Contains("POISON", StringComparison.OrdinalIgnoreCase);

                AnsiConsole.MarkupLine($"[yellow] (JetStream) Processing message (seq: {context.Sequence})[/]");
                AnsiConsole.MarkupLine($"  Order ID: {order.OrderId}");
                AnsiConsole.MarkupLine($"  Delivery: {context.NumDelivered}");
                AnsiConsole.MarkupLine($"  Redelivered: {context.Redelivered}");

                if (shouldFail)
                {
                    incrementFailedCount();
                    AnsiConsole.MarkupLine($"  [red]SIMULATED FAILURE (delivery #{context.NumDelivered})[/]");

                    // If we've exceeded max deliveries, terminate the message
                    if (context.NumDelivered >= 3)
                    {
                        AnsiConsole.MarkupLine($"  [red]Max deliveries reached - terminating message[/]");
                        await context.TermAsync();
                    }
                    else
                    {
                        // Request redelivery with delay
                        AnsiConsole.MarkupLine($"  [yellow]Requesting redelivery with 2s delay[/]");
                        await context.NackAsync(delay: TimeSpan.FromSeconds(2));
                    }
                }
                else
                {
                    // Successful processing
                    AnsiConsole.MarkupLine($"  [green]Processed successfully[/]");
                    await context.AckAsync();
                }
            },
            options: new JetStreamConsumeOptions
            {
                MaxDegreeOfParallelism = 1 // Process one at a time for demo clarity
            },
            cancellationToken: ct);
    }
    catch (NATS.Client.JetStream.NatsJSApiException ex) when (ex.Message.Contains("stream not found"))
    {
        AnsiConsole.MarkupLine($"[yellow]JetStream consumer skipped:[/] Stream '{OrdersTopology.OrdersStreamName}' not found.");
        AnsiConsole.MarkupLine("[grey]Core NATS subscriptions are still active.[/]");
    }
}

// Display status
AnsiConsole.MarkupLine("[green]Subscriptions active. Press Ctrl+C to stop.[/]");
AnsiConsole.WriteLine();

// Wait for all tasks or cancellation
try
{
    await Task.WhenAll(coreSubTask, requestReplyTask, jetStreamTask);
    
    // Keep the application running until cancelled
    await Task.Delay(Timeout.Infinite, cts.Token);
}
catch (OperationCanceledException)
{
    // Expected on cancellation
}

AnsiConsole.WriteLine();
AnsiConsole.MarkupLine($"[grey]Session summary:[/]");
AnsiConsole.MarkupLine($"  Messages received: {messageCount}");
if (simulateFailures)
{
    AnsiConsole.MarkupLine($"  Failures simulated: {failedCount}");
}

await host.StopAsync();

static string? GetArgValue(string[] args, string longName, string shortName)
{
    for (var i = 0; i < args.Length - 1; i++)
    {
        if (args[i] == longName || args[i] == shortName)
        {
            return args[i + 1];
        }
    }
    return null;
}

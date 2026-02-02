using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Extensions;
using FlySwattr.NATS.Topology.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shared;
using Spectre.Console;

/// <summary>
/// NatsWorker - Demonstrates queue groups for load-balanced task processing.
///
/// Queue groups allow multiple workers to share the processing load.
/// Each message is delivered to exactly one worker in the group.
///
/// To test load balancing:
/// 1. Start multiple instances of NatsWorker
/// 2. Use NatsSampler to publish tasks
/// 3. Observe how tasks are distributed across workers
///
/// Usage:
///   NatsWorker                     - Run with auto-generated worker ID
///   NatsWorker --worker-id worker1 - Run with specific worker ID
///   NatsWorker --group my-workers  - Use a specific queue group name
/// </summary>

var cmdArgs = Environment.GetCommandLineArgs().Skip(1).ToArray();
var workerId = GetArgValue(cmdArgs, "--worker-id", "-w") ?? $"worker-{Random.Shared.Next(1000, 9999)}";
var queueGroup = GetArgValue(cmdArgs, "--group", "-g") ?? "task-processors";

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
var jetStreamPublisher = services.GetRequiredService<IJetStreamPublisher>();
var jetStreamConsumer = services.GetRequiredService<IJetStreamConsumer>();

// Display header with unique worker identity
AnsiConsole.Write(new FigletText($"Worker"));
AnsiConsole.MarkupLine($"FlySwattr.NATS Worker Application");
AnsiConsole.MarkupLine($"Worker ID: {workerId}");
AnsiConsole.MarkupLine($"[grey]Queue Group: {queueGroup}[/]");
AnsiConsole.WriteLine();

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
    AnsiConsole.MarkupLine("\n[yellow]Shutting down...[/]");
};

var tasksProcessed = 0;
var publishedCounter = new Counter();

// Start Core NATS subscription with queue group (load-balanced)
var coreQueueTask = messageBus.SubscribeAsync<WorkTask>(
    "tasks.work",
    async context =>
    {
        var task = context.Message;
        Interlocked.Increment(ref tasksProcessed);

        AnsiConsole.MarkupLine($"{workerId}Received task #{tasksProcessed}");
        AnsiConsole.MarkupLine($"  Task ID: [cyan]{task.TaskId}[/]");
        AnsiConsole.MarkupLine($"  Description: {task.Description}");
        AnsiConsole.MarkupLine($"  Priority: {GetPriorityDisplay(task.Priority)}");

        // Simulate processing time based on priority
        var processingTime = task.Priority switch
        {
            1 => 100,  // High priority - fast
            2 => 300,  // Medium priority
            _ => 500   // Low priority - slower
        };

        await Task.Delay(processingTime);
        AnsiConsole.MarkupLine($"  [green]Completed in {processingTime}ms[/]");
    },
    queueGroup: queueGroup,
    cancellationToken: cts.Token);

// Start JetStream consumer with queue group for durable processing
var jsQueueTask = jetStreamConsumer.ConsumePullAsync<WorkTask>(
    OrdersTopology.TasksStreamName,
    OrdersTopology.TaskProcessorConsumer,
    async context =>
    {
        var task = context.Message;

        AnsiConsole.MarkupLine($"({workerId}) JetStream task (seq: {context.Sequence})");
        AnsiConsole.MarkupLine($"  Task ID: [cyan]{task.TaskId}[/]");
        AnsiConsole.MarkupLine($"  Priority: {GetPriorityDisplay(task.Priority)}");

        // Simulate work
        await Task.Delay(200);

        // Acknowledge successful processing
        await context.AckAsync();
        AnsiConsole.MarkupLine($"  [green]Acknowledged[/]");
    },
    options: new JetStreamConsumeOptions
    {
        MaxDegreeOfParallelism = 2 // Process up to 2 tasks concurrently
    },
    cancellationToken: cts.Token);

// Display status
AnsiConsole.MarkupLine("[green]Worker active. Press 'p' to publish tasks, Ctrl+C to stop.[/]");
AnsiConsole.WriteLine();

// Background task to handle keyboard input (only if running interactively)
var inputTask = Task.Run(async () =>
{
    // Check if we're running in an interactive terminal
    var isInteractive = !Console.IsInputRedirected && Environment.UserInteractive;
    if (!isInteractive)
    {
        // Non-interactive mode - just wait for cancellation
        try { await Task.Delay(Timeout.Infinite, cts.Token); }
        catch (OperationCanceledException) { }
        return;
    }

    while (!cts.Token.IsCancellationRequested)
    {
        try
        {
            if (Console.KeyAvailable)
            {
                var key = Console.ReadKey(intercept: true);
                if (key.Key == ConsoleKey.P)
                {
                    // Publish a batch of tasks
                    await PublishTaskBatch(messageBus, jetStreamPublisher, publishedCounter);
                }
            }
        }
        catch (InvalidOperationException)
        {
            // Console input not available - exit the loop
            break;
        }
        await Task.Delay(100);
    }
}, cts.Token);

// Wait for cancellation
try
{
    await Task.WhenAll(coreQueueTask, jsQueueTask, inputTask);
}
catch (OperationCanceledException)
{
    // Expected on cancellation
}

AnsiConsole.WriteLine();
AnsiConsole.MarkupLine($"[grey]Session summary for {workerId}:[/]");
AnsiConsole.MarkupLine($"  Tasks processed: {tasksProcessed}");
AnsiConsole.MarkupLine($"  Tasks published: {publishedCounter.Value}");

await host.StopAsync();

static async Task PublishTaskBatch(IMessageBus messageBus, IJetStreamPublisher publisher, Counter counter)
{
    AnsiConsole.MarkupLine("\n[yellow]Publishing task batch...[/]");

    // Publish 5 tasks via Core NATS
    for (var i = 0; i < 5; i++)
    {
        var task = new WorkTask(
            TaskId: $"TASK-{Guid.NewGuid():N}",
            Description: $"Sample task {i + 1}",
            Priority: Random.Shared.Next(1, 4),
            ScheduledAt: DateTime.UtcNow);

        await messageBus.PublishAsync("tasks.work", task);
        counter.Increment();
    }

    // Publish 3 tasks via JetStream (durable)
    for (var i = 0; i < 3; i++)
    {
        var task = new WorkTask(
            TaskId: $"JS-TASK-{Guid.NewGuid():N}",
            Description: $"Durable task {i + 1}",
            Priority: Random.Shared.Next(1, 4),
            ScheduledAt: DateTime.UtcNow);

        var messageId = $"task-{task.TaskId}-v1";
        await publisher.PublishAsync("tasks.work", task, messageId);
        counter.Increment();
    }

    AnsiConsole.MarkupLine("[green]Published 8 tasks (5 Core + 3 JetStream)[/]\n");
}

static string GetPriorityDisplay(int priority) => priority switch
{
    1 => "[red]HIGH[/]",
    2 => "[yellow]MEDIUM[/]",
    _ => "[grey]LOW[/]"
};



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

class Counter
{
    private int _value;
    public int Value => _value;
    public void Increment() => Interlocked.Increment(ref _value);
}

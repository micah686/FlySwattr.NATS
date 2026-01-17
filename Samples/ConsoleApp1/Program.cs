using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Extensions;
using FlySwattr.NATS.Topology.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NATS.Client.JetStream;
using ConsoleApp1;
using Shared;
using Spectre.Console;

// Build host with Enterprise NATS configuration
var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddEnterpriseNATSMessaging(options =>
{
    options.Core.Url = "nats://localhost:4222";
    options.EnableCaching = true;
    options.EnableResilience = true;
    options.EnableTopologyProvisioning = true;
});

var host = builder.Build();

// Get services
var messageBus = host.Services.GetRequiredService<IMessageBus>();
var topologyManager = host.Services.GetRequiredService<ITopologyManager>();
var kvStoreFactory = host.Services.GetRequiredService<Func<string, IKeyValueStore>>();
var objectStoreFactory = host.Services.GetRequiredService<Func<string, IObjectStore>>();
var jsPublisher = host.Services.GetRequiredService<IJetStreamPublisher>();
var jsConsumer = host.Services.GetRequiredService<IJetStreamConsumer>();
var dlqService = host.Services.GetRequiredService<IDlqRemediationService>();
var jsContext = host.Services.GetRequiredService<INatsJSContext>();

// Constants
const string KvBucket = "samples_kv";
const string ObjectBucket = "samples_objects";

// Initialize infrastructure
AnsiConsole.MarkupLine("[green]Connecting to NATS...[/]");
await AnsiConsole.Status()
    .StartAsync("Initializing infrastructure...", async ctx =>
    {
        ctx.Status("Ensuring KV & Object Store buckets...");
        await topologyManager.EnsureBucketAsync(BucketName.From(KvBucket), StorageType.Memory);
        await topologyManager.EnsureObjectStoreAsync(BucketName.From(ObjectBucket), StorageType.Memory);
        await topologyManager.EnsureBucketAsync(BucketName.From("fs-dlq-entries"), StorageType.File);
        
        ctx.Status("Ensuring Streams exist...");
        await topologyManager.EnsureStreamAsync(new StreamSpec
        {
            Name = StreamName.From("ORDERS_STREAM"),
            Subjects = ["orders.>"],
            StorageType = StorageType.File,
            RetentionPolicy = StreamRetention.Limits
        });
        await topologyManager.EnsureStreamAsync(new StreamSpec
        {
            Name = StreamName.From("DLQ_STREAM"),
            Subjects = ["dlq.>"],
            StorageType = StorageType.File,
            RetentionPolicy = StreamRetention.Limits
        });
    });

// Instantiate Logic Classes
var core = new Core(messageBus);
var store = new Store(kvStoreFactory(KvBucket), objectStoreFactory(ObjectBucket));
var headers = new Headers(messageBus);
var dlq = new DLQ(dlqService, jsPublisher, messageBus);
var jetStream = new JetStream(jsContext, jsPublisher, dlq);

AnsiConsole.MarkupLine("[green]ConsoleApp1 Ready![/]");
AnsiConsole.MarkupLine("[dim]Press Ctrl+C to exit[/]\n");

var running = true;
Console.CancelKeyPress += (_, e) => { e.Cancel = true; running = false; };
Func<bool> isRunning = () => running;

while (running)
{
    var choice = AnsiConsole.Prompt(
        new SelectionPrompt<string>()
            .Title("What would you like to do?")
            .AddChoices(
                "1. Place Order (Pub/Sub)",
                "2. Check Stock (Request/Reply)",
                "3. Ship Orders (Queue Group Demo)",
                "4. KV Store Operations",
                "5. Object Store Operations",
                "6. Place Order with Custom Headers",
                "7. JetStream & DLQ Operations",
                "8. Exit"));

    switch (choice)
    {
        case "1. Place Order (Pub/Sub)": await core.PlaceOrderAsync(); break;
        case "2. Check Stock (Request/Reply)": await core.CheckStockAsync(); break;
        case "3. Ship Orders (Queue Group Demo)": await core.ShipOrdersAsync(); break;
        case "4. KV Store Operations": await store.KeyValueMenuAsync(isRunning); break;
        case "5. Object Store Operations": await store.ObjectStoreMenuAsync(isRunning); break;
        case "6. Place Order with Custom Headers": await headers.PlaceOrderWithHeadersAsync(); break;
        case "7. JetStream & DLQ Operations": await jetStream.JetStreamMenuAsync(isRunning); break;
        case "8. Exit": running = false; break;
    }

    if (choice == "4. Storage Operations (KV & Object)")
    {
        // Re-prompt or just allow switching in sub-menu. 
        // Let's refine choice 4 to show both submenus if the user wants.
        // Actually, the Store class has both menus. I'll let the user choose inside.
    }
}

AnsiConsole.MarkupLine("[yellow]Goodbye![/]");
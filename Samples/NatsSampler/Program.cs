using FlySwattr.NATS.Extensions;
using FlySwattr.NATS.Topology.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NatsSampler;
using Shared;
using Spectre.Console;

// Parse command-line arguments for non-interactive mode
var cmdArgs = Environment.GetCommandLineArgs().Skip(1).ToArray();
var nonInteractive = cmdArgs.Contains("--non-interactive") || cmdArgs.Contains("-n");

// Build the host with FlySwattr.NATS services
var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder(args);

// Configure logging
builder.Logging.ClearProviders();
builder.Logging.AddConsole();
builder.Logging.SetMinimumLevel(LogLevel.Debug);

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

// Get the service provider for dependency injection
var services = host.Services;

// Create operation handlers
var coreOps = new CoreOperations(services);
var jetStreamOps = new JetStreamOperations(services);
var storeOps = new StoreOperations(services);
var dlqOps = new DlqOperations(services);

if (nonInteractive)
{
    // Run specific command from CLI arguments
    await RunCliCommand(cmdArgs, coreOps, jetStreamOps, storeOps, dlqOps);
}
else
{
    // Run interactive menu
    await RunInteractiveMenu(coreOps, jetStreamOps, storeOps, dlqOps);
}

await host.StopAsync();
return;

static async Task RunInteractiveMenu(
    CoreOperations coreOps,
    JetStreamOperations jetStreamOps,
    StoreOperations storeOps,
    DlqOperations dlqOps)
{
    AnsiConsole.Write(new FigletText("NATS Sampler").Color(Color.Cyan1));
    AnsiConsole.MarkupLine("[grey]FlySwattr.NATS Sample Application[/]");
    AnsiConsole.WriteLine();

    while (true)
    {
        var category = AnsiConsole.Prompt(
            new SelectionPrompt<string>()
                .Title("Select a [green]category[/]:")
                .PageSize(10)
                .AddChoices(
                    "Core Operations (Pub/Sub, Request/Reply, Headers)",
                    "JetStream Operations",
                    "KV Store Operations",
                    "Object Store Operations",
                    "DLQ Operations",
                    "Exit"));

        switch (category)
        {
            case "Core Operations (Pub/Sub, Request/Reply, Headers)":
                await RunCoreOperationsMenu(coreOps);
                break;
            case "JetStream Operations":
                await RunJetStreamMenu(jetStreamOps);
                break;
            case "KV Store Operations":
                await RunKvStoreMenu(storeOps);
                break;
            case "Object Store Operations":
                await RunObjectStoreMenu(storeOps);
                break;
            case "DLQ Operations":
                await RunDlqMenu(dlqOps);
                break;
            case "Exit":
                return;
        }
    }
}

static async Task RunCoreOperationsMenu(CoreOperations ops)
{
    while (true)
    {
        var operation = AnsiConsole.Prompt(
            new SelectionPrompt<string>()
                .Title("Select a [green]Core operation[/]:")
                .PageSize(10)
                .AddChoices(
                    "Publish Message",
                    "Publish with Headers",
                    "Request/Reply",
                    "Subscribe (listen for 10 seconds)",
                    "Back"));

        switch (operation)
        {
            case "Publish Message":
                await ops.PublishMessageAsync();
                break;
            case "Publish with Headers":
                await ops.PublishWithHeadersAsync();
                break;
            case "Request/Reply":
                await ops.RequestReplyAsync();
                break;
            case "Subscribe (listen for 10 seconds)":
                await ops.SubscribeAsync();
                break;
            case "Back":
                return;
        }
    }
}

static async Task RunJetStreamMenu(JetStreamOperations ops)
{
    while (true)
    {
        var operation = AnsiConsole.Prompt(
            new SelectionPrompt<string>()
                .Title("Select a [green]JetStream operation[/]:")
                .PageSize(10)
                .AddChoices(
                    "List Streams",
                    "Inspect Stream",
                    "Publish with Message ID (Idempotent)",
                    "Purge Stream",
                    "Delete Stream",
                    "Back"));

        switch (operation)
        {
            case "List Streams":
                await ops.ListStreamsAsync();
                break;
            case "Inspect Stream":
                await ops.InspectStreamAsync();
                break;
            case "Publish with Message ID (Idempotent)":
                await ops.PublishIdempotentAsync();
                break;
            case "Purge Stream":
                await ops.PurgeStreamAsync();
                break;
            case "Delete Stream":
                await ops.DeleteStreamAsync();
                break;
            case "Back":
                return;
        }
    }
}

static async Task RunKvStoreMenu(StoreOperations ops)
{
    while (true)
    {
        var operation = AnsiConsole.Prompt(
            new SelectionPrompt<string>()
                .Title("Select a [green]KV Store operation[/]:")
                .PageSize(10)
                .AddChoices(
                    "Put Value",
                    "Get Value",
                    "Delete Key",
                    "List Keys",
                    "Watch Key (10 seconds)",
                    "Back"));

        switch (operation)
        {
            case "Put Value":
                await ops.KvPutAsync();
                break;
            case "Get Value":
                await ops.KvGetAsync();
                break;
            case "Delete Key":
                await ops.KvDeleteAsync();
                break;
            case "List Keys":
                await ops.KvListKeysAsync();
                break;
            case "Watch Key (10 seconds)":
                await ops.KvWatchAsync();
                break;
            case "Back":
                return;
        }
    }
}

static async Task RunObjectStoreMenu(StoreOperations ops)
{
    while (true)
    {
        var operation = AnsiConsole.Prompt(
            new SelectionPrompt<string>()
                .Title("Select an [green]Object Store operation[/]:")
                .PageSize(10)
                .AddChoices(
                    "Upload File",
                    "Download File",
                    "Get File Info",
                    "List Objects",
                    "Delete Object",
                    "Back"));

        switch (operation)
        {
            case "Upload File":
                await ops.ObjUploadAsync();
                break;
            case "Download File":
                await ops.ObjDownloadAsync();
                break;
            case "Get File Info":
                await ops.ObjInfoAsync();
                break;
            case "List Objects":
                await ops.ObjListAsync();
                break;
            case "Delete Object":
                await ops.ObjDeleteAsync();
                break;
            case "Back":
                return;
        }
    }
}

static async Task RunDlqMenu(DlqOperations ops)
{
    while (true)
    {
        var operation = AnsiConsole.Prompt(
            new SelectionPrompt<string>()
                .Title("Select a [green]DLQ operation[/]:")
                .PageSize(10)
                .AddChoices(
                    "Simulate Poison Message",
                    "List DLQ Entries",
                    "Inspect DLQ Entry",
                    "Replay Message",
                    "Archive Message",
                    "Delete Message",
                    "Back"));

        switch (operation)
        {
            case "Simulate Poison Message":
                await ops.SimulatePoisonMessageAsync();
                break;
            case "List DLQ Entries":
                await ops.ListDlqEntriesAsync();
                break;
            case "Inspect DLQ Entry":
                await ops.InspectDlqEntryAsync();
                break;
            case "Replay Message":
                await ops.ReplayMessageAsync();
                break;
            case "Archive Message":
                await ops.ArchiveMessageAsync();
                break;
            case "Delete Message":
                await ops.DeleteMessageAsync();
                break;
            case "Back":
                return;
        }
    }
}

static async Task RunCliCommand(
    string[] args,
    CoreOperations coreOps,
    JetStreamOperations jetStreamOps,
    StoreOperations storeOps,
    DlqOperations dlqOps)
{
    // Skip the --non-interactive flag
    var commands = args.Where(a => !a.StartsWith("-")).ToArray();

    if (commands.Length == 0)
    {
        AnsiConsole.MarkupLine("[red]No command specified.[/]");
        PrintCliHelp();
        return;
    }

    var command = commands[0].ToLowerInvariant();
    var subCommand = commands.Length > 1 ? commands[1].ToLowerInvariant() : null;
    var param = commands.Length > 2 ? commands[2] : null;

    switch (command)
    {
        case "core":
            await HandleCoreCommand(coreOps, subCommand, param);
            break;
        case "js":
        case "jetstream":
            await HandleJetStreamCommand(jetStreamOps, subCommand, param);
            break;
        case "kv":
            await HandleKvCommand(storeOps, subCommand, param, commands);
            break;
        case "obj":
        case "object":
            await HandleObjCommand(storeOps, subCommand, param, commands);
            break;
        case "dlq":
            await HandleDlqCommand(dlqOps, subCommand, param);
            break;
        case "help":
            PrintCliHelp();
            break;
        default:
            AnsiConsole.MarkupLine($"[red]Unknown command: {command}[/]");
            PrintCliHelp();
            break;
    }
}

static async Task HandleCoreCommand(CoreOperations ops, string? subCommand, string? param)
{
    switch (subCommand)
    {
        case "publish":
            await ops.PublishMessageAsync(param);
            break;
        case "headers":
            await ops.PublishWithHeadersAsync(param);
            break;
        case "request":
            await ops.RequestReplyAsync(param);
            break;
        case "subscribe":
            var duration = int.TryParse(param, out var d) ? d : 10;
            await ops.SubscribeAsync(duration);
            break;
        default:
            AnsiConsole.MarkupLine("[red]Unknown core sub-command. Use: publish, headers, request, subscribe[/]");
            break;
    }
}

static async Task HandleJetStreamCommand(JetStreamOperations ops, string? subCommand, string? param)
{
    switch (subCommand)
    {
        case "list":
            await ops.ListStreamsAsync();
            break;
        case "inspect":
            await ops.InspectStreamAsync(param);
            break;
        case "publish":
            await ops.PublishIdempotentAsync(param);
            break;
        case "purge":
            await ops.PurgeStreamAsync(param);
            break;
        case "delete":
            await ops.DeleteStreamAsync(param);
            break;
        default:
            AnsiConsole.MarkupLine("[red]Unknown jetstream sub-command. Use: list, inspect, publish, purge, delete[/]");
            break;
    }
}

static async Task HandleKvCommand(StoreOperations ops, string? subCommand, string? param, string[] commands)
{
    switch (subCommand)
    {
        case "put":
            var value = commands.Length > 3 ? commands[3] : null;
            await ops.KvPutAsync(param, value);
            break;
        case "get":
            await ops.KvGetAsync(param);
            break;
        case "delete":
            await ops.KvDeleteAsync(param);
            break;
        case "list":
            await ops.KvListKeysAsync(param);
            break;
        case "watch":
            var duration = commands.Length > 3 && int.TryParse(commands[3], out var d) ? d : 10;
            await ops.KvWatchAsync(param, duration);
            break;
        default:
            AnsiConsole.MarkupLine("[red]Unknown kv sub-command. Use: put, get, delete, list, watch[/]");
            break;
    }
}

static async Task HandleObjCommand(StoreOperations ops, string? subCommand, string? param, string[] commands)
{
    switch (subCommand)
    {
        case "upload":
            var targetKey = commands.Length > 3 ? commands[3] : null;
            await ops.ObjUploadAsync(param, targetKey);
            break;
        case "download":
            var targetPath = commands.Length > 3 ? commands[3] : null;
            await ops.ObjDownloadAsync(param, targetPath);
            break;
        case "info":
            await ops.ObjInfoAsync(param);
            break;
        case "list":
            await ops.ObjListAsync();
            break;
        case "delete":
            await ops.ObjDeleteAsync(param);
            break;
        default:
            AnsiConsole.MarkupLine("[red]Unknown obj sub-command. Use: upload, download, info, list, delete[/]");
            break;
    }
}

static async Task HandleDlqCommand(DlqOperations ops, string? subCommand, string? param)
{
    switch (subCommand)
    {
        case "poison":
            await ops.SimulatePoisonMessageAsync(param);
            break;
        case "list":
            await ops.ListDlqEntriesAsync();
            break;
        case "inspect":
            await ops.InspectDlqEntryAsync(param);
            break;
        case "replay":
            await ops.ReplayMessageAsync(param);
            break;
        case "archive":
            await ops.ArchiveMessageAsync(param);
            break;
        case "delete":
            await ops.DeleteMessageAsync(param);
            break;
        default:
            AnsiConsole.MarkupLine("[red]Unknown dlq sub-command. Use: poison, list, inspect, replay, archive, delete[/]");
            break;
    }
}

static void PrintCliHelp()
{
    AnsiConsole.MarkupLine(@"
[bold]FlySwattr.NATS Sampler CLI[/]

[yellow]Usage:[/] NatsSampler [options] <command> <sub-command> [params]

[yellow]Options:[/]
  -n, --non-interactive    Run in non-interactive CLI mode

[yellow]Commands:[/]
  [green]core[/]      Core NATS operations
    publish [message]       Publish a message
    headers [message]       Publish with custom headers
    request [orderId]       Send request/reply
    subscribe [seconds]     Subscribe for N seconds

  [green]js/jetstream[/]  JetStream operations
    list                    List all streams
    inspect [stream]        Inspect stream details
    publish [orderId]       Publish with idempotent message ID
    purge [stream]          Purge stream messages
    delete [stream]         Delete a stream

  [green]kv[/]        Key-Value Store operations
    put <key> <value>       Put a value
    get <key>               Get a value
    delete <key>            Delete a key
    list [pattern]          List keys matching pattern
    watch <key> [seconds]   Watch key for changes

  [green]obj/object[/]    Object Store operations
    upload <file> [key]     Upload a file
    download <key> [file]   Download an object
    info <key>              Get object info
    list                    List all objects
    delete <key>            Delete an object

  [green]dlq[/]       Dead Letter Queue operations
    poison [reason]         Simulate a poison message
    list                    List DLQ entries
    inspect <id>            Inspect a DLQ entry
    replay <id>             Replay a message
    archive <id>            Archive a message
    delete <id>             Delete a message

[yellow]Examples:[/]
  NatsSampler -n core publish ""Hello World""
  NatsSampler -n js list
  NatsSampler -n kv put config.app.theme dark
  NatsSampler -n dlq list
");
}

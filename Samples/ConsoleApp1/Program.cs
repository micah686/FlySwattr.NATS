using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Extensions;
using FlySwattr.NATS.Topology.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
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

// Register Orders topology for auto-provisioning
builder.Services.AddNatsTopologySource<OrdersTopology>();

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

// Initialize buckets
AnsiConsole.MarkupLine("[green]Connecting to NATS...[/]");
await AnsiConsole.Status()
    .StartAsync("Initializing buckets...", async ctx =>
    {
        ctx.Status("Creating KV bucket...");
        await topologyManager.EnsureBucketAsync(BucketName.From(KvBucket), StorageType.Memory);
        
        ctx.Status("Creating Object Store bucket...");
        await topologyManager.EnsureObjectStoreAsync(BucketName.From(ObjectBucket), StorageType.Memory);
        
        ctx.Status("Creating DLQ entries bucket...");
        await topologyManager.EnsureBucketAsync(BucketName.From("fs-dlq-entries"), StorageType.File);
        
        ctx.Status("Ensuring ORDERS_STREAM exists...");
        await topologyManager.EnsureStreamAsync(new StreamSpec
        {
            Name = StreamName.From("ORDERS_STREAM"),
            Subjects = ["orders.>"],
            StorageType = StorageType.File,
            RetentionPolicy = StreamRetention.Limits
        });
        
        ctx.Status("Ensuring DLQ_STREAM exists...");
        await topologyManager.EnsureStreamAsync(new StreamSpec
        {
            Name = StreamName.From("DLQ_STREAM"),
            Subjects = ["dlq.>"],
            StorageType = StorageType.File,
            RetentionPolicy = StreamRetention.Limits
        });
    });

// Get store instances
var kvStore = kvStoreFactory(KvBucket);
var objectStore = objectStoreFactory(ObjectBucket);

AnsiConsole.MarkupLine("[green]ConsoleApp1 (Storefront) Ready![/]");
AnsiConsole.MarkupLine("[dim]Press Ctrl+C to exit[/]\n");

// Run interactive menu loop
var running = true;
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    running = false;
};

while (running)
{
    var choice = AnsiConsole.Prompt(
        new SelectionPrompt<string>()
            .Title("What would you like to do?")
            .AddChoices(
                "1. Place Order (Pub/Sub)",
                "2. Check Stock (Request/Reply)",
                "3. Ship Orders (Queue Group Demo)",
                "4. Storage: Key-Value Store",
                "5. Storage: Object Store",
                "6. Place Order with Headers",
                "7. JetStream Operations",
                "8. Exit"));

    switch (choice)
    {
        case "1. Place Order (Pub/Sub)":
            await PlaceOrderAsync();
            break;
        case "2. Check Stock (Request/Reply)":
            await CheckStockAsync();
            break;
        case "3. Ship Orders (Queue Group Demo)":
            await ShipOrdersAsync();
            break;
        case "4. Storage: Key-Value Store":
            await KeyValueMenuAsync();
            break;
        case "5. Storage: Object Store":
            await ObjectStoreMenuAsync();
            break;
        case "6. Place Order with Headers":
            await PlaceOrderWithHeadersAsync();
            break;
        case "7. JetStream Operations":
            await JetStreamMenuAsync();
            break;
        case "8. Exit":
            running = false;
            break;
    }
}

AnsiConsole.MarkupLine("[yellow]Goodbye![/]");

// ─────────────────────────────────────────────────────────────────────────────
// Original Menu Actions
// ─────────────────────────────────────────────────────────────────────────────

async Task PlaceOrderAsync()
{
    var orderId = Guid.NewGuid();
    var sku = $"SKU-{Random.Shared.Next(100, 999)}";
    
    var orderEvent = new OrderPlacedEvent(orderId, sku);
    
    await messageBus.PublishAsync("orders.placed", orderEvent);
    
    AnsiConsole.MarkupLine($"[green]✓ Published OrderPlacedEvent[/]");
    AnsiConsole.MarkupLine($"  OrderId: [cyan]{orderId}[/]");
    AnsiConsole.MarkupLine($"  SKU: [cyan]{sku}[/]\n");
}

async Task CheckStockAsync()
{
    var sku = AnsiConsole.Ask<string>("Enter SKU to check:");
    
    AnsiConsole.MarkupLine($"[yellow]Checking stock for {sku}...[/]");
    
    var request = new CheckStockRequest(sku);
    var response = await messageBus.RequestAsync<CheckStockRequest, StockStatusResponse>(
        "stock.check", request, TimeSpan.FromSeconds(5));
    
    if (response is not null)
    {
        var statusColor = response.InStock ? "green" : "red";
        AnsiConsole.MarkupLine($"[{statusColor}]Stock Status for {response.Sku}:[/]");
        AnsiConsole.MarkupLine($"  In Stock: [{statusColor}]{response.InStock}[/]");
        AnsiConsole.MarkupLine($"  Quantity: [cyan]{response.Quantity}[/]\n");
    }
    else
    {
        AnsiConsole.MarkupLine("[red]No response received (timeout)[/]\n");
    }
}

async Task ShipOrdersAsync()
{
    AnsiConsole.MarkupLine("[yellow]Sending 10 ship commands to demonstrate Queue Group load balancing...[/]");
    
    await AnsiConsole.Progress()
        .StartAsync(async ctx =>
        {
            var task = ctx.AddTask("Sending ship commands", maxValue: 10);
            
            for (int i = 0; i < 10; i++)
            {
                var command = new ShipOrderCommand(Guid.NewGuid());
                await messageBus.PublishAsync("orders.ship", command);
                task.Increment(1);
                await Task.Delay(100);
            }
        });
    
    AnsiConsole.MarkupLine("[green]✓ All 10 ship commands sent![/]");
    AnsiConsole.MarkupLine("[dim]Check App2 and App3 consoles to see load balancing in action.[/]\n");
}

// ─────────────────────────────────────────────────────────────────────────────
// Key-Value Store Menu
// ─────────────────────────────────────────────────────────────────────────────

async Task KeyValueMenuAsync()
{
    var inKvMenu = true;
    while (inKvMenu && running)
    {
        var choice = AnsiConsole.Prompt(
            new SelectionPrompt<string>()
                .Title("[blue]Key-Value Store[/] - Select an operation:")
                .AddChoices(
                    "1. Put Value",
                    "2. Get Value",
                    "3. Delete Key",
                    "4. List Keys",
                    "5. Watch Changes",
                    "← Back to Main Menu"));

        switch (choice)
        {
            case "1. Put Value":
                await KvPutAsync();
                break;
            case "2. Get Value":
                await KvGetAsync();
                break;
            case "3. Delete Key":
                await KvDeleteAsync();
                break;
            case "4. List Keys":
                await KvListKeysAsync();
                break;
            case "5. Watch Changes":
                await KvWatchAsync();
                break;
            case "← Back to Main Menu":
                inKvMenu = false;
                break;
        }
    }
}

async Task KvPutAsync()
{
    var key = AnsiConsole.Ask<string>("Enter [cyan]key[/]:");
    var value = AnsiConsole.Ask<string>("Enter [cyan]value[/]:");
    
    await kvStore.PutAsync(key, value);
    AnsiConsole.MarkupLine($"[green]✓ Stored[/] [cyan]{key}[/] = {value}\n");
}

async Task KvGetAsync()
{
    var key = AnsiConsole.Ask<string>("Enter [cyan]key[/] to retrieve:");
    
    var value = await kvStore.GetAsync<string>(key);
    if (value is not null)
    {
        AnsiConsole.MarkupLine($"[green]✓ Value:[/] {value}\n");
    }
    else
    {
        AnsiConsole.MarkupLine($"[yellow]Key not found: {key}[/]\n");
    }
}

async Task KvDeleteAsync()
{
    var key = AnsiConsole.Ask<string>("Enter [cyan]key[/] to delete:");
    
    await kvStore.DeleteAsync(key);
    AnsiConsole.MarkupLine($"[green]✓ Deleted key:[/] {key}\n");
}

async Task KvListKeysAsync()
{
    AnsiConsole.MarkupLine("[yellow]Listing all keys...[/]");
    
    var table = new Table()
        .Border(TableBorder.Rounded)
        .AddColumn("Key");

    var hasKeys = false;
    await foreach (var key in kvStore.GetKeysAsync([">"]))
    {
        table.AddRow(key);
        hasKeys = true;
    }
    
    if (hasKeys)
    {
        AnsiConsole.Write(table);
    }
    else
    {
        AnsiConsole.MarkupLine("[dim]No keys found in bucket.[/]");
    }
    Console.WriteLine();
}

async Task KvWatchAsync()
{
    AnsiConsole.MarkupLine("[yellow]Watching for changes (press any key to stop)...[/]\n");
    
    using var cts = new CancellationTokenSource();
    
    var watchTask = kvStore.WatchAsync<string>(">", async change =>
    {
        var operation = change.IsDelete ? "[red]DELETE[/]" : "[green]PUT[/]";
        AnsiConsole.MarkupLine($"  {operation} [cyan]{change.Key}[/] = {change.Value ?? "(deleted)"}");
    }, cts.Token);
    
    // Wait for key press
    await Task.Run(() => Console.ReadKey(true));
    cts.Cancel();
    
    try { await watchTask; } catch (OperationCanceledException) { }
    AnsiConsole.MarkupLine("[dim]Watch stopped.[/]\n");
}

// ─────────────────────────────────────────────────────────────────────────────
// Object Store Menu
// ─────────────────────────────────────────────────────────────────────────────

async Task ObjectStoreMenuAsync()
{
    var inObjMenu = true;
    while (inObjMenu && running)
    {
        var choice = AnsiConsole.Prompt(
            new SelectionPrompt<string>()
                .Title("[blue]Object Store[/] - Select an operation:")
                .AddChoices(
                    "1. Upload Object",
                    "2. Download/View Info",
                    "3. List Objects",
                    "4. Delete Object",
                    "← Back to Main Menu"));

        switch (choice)
        {
            case "1. Upload Object":
                await ObjUploadAsync();
                break;
            case "2. Download/View Info":
                await ObjDownloadAsync();
                break;
            case "3. List Objects":
                await ObjListAsync();
                break;
            case "4. Delete Object":
                await ObjDeleteAsync();
                break;
            case "← Back to Main Menu":
                inObjMenu = false;
                break;
        }
    }
}

async Task ObjUploadAsync()
{
    var name = AnsiConsole.Ask<string>("Enter [cyan]object name[/]:");
    var content = AnsiConsole.Ask<string>("Enter [cyan]content[/] (or press Enter for sample text):", "");
    
    if (string.IsNullOrEmpty(content))
    {
        content = $"Sample content generated at {DateTime.UtcNow:O}\n" +
                  string.Join("\n", Enumerable.Range(1, 10).Select(i => $"Line {i}: Lorem ipsum dolor sit amet"));
    }
    
    using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content));
    var digest = await objectStore.PutAsync(name, stream);
    
    AnsiConsole.MarkupLine($"[green]✓ Uploaded[/] [cyan]{name}[/]");
    AnsiConsole.MarkupLine($"  Size: {content.Length} bytes");
    AnsiConsole.MarkupLine($"  Digest: {digest}\n");
}

async Task ObjDownloadAsync()
{
    var name = AnsiConsole.Ask<string>("Enter [cyan]object name[/]:");
    
    var info = await objectStore.GetInfoAsync(name);
    if (info is null)
    {
        AnsiConsole.MarkupLine($"[red]Object not found: {name}[/]\n");
        return;
    }
    
    var table = new Table()
        .Border(TableBorder.Rounded)
        .AddColumn("Property")
        .AddColumn("Value");
    
    table.AddRow("Name", info.Key);
    table.AddRow("Size", $"{info.Size} bytes");
    table.AddRow("Last Modified", info.LastModified.ToString("u"));
    table.AddRow("Digest", info.Digest);
    
    AnsiConsole.Write(table);
    
    if (AnsiConsole.Confirm("Download and display content?", false))
    {
        var tempFile = Path.GetTempFileName();
        try
        {
            await using (var fileStream = File.Create(tempFile))
            {
                await objectStore.GetAsync(name, fileStream);
            }
            
            // Read preview (first 4KB max for console display)
            const int previewSize = 4096;
            var fileInfo = new FileInfo(tempFile);
            var isTruncated = fileInfo.Length > previewSize;
            
            await using var readStream = File.OpenRead(tempFile);
            var buffer = new byte[Math.Min(previewSize, fileInfo.Length)];
            _ = await readStream.ReadAsync(buffer);
            var content = System.Text.Encoding.UTF8.GetString(buffer);
            
            if (isTruncated)
            {
                content += $"\n\n[dim]... truncated ({fileInfo.Length:N0} bytes total)[/]";
            }
            
            AnsiConsole.Write(new Panel(content)
                .Header($"[cyan]{name}[/]")
                .Border(BoxBorder.Rounded));
        }
        finally
        {
            File.Delete(tempFile);
        }
    }
    Console.WriteLine();
}

async Task ObjListAsync()
{
    AnsiConsole.MarkupLine("[yellow]Listing all objects...[/]");
    
    var objects = await objectStore.ListAsync();
    
    var table = new Table()
        .Border(TableBorder.Rounded)
        .AddColumn("Name")
        .AddColumn("Size")
        .AddColumn("Last Modified");

    foreach (var obj in objects)
    {
        table.AddRow(obj.Key, $"{obj.Size} bytes", obj.LastModified.ToString("u"));
    }
    
    if (objects.Any())
    {
        AnsiConsole.Write(table);
    }
    else
    {
        AnsiConsole.MarkupLine("[dim]No objects found in bucket.[/]");
    }
    Console.WriteLine();
}

async Task ObjDeleteAsync()
{
    var name = AnsiConsole.Ask<string>("Enter [cyan]object name[/] to delete:");
    
    await objectStore.DeleteAsync(name);
    AnsiConsole.MarkupLine($"[green]✓ Deleted object:[/] {name}\n");
}

// ─────────────────────────────────────────────────────────────────────────────
// Headers Feature
// ─────────────────────────────────────────────────────────────────────────────

async Task PlaceOrderWithHeadersAsync()
{
    var orderId = Guid.NewGuid();
    var sku = $"SKU-{Random.Shared.Next(100, 999)}";
    
    AnsiConsole.MarkupLine("[yellow]Add custom headers (enter empty key to finish):[/]");
    
    var headers = new Dictionary<string, string>();
    while (true)
    {
        var key = AnsiConsole.Ask<string>("  Header key (empty to finish):", "");
        if (string.IsNullOrEmpty(key)) break;
        
        var value = AnsiConsole.Ask<string>($"  Value for [cyan]{key}[/]:");
        headers[key] = value;
    }
    
    var orderEvent = new OrderPlacedEvent(orderId, sku);
    var messageHeaders = new MessageHeaders(headers);
    
    await messageBus.PublishAsync("orders.placed", orderEvent, messageHeaders);
    
    AnsiConsole.MarkupLine($"[green]✓ Published OrderPlacedEvent with {headers.Count} headers[/]");
    AnsiConsole.MarkupLine($"  OrderId: [cyan]{orderId}[/]");
    AnsiConsole.MarkupLine($"  SKU: [cyan]{sku}[/]");
    
    if (headers.Count > 0)
    {
        var table = new Table()
            .Border(TableBorder.Rounded)
            .AddColumn("Header")
            .AddColumn("Value");
        foreach (var h in headers)
        {
            table.AddRow($"[cyan]{h.Key}[/]", h.Value);
        }
        AnsiConsole.Write(table);
    }
    Console.WriteLine();
}

// ─────────────────────────────────────────────────────────────────────────────
// JetStream Operations Menu
// ─────────────────────────────────────────────────────────────────────────────

async Task JetStreamMenuAsync()
{
    var inJsMenu = true;
    while (inJsMenu && running)
    {
        var choice = AnsiConsole.Prompt(
            new SelectionPrompt<string>()
                .Title("[blue]JetStream Operations[/] - Select an operation:")
                .AddChoices(
                    "1. Infrastructure: List Streams",
                    "2. Infrastructure: Inspect Stream",
                    "3. Infrastructure: Purge Stream",
                    "4. Publish with Message ID (Idempotent)",
                    "5. DLQ Manager",
                    "← Back to Main Menu"));

        switch (choice)
        {
            case "1. Infrastructure: List Streams":
                await JsListStreamsAsync();
                break;
            case "2. Infrastructure: Inspect Stream":
                await JsInspectStreamAsync();
                break;
            case "3. Infrastructure: Purge Stream":
                await JsPurgeStreamAsync();
                break;
            case "4. Publish with Message ID (Idempotent)":
                await JsPublishWithIdAsync();
                break;
            case "5. DLQ Manager":
                await DlqManagerMenuAsync();
                break;
            case "← Back to Main Menu":
                inJsMenu = false;
                break;
        }
    }
}

async Task JsListStreamsAsync()
{
    AnsiConsole.MarkupLine("[yellow]Listing all JetStream streams...[/]");
    
    var table = new Table()
        .Border(TableBorder.Rounded)
        .AddColumn("Stream Name")
        .AddColumn("Messages")
        .AddColumn("Bytes")
        .AddColumn("Subjects");

    var hasStreams = false;
    await foreach (var streamName in jsContext.ListStreamNamesAsync())
    {
        hasStreams = true;
        try
        {
            var stream = await jsContext.GetStreamAsync(streamName);
            var info = stream.Info;
            var subjects = string.Join(", ", info.Config.Subjects ?? []);
            table.AddRow(
                $"[cyan]{streamName}[/]",
                info.State.Messages.ToString("N0"),
                $"{info.State.Bytes:N0}",
                subjects);
        }
        catch
        {
            table.AddRow($"[cyan]{streamName}[/]", "[dim]?[/]", "[dim]?[/]", "[dim]?[/]");
        }
    }
    
    if (hasStreams)
    {
        AnsiConsole.Write(table);
    }
    else
    {
        AnsiConsole.MarkupLine("[dim]No streams found.[/]");
    }
    Console.WriteLine();
}

async Task JsInspectStreamAsync()
{
    var streamName = AnsiConsole.Ask<string>("Enter [cyan]stream name[/] (e.g., ORDERS_STREAM):", "ORDERS_STREAM");
    
    try
    {
        var stream = await jsContext.GetStreamAsync(streamName);
        var info = stream.Info;
        
        AnsiConsole.Write(new Rule($"[cyan]{streamName}[/] Configuration").LeftJustified());
        
        var configTable = new Table()
            .Border(TableBorder.Rounded)
            .AddColumn("Property")
            .AddColumn("Value");
        
        configTable.AddRow("Subjects", string.Join(", ", info.Config.Subjects ?? []));
        configTable.AddRow("Storage", info.Config.Storage.ToString());
        configTable.AddRow("Retention", info.Config.Retention.ToString());
        configTable.AddRow("Replicas", info.Config.NumReplicas.ToString());
        configTable.AddRow("Max Age", info.Config.MaxAge == TimeSpan.Zero ? "Unlimited" : info.Config.MaxAge.ToString());
        configTable.AddRow("Max Bytes", info.Config.MaxBytes == 0 ? "Unlimited" : info.Config.MaxBytes.ToString("N0"));
        
        AnsiConsole.Write(configTable);
        
        AnsiConsole.Write(new Rule($"[cyan]{streamName}[/] State").LeftJustified());
        
        var stateTable = new Table()
            .Border(TableBorder.Rounded)
            .AddColumn("Property")
            .AddColumn("Value");
        
        stateTable.AddRow("Messages", info.State.Messages.ToString("N0"));
        stateTable.AddRow("Bytes", info.State.Bytes.ToString("N0"));
        stateTable.AddRow("First Sequence", info.State.FirstSeq.ToString("N0"));
        stateTable.AddRow("Last Sequence", info.State.LastSeq.ToString("N0"));
        stateTable.AddRow("Consumer Count", info.State.ConsumerCount.ToString());
        
        AnsiConsole.Write(stateTable);
    }
    catch (Exception ex)
    {
        AnsiConsole.MarkupLine($"[red]Error inspecting stream: {ex.Message}[/]");
    }
    Console.WriteLine();
}

async Task JsPurgeStreamAsync()
{
    var streamName = AnsiConsole.Ask<string>("Enter [cyan]stream name[/] to purge:", "ORDERS_STREAM");
    
    if (!AnsiConsole.Confirm($"[red]Are you sure you want to purge ALL messages from {streamName}?[/]", false))
    {
        AnsiConsole.MarkupLine("[dim]Purge cancelled.[/]\n");
        return;
    }
    
    try
    {
        var stream = await jsContext.GetStreamAsync(streamName);
        var response = await stream.PurgeAsync(new StreamPurgeRequest());
        
        AnsiConsole.MarkupLine($"[green]✓ Purged {response.Purged} messages from {streamName}[/]\n");
    }
    catch (Exception ex)
    {
        AnsiConsole.MarkupLine($"[red]Error purging stream: {ex.Message}[/]\n");
    }
}

async Task JsPublishWithIdAsync()
{
    var orderId = Guid.NewGuid();
    var sku = $"SKU-{Random.Shared.Next(100, 999)}";
    
    var defaultMsgId = $"order-{orderId}";
    var messageId = AnsiConsole.Ask<string>($"Enter [cyan]message ID[/] for deduplication:", defaultMsgId);
    
    var orderEvent = new OrderPlacedEvent(orderId, sku);
    
    await jsPublisher.PublishAsync("orders.placed", orderEvent, messageId);
    
    AnsiConsole.MarkupLine($"[green]✓ Published to JetStream with Message ID[/]");
    AnsiConsole.MarkupLine($"  MessageId: [yellow]{messageId}[/]");
    AnsiConsole.MarkupLine($"  OrderId: [cyan]{orderId}[/]");
    AnsiConsole.MarkupLine($"  SKU: [cyan]{sku}[/]");
    
    if (AnsiConsole.Confirm("Publish again with [yellow]same Message ID[/] to test deduplication?", true))
    {
        var newOrder = new OrderPlacedEvent(Guid.NewGuid(), $"SKU-{Random.Shared.Next(100, 999)}");
        try
        {
            await jsPublisher.PublishAsync("orders.placed", newOrder, messageId);
            AnsiConsole.MarkupLine($"[yellow]Published duplicate with same Message ID: {messageId}[/]");
            AnsiConsole.MarkupLine("[dim]Check stream message count - it should NOT have increased due to deduplication![/]");
        }
        catch (NATS.Client.JetStream.NatsJSDuplicateMessageException)
        {
            AnsiConsole.MarkupLine($"[green]✓ DEDUPLICATION CONFIRMED![/] NATS rejected the duplicate message.");
            AnsiConsole.MarkupLine($"  [dim]Message ID {messageId} was already stored in the stream.[/]");
        }
    }
    Console.WriteLine();
}

// ─────────────────────────────────────────────────────────────────────────────
// DLQ Manager Menu
// ─────────────────────────────────────────────────────────────────────────────

async Task DlqManagerMenuAsync()
{
    var inDlqMenu = true;
    while (inDlqMenu && running)
    {
        var choice = AnsiConsole.Prompt(
            new SelectionPrompt<string>()
                .Title("[red]DLQ Manager[/] - Select an operation:")
                .AddChoices(
                    "1. Simulate Poison Message",
                    "2. List DLQ Entries",
                    "3. Replay Message",
                    "4. Archive Message",
                    "5. Delete Message",
                    "← Back to JetStream Menu"));

        switch (choice)
        {
            case "1. Simulate Poison Message":
                await DlqSimulatePoisonAsync();
                break;
            case "2. List DLQ Entries":
                await DlqListAsync();
                break;
            case "3. Replay Message":
                await DlqReplayAsync();
                break;
            case "4. Archive Message":
                await DlqArchiveAsync();
                break;
            case "5. Delete Message":
                await DlqDeleteAsync();
                break;
            case "← Back to JetStream Menu":
                inDlqMenu = false;
                break;
        }
    }
}

async Task DlqSimulatePoisonAsync()
{
    var orderId = Guid.NewGuid();
    var sku = "POISON-SKU";
    
    var orderEvent = new OrderPlacedEvent(orderId, sku);
    var headers = new MessageHeaders(new Dictionary<string, string>
    {
        ["x-poison"] = "true",
        ["x-simulate-failure"] = "validation-error"
    });
    
    await jsPublisher.PublishAsync("orders.placed", orderEvent, $"poison-{orderId}");
    await messageBus.PublishAsync("orders.placed", orderEvent, headers);
    
    AnsiConsole.MarkupLine($"[red]⚠ Published POISON message[/]");
    AnsiConsole.MarkupLine($"  OrderId: [cyan]{orderId}[/]");
    AnsiConsole.MarkupLine($"  SKU: [yellow]{sku}[/]");
    AnsiConsole.MarkupLine($"  Headers: [dim]x-poison=true, x-simulate-failure=validation-error[/]");
    AnsiConsole.MarkupLine("[dim]This message will fail validation in ConsoleApp2 and be routed to DLQ after max retries.[/]\n");
}

async Task DlqListAsync()
{
    AnsiConsole.MarkupLine("[yellow]Fetching DLQ entries...[/]");
    
    try
    {
        var entries = await dlqService.ListAsync(limit: 50);
        
        if (entries.Count == 0)
        {
            AnsiConsole.MarkupLine("[dim]No entries in DLQ.[/]\n");
            return;
        }
        
        var table = new Table()
            .Border(TableBorder.Rounded)
            .AddColumn("ID")
            .AddColumn("Stream")
            .AddColumn("Consumer")
            .AddColumn("Subject")
            .AddColumn("Deliveries")
            .AddColumn("Status")
            .AddColumn("Stored At");

        foreach (var entry in entries)
        {
            var statusColor = entry.Status switch
            {
                DlqMessageStatus.Pending => "yellow",
                DlqMessageStatus.Processing => "blue",
                DlqMessageStatus.Resolved => "green",
                DlqMessageStatus.Archived => "dim",
                _ => "white"
            };
            
            table.AddRow(
                $"[cyan]{entry.Id[..Math.Min(12, entry.Id.Length)]}...[/]",
                entry.OriginalStream,
                entry.OriginalConsumer,
                entry.OriginalSubject,
                entry.DeliveryCount.ToString(),
                $"[{statusColor}]{entry.Status}[/]",
                entry.StoredAt.ToString("g"));
        }
        
        AnsiConsole.Write(table);
        AnsiConsole.MarkupLine($"[dim]Total: {entries.Count} entries[/]\n");
    }
    catch (Exception ex)
    {
        AnsiConsole.MarkupLine($"[red]Error listing DLQ: {ex.Message}[/]\n");
    }
}

async Task DlqReplayAsync()
{
    var id = AnsiConsole.Ask<string>("Enter [cyan]DLQ entry ID[/] to replay:");
    
    try
    {
        var result = await dlqService.ReplayAsync(id);
        
        if (result.Success)
        {
            AnsiConsole.MarkupLine($"[green]✓ Successfully replayed message {id}[/]\n");
        }
        else
        {
            AnsiConsole.MarkupLine($"[red]✗ Replay failed: {result.ErrorMessage}[/]\n");
        }
    }
    catch (Exception ex)
    {
        AnsiConsole.MarkupLine($"[red]Error replaying message: {ex.Message}[/]\n");
    }
}

async Task DlqArchiveAsync()
{
    var id = AnsiConsole.Ask<string>("Enter [cyan]DLQ entry ID[/] to archive:");
    var reason = AnsiConsole.Ask<string>("Enter [cyan]archive reason[/] (optional):", "");
    
    try
    {
        var result = await dlqService.ArchiveAsync(id, string.IsNullOrEmpty(reason) ? null : reason);
        
        if (result.Success)
        {
            AnsiConsole.MarkupLine($"[green]✓ Successfully archived message {id}[/]\n");
        }
        else
        {
            AnsiConsole.MarkupLine($"[red]✗ Archive failed: {result.ErrorMessage}[/]\n");
        }
    }
    catch (Exception ex)
    {
        AnsiConsole.MarkupLine($"[red]Error archiving message: {ex.Message}[/]\n");
    }
}

async Task DlqDeleteAsync()
{
    var id = AnsiConsole.Ask<string>("Enter [cyan]DLQ entry ID[/] to delete:");
    
    if (!AnsiConsole.Confirm($"[red]Are you sure you want to permanently delete {id}?[/]", false))
    {
        AnsiConsole.MarkupLine("[dim]Delete cancelled.[/]\n");
        return;
    }
    
    try
    {
        var result = await dlqService.DeleteAsync(id);
        
        if (result.Success)
        {
            AnsiConsole.MarkupLine($"[green]✓ Successfully deleted message {id}[/]\n");
        }
        else
        {
            AnsiConsole.MarkupLine($"[red]✗ Delete failed: {result.ErrorMessage}[/]\n");
        }
    }
    catch (Exception ex)
    {
        AnsiConsole.MarkupLine($"[red]Error deleting message: {ex.Message}[/]\n");
    }
}
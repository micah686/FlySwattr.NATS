using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shared;
using Spectre.Console;

// Build host with Enterprise NATS configuration
var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddEnterpriseNATSMessaging(options =>
{
    options.Core.Url = "nats://localhost:4222";
    options.EnableCaching = true;
    options.EnableResilience = true;
});

var host = builder.Build();

// Get services
var messageBus = host.Services.GetRequiredService<IMessageBus>();
var topologyManager = host.Services.GetRequiredService<ITopologyManager>();
var kvStoreFactory = host.Services.GetRequiredService<Func<string, IKeyValueStore>>();
var objectStoreFactory = host.Services.GetRequiredService<Func<string, IObjectStore>>();

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
                "7. Exit"));

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
        case "7. Exit":
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
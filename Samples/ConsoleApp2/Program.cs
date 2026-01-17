using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Extensions;
using FlySwattr.NATS.Topology.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shared;
using Spectre.Console;

AnsiConsole.MarkupLine("[cyan]ConsoleApp2 (Inventory Service) Starting...[/]");

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
var messageBus = host.Services.GetRequiredService<IMessageBus>();
var jsConsumer = host.Services.GetRequiredService<IJetStreamConsumer>();
var random = new Random();

AnsiConsole.MarkupLine("[green]Connected to NATS. Setting up subscriptions...[/]\n");

// ─────────────────────────────────────────────────────────────────────────────
// Core NATS Subscriptions (existing functionality)
// ─────────────────────────────────────────────────────────────────────────────

// 1. Subscribe to OrderPlacedEvent (Pub/Sub) - Now with header display
_ = messageBus.SubscribeAsync<OrderPlacedEvent>("orders.placed", async ctx =>
{
    var rule = new Rule("[green]OrderPlacedEvent Received[/]").LeftJustified();
    AnsiConsole.Write(rule);
    
    // Display headers if present
    if (ctx.Headers.Headers.Count > 0)
    {
        var headerTable = new Table()
            .Border(TableBorder.Rounded)
            .BorderColor(Color.Magenta1)
            .AddColumn("[magenta1]Header[/]")
            .AddColumn("[magenta1]Value[/]");
        
        foreach (var header in ctx.Headers.Headers)
        {
            headerTable.AddRow($"[cyan]{header.Key}[/]", header.Value);
        }
        AnsiConsole.Write(headerTable);
    }
    
    AnsiConsole.MarkupLine($"  OrderId: [cyan]{ctx.Message.OrderId}[/]");
    AnsiConsole.MarkupLine($"  SKU: [cyan]{ctx.Message.Sku}[/]");
    Console.WriteLine();
}, cancellationToken: default);

// 2. Reply to CheckStockRequest (Request/Reply)
_ = messageBus.SubscribeAsync<CheckStockRequest>("stock.check", async ctx =>
{
    AnsiConsole.MarkupLine($"[yellow][[CheckStockRequest]][/] Checking stock for: [cyan]{ctx.Message.Sku}[/]");
    
    // Simulate stock check with random values
    var inStock = random.Next(0, 2) == 1;
    var quantity = inStock ? random.Next(1, 100) : 0;
    
    var response = new StockStatusResponse(ctx.Message.Sku, inStock, quantity);
    AnsiConsole.MarkupLine($"  Responding: InStock=[{(inStock ? "green" : "red")}]{inStock}[/], Quantity=[cyan]{quantity}[/]");
    Console.WriteLine();
    
    await ctx.RespondAsync(response);
}, cancellationToken: default);

// 3. Subscribe to ShipOrderCommand with Queue Group (Load Balancing)
_ = messageBus.SubscribeAsync<ShipOrderCommand>("orders.ship", async ctx =>
{
    AnsiConsole.MarkupLine($"[cyan][[App2]][/] Shipping Order: [yellow]{ctx.Message.OrderId}[/]");
    
    // Simulate work
    await Task.Delay(500);
}, queueGroup: "ShippingService", cancellationToken: default);

// ─────────────────────────────────────────────────────────────────────────────
// JetStream Consumer - Durable consumption with metadata
// ─────────────────────────────────────────────────────────────────────────────

AnsiConsole.MarkupLine("[blue]Starting JetStream consumer for ORDERS_STREAM...[/]");

// Consume from JetStream with metadata display and poison message handling
_ = jsConsumer.ConsumeAsync<OrderPlacedEvent>(
    StreamName.From("ORDERS_STREAM"),
    SubjectName.From("orders.>"),
    async ctx =>
    {
        var rule = new Rule("[blue]JetStream Message Received[/]").LeftJustified();
        AnsiConsole.Write(rule);
        
        // Display JetStream metadata
        var metaTable = new Table()
            .Border(TableBorder.Simple)
            .BorderColor(Color.Blue)
            .AddColumn("[blue]Metadata[/]")
            .AddColumn("[blue]Value[/]");
        
        metaTable.AddRow("Stream Sequence", ctx.Sequence.ToString("N0"));
        metaTable.AddRow("Delivery Count", ctx.NumDelivered.ToString());
        metaTable.AddRow("Timestamp", ctx.Timestamp.ToString("u"));
        metaTable.AddRow("Subject", ctx.Subject);
        metaTable.AddRow("Redelivered", ctx.Redelivered.ToString());
        
        AnsiConsole.Write(metaTable);
        
        // Display message content
        AnsiConsole.MarkupLine($"  OrderId: [cyan]{ctx.Message.OrderId}[/]");
        AnsiConsole.MarkupLine($"  SKU: [cyan]{ctx.Message.Sku ?? "(null)"}[/]");
        
        // Display headers if present
        if (ctx.Headers.Headers.Count > 0)
        {
            AnsiConsole.MarkupLine("  Headers:");
            foreach (var header in ctx.Headers.Headers)
            {
                AnsiConsole.MarkupLine($"    [magenta]{header.Key}[/]: {header.Value}");
            }
        }
        
        // Check for poison message simulation via headers
        if (ctx.Headers.Headers.TryGetValue("x-poison", out var poisonValue) && poisonValue == "true")
        {
            AnsiConsole.MarkupLine("[red]  ⚠ POISON MESSAGE DETECTED![/]");
            AnsiConsole.MarkupLine($"  [red]Delivery attempt {ctx.NumDelivered} - calling Term to trigger DLQ[/]");
            
            // Terminate the message (don't redeliver, send to DLQ)
            await ctx.TermAsync();
            Console.WriteLine();
            return;
        }
        
        // Check for SKU that should fail validation
        if (ctx.Message.Sku != null && ctx.Message.Sku.StartsWith("POISON-"))
        {
            AnsiConsole.MarkupLine($"[yellow]  ⚠ Invalid SKU detected: {ctx.Message.Sku}[/]");
            
            if (ctx.NumDelivered >= 3)
            {
                AnsiConsole.MarkupLine("[red]  Max deliveries reached - calling Term to trigger DLQ[/]");
                await ctx.TermAsync();
            }
            else
            {
                AnsiConsole.MarkupLine($"[yellow]  Delivery {ctx.NumDelivered}/3 - calling Nak to retry[/]");
                await ctx.NackAsync();
            }
            Console.WriteLine();
            return;
        }
        
        // Normal processing - acknowledge the message
        AnsiConsole.MarkupLine("[green]  ✓ Message processed successfully[/]");
        await ctx.AckAsync();
        Console.WriteLine();
    },
    new JetStreamConsumeOptions
    {
        QueueGroup = QueueGroup.From("orders-consumer")
    },
    cancellationToken: default);

AnsiConsole.MarkupLine("[green]Subscriptions active. Waiting for messages...[/]");
AnsiConsole.MarkupLine("[dim]• Core NATS: orders.placed, stock.check, orders.ship[/]");
AnsiConsole.MarkupLine("[dim]• JetStream: ORDERS_STREAM (orders-consumer)[/]");
AnsiConsole.MarkupLine("[dim]Press Ctrl+C to exit.[/]\n");

// Keep the app running
await host.RunAsync();
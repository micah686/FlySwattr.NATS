using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
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
});

var host = builder.Build();
var logger = host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("InventoryService");
var messageBus = host.Services.GetRequiredService<IMessageBus>();
var random = new Random();

AnsiConsole.MarkupLine("[green]Connected to NATS. Setting up subscriptions...[/]\n");

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

AnsiConsole.MarkupLine("[green]Subscriptions active. Waiting for messages...[/]");
AnsiConsole.MarkupLine("[dim]Press Ctrl+C to exit.[/]\n");

// Keep the app running
await host.RunAsync();
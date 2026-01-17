using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shared;
using Spectre.Console;

// Build host with NATS configuration
var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddFlySwattrNatsCore(options =>
{
    options.Url = "nats://localhost:4222";
});

var host = builder.Build();

// Get the message bus
var messageBus = host.Services.GetRequiredService<IMessageBus>();

// Connect to NATS
AnsiConsole.MarkupLine("[green]Connecting to NATS...[/]");
await Task.Delay(500); // Give connection time to establish

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
                "4. Exit"));

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
        case "4. Exit":
            running = false;
            break;
    }
}

AnsiConsole.MarkupLine("[yellow]Goodbye![/]");

// ─────────────────────────────────────────────────────────────────────────────
// Menu Actions
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
                await Task.Delay(100); // Small delay so we can see distribution
            }
        });
    
    AnsiConsole.MarkupLine("[green]✓ All 10 ship commands sent![/]");
    AnsiConsole.MarkupLine("[dim]Check App2 and App3 consoles to see load balancing in action.[/]\n");
}
using FlySwattr.NATS.Abstractions;
using Shared;
using Spectre.Console;

namespace ConsoleApp1;

public class Core(IMessageBus messageBus)
{
    public async Task PlaceOrderAsync()
    {
        var orderId = Guid.NewGuid();
        var sku = $"SKU-{Random.Shared.Next(100, 999)}";
        
        var orderEvent = new OrderPlacedEvent(orderId, sku);
        
        await messageBus.PublishAsync("orders.placed", orderEvent);
        
        AnsiConsole.MarkupLine($"[green]✓ Published OrderPlacedEvent[/]");
        AnsiConsole.MarkupLine($"  OrderId: [cyan]{orderId}[/]");
        AnsiConsole.MarkupLine($"  SKU: [cyan]{sku}[/]\n");
    }

    public async Task CheckStockAsync()
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

    public async Task ShipOrdersAsync()
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
}

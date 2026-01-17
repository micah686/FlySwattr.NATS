using FlySwattr.NATS.Abstractions;
using Shared;
using Spectre.Console;

namespace ConsoleApp1;

public class Headers(IMessageBus messageBus)
{
    public async Task PlaceOrderWithHeadersAsync()
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
}

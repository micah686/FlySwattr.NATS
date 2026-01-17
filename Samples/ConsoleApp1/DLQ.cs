using FlySwattr.NATS.Abstractions;
using Shared;
using Spectre.Console;

namespace ConsoleApp1;

public class DLQ(IDlqRemediationService dlqService, IJetStreamPublisher jsPublisher, IMessageBus messageBus)
{
    public async Task DlqManagerMenuAsync(Func<bool> isRunning)
    {
        var inDlqMenu = true;
        while (inDlqMenu && isRunning())
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
                        "Back to Main Menu"));

            switch (choice)
            {
                case "1. Simulate Poison Message": await DlqSimulatePoisonAsync(); break;
                case "2. List DLQ Entries": await DlqListAsync(); break;
                case "3. Replay Message": await DlqReplayAsync(); break;
                case "4. Archive Message": await DlqArchiveAsync(); break;
                case "5. Delete Message": await DlqDeleteAsync(); break;
                case "Back to Main Menu": inDlqMenu = false; break;
            }
        }
    }

    private async Task DlqSimulatePoisonAsync()
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
        
        AnsiConsole.MarkupLine($"[red]WARN: Published POISON message[/]\n  OrderId: [cyan]{orderId}[/]\n  SKU: [yellow]{sku}[/]");
        AnsiConsole.MarkupLine("[dim]This message will fail validation in ConsoleApp2 and be routed to DLQ after max retries.[/]");
    }

    private async Task DlqListAsync()
    {
        var entries = await dlqService.ListAsync(limit: 50);
        if (entries.Count == 0) { AnsiConsole.MarkupLine("[dim]No entries in DLQ.[/]"); return; }
        var table = new Table().AddColumns("ID", "Stream", "Consumer", "Subject", "Deliveries", "Status", "Stored At");
        foreach (var entry in entries) {
            var color = entry.Status == DlqMessageStatus.Resolved ? "green" : "yellow";
            table.AddRow($"[cyan]{entry.Id[..Math.Min(12, entry.Id.Length)]}...[/]", entry.OriginalStream, entry.OriginalConsumer, entry.OriginalSubject, entry.DeliveryCount.ToString(), $"[{color}]{entry.Status}[/]", entry.StoredAt.ToString("g"));
        }
        AnsiConsole.Write(table);
        AnsiConsole.MarkupLine($"[dim]Total: {entries.Count} entries[/]");
    }

    private async Task DlqReplayAsync()
    {
        var id = AnsiConsole.Ask<string>("Enter DLQ entry ID to replay:");
        var result = await dlqService.ReplayAsync(id);
        if (result.Success) AnsiConsole.MarkupLine($"[green]OK: Successfully replayed message {id}[/]");
        else AnsiConsole.MarkupLine($"[red]ERR: Replay failed: {result.ErrorMessage}[/]");
    }

    private async Task DlqArchiveAsync()
    {
        var id = AnsiConsole.Ask<string>("Enter DLQ entry ID to archive:");
        var reason = AnsiConsole.Ask<string>("Enter archive reason (optional):", "");
        var result = await dlqService.ArchiveAsync(id, string.IsNullOrEmpty(reason) ? null : reason);
        if (result.Success) AnsiConsole.MarkupLine($"[green]OK: Successfully archived message {id}[/]");
        else AnsiConsole.MarkupLine($"[red]ERR: Archive failed: {result.ErrorMessage}[/]");
    }

    private async Task DlqDeleteAsync()
    {
        var id = AnsiConsole.Ask<string>("Enter DLQ entry ID to delete:");
        if (!AnsiConsole.Confirm($"[red]Are you sure you want to permanently delete {id}?[/", false)) return;
        var result = await dlqService.DeleteAsync(id);
        if (result.Success) AnsiConsole.MarkupLine($"[green]OK: Successfully deleted message {id}[/]");
        else AnsiConsole.MarkupLine($"[red]ERR: Delete failed: {result.ErrorMessage}[/]");
    }
}
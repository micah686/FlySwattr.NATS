using FlySwattr.NATS.Abstractions;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Shared;
using Spectre.Console;

namespace ConsoleApp1;

public class JetStream(INatsJSContext jsContext, IJetStreamPublisher jsPublisher, DLQ dlq)
{
    public async Task JetStreamMenuAsync(Func<bool> isRunning)
    {
        var inJsMenu = true;
        while (inJsMenu && isRunning())
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
                        "Back to Main Menu"));

            switch (choice)
            {
                case "1. Infrastructure: List Streams": await JsListStreamsAsync(); break;
                case "2. Infrastructure: Inspect Stream": await JsInspectStreamAsync(); break;
                case "3. Infrastructure: Purge Stream": await JsPurgeStreamAsync(); break;
                case "4. Publish with Message ID (Idempotent)": await JsPublishWithIdAsync(); break;
                case "5. DLQ Manager": await dlq.DlqManagerMenuAsync(isRunning); break;
                case "Back to Main Menu": inJsMenu = false; break;
            }
        }
    }

    private async Task JsListStreamsAsync()
    {
        var table = new Table().AddColumns("Stream Name", "Messages", "Bytes", "Subjects");
        var hasStreams = false;
        await foreach (var streamName in jsContext.ListStreamNamesAsync())
        {
            hasStreams = true;
            try {
                var stream = await jsContext.GetStreamAsync(streamName);
                var info = stream.Info;
                table.AddRow($"[cyan]{streamName}[/]", info.State.Messages.ToString("N0"), $"{info.State.Bytes:N0}", string.Join(", ", info.Config.Subjects ?? []));
            } catch { table.AddRow($"[cyan]{streamName}[/]", "?", "?", "?"); }
        }
        if (hasStreams) AnsiConsole.Write(table);
        else AnsiConsole.MarkupLine("[dim]No streams found.[/]");
    }

    private async Task JsInspectStreamAsync()
    {
        var streamName = AnsiConsole.Ask<string>("Enter stream name:", "ORDERS_STREAM");
        try {
            var stream = await jsContext.GetStreamAsync(streamName);
            var info = stream.Info;
            AnsiConsole.Write(new Rule($"[cyan]{streamName}[/] Configuration").LeftJustified());
            var configTable = new Table().AddColumn("Property").AddColumn("Value");
            configTable.AddRow("Subjects", string.Join(", ", info.Config.Subjects ?? []));
            configTable.AddRow("Storage", info.Config.Storage.ToString());
            configTable.AddRow("Retention", info.Config.Retention.ToString());
            AnsiConsole.Write(configTable);
            AnsiConsole.Write(new Rule($"[cyan]{streamName}[/] State").LeftJustified());
            var stateTable = new Table().AddColumn("Property").AddColumn("Value");
            stateTable.AddRow("Messages", info.State.Messages.ToString("N0"));
            stateTable.AddRow("Bytes", info.State.Bytes.ToString("N0"));
            AnsiConsole.Write(stateTable);
        } catch (Exception ex) { AnsiConsole.MarkupLine($"[red]Error: {ex.Message}[/]"); }
    }

    private async Task JsPurgeStreamAsync()
    {
        var streamName = AnsiConsole.Ask<string>("Enter stream name to purge:", "ORDERS_STREAM");
        if (!AnsiConsole.Confirm($"[red]Are you sure you want to purge ALL messages from {streamName}?[/]", false)) return;
        try {
            var stream = await jsContext.GetStreamAsync(streamName);
            var response = await stream.PurgeAsync(new StreamPurgeRequest());
            AnsiConsole.MarkupLine($"[green]OK: Purged {response.Purged} messages from {streamName}[/]");
        } catch (Exception ex) { AnsiConsole.MarkupLine($"[red]Error: {ex.Message}[/]"); }
    }

    private async Task JsPublishWithIdAsync()
    {
        var orderId = Guid.NewGuid();
        var messageId = AnsiConsole.Ask<string>($"Enter message ID:", $"order-{orderId}");
        var orderEvent = new OrderPlacedEvent(orderId, $"SKU-{Random.Shared.Next(100, 999)}");
        await jsPublisher.PublishAsync("orders.placed", orderEvent, messageId);
        AnsiConsole.MarkupLine($"[green]OK: Published to JetStream with Message ID {messageId}[/]");
        
        if (AnsiConsole.Confirm("Publish again with same Message ID?", true)) {
            try {
                await jsPublisher.PublishAsync("orders.placed", orderEvent, messageId);
                AnsiConsole.MarkupLine($"[yellow]Published duplicate with same Message ID: {messageId}[/]");
            } catch (NatsJSDuplicateMessageException) {
                AnsiConsole.MarkupLine($"[green]OK: DEDUPLICATION CONFIRMED![/] NATS rejected the duplicate.");
            }
        }
    }
}
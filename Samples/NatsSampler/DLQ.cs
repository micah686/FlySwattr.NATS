using FlySwattr.NATS.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Shared;
using Spectre.Console;

namespace NatsSampler;

/// <summary>
/// Demonstrates Dead Letter Queue (DLQ) operations for message remediation.
/// The DLQ stores messages that failed processing after exhausting retries.
/// </summary>
public class DlqOperations
{
    private readonly IDlqRemediationService _dlqService;
    private readonly IJetStreamPublisher _publisher;

    public DlqOperations(IServiceProvider services)
    {
        _dlqService = services.GetRequiredService<IDlqRemediationService>();
        _publisher = services.GetRequiredService<IJetStreamPublisher>();
    }

    /// <summary>
    /// Simulates a poison message that will fail processing and land in the DLQ.
    /// Note: Requires NatsSubscriber to be running with poison message handling.
    /// </summary>
    public async Task SimulatePoisonMessageAsync(string? reason = null)
    {
        reason ??= AnsiConsole.Ask<string>("Enter failure reason:", "Intentional failure for DLQ demo");

        var poisonMessage = new PoisonMessage(
            Id: $"POISON-{Guid.NewGuid():N}",
            Reason: reason,
            CreatedAt: DateTime.UtcNow);

        // Use a deterministic message ID for idempotency
        var messageId = $"poison-{poisonMessage.Id}-v1";

        AnsiConsole.MarkupLine("[yellow]Publishing poison message...[/]");
        AnsiConsole.MarkupLine("[grey]Note: Start NatsSubscriber with --simulate-failures to process this message.[/]");
        AnsiConsole.MarkupLine("[grey]The message will fail and eventually land in the DLQ.[/]");

        try
        {
            await _publisher.PublishAsync("orders.created", poisonMessage, messageId);

            AnsiConsole.MarkupLine($"[green]Published poison message:[/] {poisonMessage.Id}");
            AnsiConsole.Write(new Panel(new Rows(
                new Markup($"[bold]ID:[/] {poisonMessage.Id}"),
                new Markup($"[bold]Reason:[/] {poisonMessage.Reason}"),
                new Markup($"[bold]Created:[/] {poisonMessage.CreatedAt:O}")))
                .Header("[red]Poison Message[/]")
                .Border(BoxBorder.Rounded));
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Publish failed:[/] {ex.Message}");
        }
    }

    /// <summary>
    /// Lists all entries in the Dead Letter Queue.
    /// Entries can be filtered by stream, consumer, or status.
    /// </summary>
    public async Task ListDlqEntriesAsync()
    {
        AnsiConsole.MarkupLine("[yellow]Listing DLQ entries...[/]");

        try
        {
            var entries = await _dlqService.ListAsync(limit: 50);

            if (entries.Count == 0)
            {
                AnsiConsole.MarkupLine("[grey]No DLQ entries found.[/]");
                AnsiConsole.MarkupLine("[grey]Publish a poison message and process it with NatsSubscriber to generate DLQ entries.[/]");
                return;
            }

            var table = new Table();
            table.AddColumn("ID");
            table.AddColumn("Stream/Consumer");
            table.AddColumn("Subject");
            table.AddColumn("Deliveries");
            table.AddColumn("Stored At");
            table.AddColumn("Status");
            table.AddColumn("Error");

            foreach (var entry in entries.OrderByDescending(e => e.StoredAt))
            {
                var statusColor = entry.Status switch
                {
                    DlqMessageStatus.Pending => "yellow",
                    DlqMessageStatus.Processing => "cyan",
                    DlqMessageStatus.Resolved => "green",
                    DlqMessageStatus.Archived => "grey",
                    _ => "white"
                };

                table.AddRow(
                    $"[cyan]{TruncateString(entry.Id, 30)}[/]",
                    $"{entry.OriginalStream}/{entry.OriginalConsumer}",
                    entry.OriginalSubject,
                    entry.DeliveryCount.ToString(),
                    entry.StoredAt.ToString("g"),
                    $"[{statusColor}]{entry.Status}[/]",
                    TruncateString(entry.ErrorReason ?? "-", 30));
            }

            AnsiConsole.Write(table);
            AnsiConsole.MarkupLine($"[grey]Total entries: {entries.Count}[/]");
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Error listing DLQ entries:[/] {ex.Message}");
        }
    }

    /// <summary>
    /// Inspects a specific DLQ entry showing full details and payload.
    /// </summary>
    public async Task InspectDlqEntryAsync(string? entryId = null)
    {
        if (string.IsNullOrEmpty(entryId))
        {
            // List entries first to help user select one
            var entries = await _dlqService.ListAsync(limit: 10);
            if (entries.Count == 0)
            {
                AnsiConsole.MarkupLine("[grey]No DLQ entries found.[/]");
                return;
            }

            var choices = entries.Select(e => e.Id).ToList();
            entryId = AnsiConsole.Prompt(
                new SelectionPrompt<string>()
                    .Title("Select an entry to inspect:")
                    .PageSize(10)
                    .AddChoices(choices));
        }

        try
        {
            var entry = await _dlqService.InspectAsync(entryId);

            if (entry == null)
            {
                AnsiConsole.MarkupLine($"[yellow]Entry not found:[/] {entryId}");
                return;
            }

            // Entry Details
            var detailsPanel = new Panel(new Rows(
                new Markup($"[bold]ID:[/] {entry.Id}"),
                new Markup($"[bold]Original Stream:[/] {entry.OriginalStream}"),
                new Markup($"[bold]Original Consumer:[/] {entry.OriginalConsumer}"),
                new Markup($"[bold]Original Subject:[/] {entry.OriginalSubject}"),
                new Markup($"[bold]Original Sequence:[/] {entry.OriginalSequence}"),
                new Markup($"[bold]Delivery Count:[/] {entry.DeliveryCount}"),
                new Markup($"[bold]Stored At:[/] {entry.StoredAt:O}"),
                new Markup($"[bold]Status:[/] {entry.Status}"),
                new Markup($"[bold]Error Reason:[/] {entry.ErrorReason ?? "(none)"}"),
                new Markup($"[bold]Payload Encoding:[/] {entry.PayloadEncoding ?? "unknown"}")))
                .Header("[cyan]DLQ Entry Details[/]")
                .Border(BoxBorder.Rounded);

            AnsiConsole.Write(detailsPanel);

            // Original Headers
            if (entry.OriginalHeaders != null && entry.OriginalHeaders.Count > 0)
            {
                AnsiConsole.MarkupLine("\n[yellow]Original Headers:[/]");
                var headerTable = new Table();
                headerTable.AddColumn("Key");
                headerTable.AddColumn("Value");
                foreach (var header in entry.OriginalHeaders)
                {
                    headerTable.AddRow(header.Key, header.Value);
                }
                AnsiConsole.Write(headerTable);
            }

            // Payload
            if (entry.Payload != null && entry.Payload.Length > 0)
            {
                AnsiConsole.MarkupLine("\n[yellow]Payload:[/]");
                try
                {
                    var payloadJson = System.Text.Encoding.UTF8.GetString(entry.Payload);
                    AnsiConsole.Write(new Panel(Markup.Escape(payloadJson))
                        .Header("Message Payload")
                        .Border(BoxBorder.Rounded));
                }
                catch
                {
                    AnsiConsole.MarkupLine($"[grey]Binary payload: {entry.Payload.Length} bytes[/]");
                }
            }
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Error inspecting entry:[/] {ex.Message}");
        }
    }

    /// <summary>
    /// Replays a DLQ message to its original subject for reprocessing.
    /// Use this when the underlying issue has been fixed.
    /// </summary>
    public async Task ReplayMessageAsync(string? entryId = null)
    {
        entryId = await SelectEntryIfNeeded(entryId);
        if (string.IsNullOrEmpty(entryId)) return;

        AnsiConsole.MarkupLine($"[yellow]Replaying message:[/] {entryId}");
        AnsiConsole.MarkupLine("[grey]The message will be republished to its original subject.[/]");

        try
        {
            var result = await _dlqService.ReplayAsync(entryId);

            if (result.Success)
            {
                AnsiConsole.MarkupLine($"[green]Message replayed successfully![/]");
                AnsiConsole.MarkupLine($"[grey]Action: {result.Action}[/]");
                if (result.CompletedAt.HasValue)
                {
                    AnsiConsole.MarkupLine($"[grey]Completed at: {result.CompletedAt.Value:O}[/]");
                }
            }
            else
            {
                AnsiConsole.MarkupLine($"[red]Replay failed:[/] {result.ErrorMessage}");
                AnsiConsole.MarkupLine($"[grey]Action: {result.Action}[/]");
            }
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Error replaying message:[/] {ex.Message}");
        }
    }

    /// <summary>
    /// Archives a DLQ message without replaying it.
    /// Use for messages that should not be reprocessed but need to be retained for audit.
    /// </summary>
    public async Task ArchiveMessageAsync(string? entryId = null)
    {
        entryId = await SelectEntryIfNeeded(entryId);
        if (string.IsNullOrEmpty(entryId)) return;

        var reason = AnsiConsole.Ask<string>("Enter archive reason:", "Manual review completed - data no longer relevant");

        AnsiConsole.MarkupLine($"[yellow]Archiving message:[/] {entryId}");

        try
        {
            var result = await _dlqService.ArchiveAsync(entryId, reason);

            if (result.Success)
            {
                AnsiConsole.MarkupLine($"[green]Message archived successfully![/]");
                AnsiConsole.MarkupLine($"[grey]Reason: {reason}[/]");
            }
            else
            {
                AnsiConsole.MarkupLine($"[red]Archive failed:[/] {result.ErrorMessage}");
            }
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Error archiving message:[/] {ex.Message}");
        }
    }

    /// <summary>
    /// Permanently deletes a DLQ message.
    /// Use with caution - this removes all trace of the failed message.
    /// </summary>
    public async Task DeleteMessageAsync(string? entryId = null)
    {
        entryId = await SelectEntryIfNeeded(entryId);
        if (string.IsNullOrEmpty(entryId)) return;

        AnsiConsole.MarkupLine($"[red]WARNING: This will permanently delete the DLQ entry![/]");
        if (!AnsiConsole.Confirm("Are you sure you want to continue?", false))
        {
            AnsiConsole.MarkupLine("[grey]Delete cancelled.[/]");
            return;
        }

        try
        {
            var result = await _dlqService.DeleteAsync(entryId);

            if (result.Success)
            {
                AnsiConsole.MarkupLine($"[green]Message deleted successfully![/]");
            }
            else
            {
                AnsiConsole.MarkupLine($"[red]Delete failed:[/] {result.ErrorMessage}");
            }
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Error deleting message:[/] {ex.Message}");
        }
    }

    private async Task<string?> SelectEntryIfNeeded(string? entryId)
    {
        if (!string.IsNullOrEmpty(entryId)) return entryId;

        var entries = await _dlqService.ListAsync(filterStatus: DlqMessageStatus.Pending, limit: 10);
        if (entries.Count == 0)
        {
            AnsiConsole.MarkupLine("[grey]No pending DLQ entries found.[/]");
            return null;
        }

        var choices = entries.Select(e => e.Id).ToList();
        return AnsiConsole.Prompt(
            new SelectionPrompt<string>()
                .Title("Select a DLQ entry:")
                .PageSize(10)
                .AddChoices(choices));
    }

    private static string TruncateString(string str, int maxLength)
    {
        if (string.IsNullOrEmpty(str)) return str;
        return str.Length <= maxLength ? str : str[..(maxLength - 3)] + "...";
    }
}

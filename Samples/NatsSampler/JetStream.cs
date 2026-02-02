using FlySwattr.NATS.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Shared;
using Spectre.Console;

namespace NatsSampler;

/// <summary>
/// Demonstrates JetStream operations including stream management
/// and idempotent publishing with message IDs.
/// </summary>
public class JetStreamOperations
{
    private readonly IJetStreamPublisher _publisher;
    private readonly INatsJSContext _jsContext;

    public JetStreamOperations(IServiceProvider services)
    {
        _publisher = services.GetRequiredService<IJetStreamPublisher>();
        _jsContext = services.GetRequiredService<INatsJSContext>();
    }

    /// <summary>
    /// Lists all JetStream streams with their configurations.
    /// </summary>
    public async Task ListStreamsAsync()
    {
        AnsiConsole.MarkupLine("[yellow]Listing JetStream streams...[/]");

        var table = new Table();
        table.AddColumn("Stream Name");
        table.AddColumn("Subjects");
        table.AddColumn("Messages");
        table.AddColumn("Bytes");
        table.AddColumn("Storage");
        table.AddColumn("Retention");

        try
        {
            var streams = new List<(string Name, string Subjects, ulong Messages, ulong Bytes, string Storage, string Retention)>();

            await foreach (var streamName in _jsContext.ListStreamNamesAsync())
            {
                try
                {
                    var stream = await _jsContext.GetStreamAsync(streamName);
                    var info = stream.Info;

                    streams.Add((
                        info.Config.Name ?? streamName,
                        string.Join(", ", info.Config.Subjects ?? Array.Empty<string>()),
                        (ulong)info.State.Messages,
                        (ulong)info.State.Bytes,
                        info.Config.Storage.ToString(),
                        info.Config.Retention.ToString()));
                }
                catch
                {
                    streams.Add((streamName, "Error fetching info", 0UL, 0UL, "-", "-"));
                }
            }

            if (streams.Count == 0)
            {
                AnsiConsole.MarkupLine("[grey]No streams found.[/]");
                return;
            }

            foreach (var stream in streams.OrderBy(s => s.Name))
            {
                table.AddRow(
                    $"[cyan]{stream.Name}[/]",
                    stream.Subjects,
                    stream.Messages.ToString("N0"),
                    FormatBytes(stream.Bytes),
                    stream.Storage,
                    stream.Retention);
            }

            AnsiConsole.Write(table);
            AnsiConsole.MarkupLine($"[grey]Total streams: {streams.Count}[/]");
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Error listing streams:[/] {ex.Message}");
        }
    }

    /// <summary>
    /// Inspects a specific stream showing detailed information
    /// including consumers and state.
    /// </summary>
    public async Task InspectStreamAsync(string? streamName = null)
    {
        streamName ??= AnsiConsole.Ask<string>("Enter stream name:", OrdersTopology.OrdersStreamName.Value);

        try
        {
            var stream = await _jsContext.GetStreamAsync(streamName);
            var info = stream.Info;

            // Stream Configuration
            var configPanel = new Panel(new Rows(
                new Markup($"[bold]Name:[/] {info.Config.Name}"),
                new Markup($"[bold]Description:[/] {info.Config.Description ?? "(none)"}"),
                new Markup($"[bold]Subjects:[/] {string.Join(", ", info.Config.Subjects ?? Array.Empty<string>())}"),
                new Markup($"[bold]Storage:[/] {info.Config.Storage}"),
                new Markup($"[bold]Retention:[/] {info.Config.Retention}"),
                new Markup($"[bold]Max Age:[/] {info.Config.MaxAge}"),
                new Markup($"[bold]Max Bytes:[/] {(info.Config.MaxBytes == -1 ? "Unlimited" : FormatBytes((ulong)info.Config.MaxBytes))}"),
                new Markup($"[bold]Max Msg Size:[/] {(info.Config.MaxMsgSize == -1 ? "Unlimited" : FormatBytes((ulong)info.Config.MaxMsgSize))}"),
                new Markup($"[bold]Replicas:[/] {info.Config.NumReplicas}")))
                .Header("[cyan]Stream Configuration[/]")
                .Border(BoxBorder.Rounded);

            AnsiConsole.Write(configPanel);

            // Stream State
            var statePanel = new Panel(new Rows(
                new Markup($"[bold]Messages:[/] {info.State.Messages:N0}"),
                new Markup($"[bold]Bytes:[/] {FormatBytes((ulong)info.State.Bytes)}"),
                new Markup($"[bold]First Sequence:[/] {info.State.FirstSeq}"),
                new Markup($"[bold]Last Sequence:[/] {info.State.LastSeq}"),
                new Markup($"[bold]First Time:[/] {info.State.FirstTs:O}"),
                new Markup($"[bold]Last Time:[/] {info.State.LastTs:O}"),
                new Markup($"[bold]Consumer Count:[/] {info.State.ConsumerCount}")))
                .Header("[green]Stream State[/]")
                .Border(BoxBorder.Rounded);

            AnsiConsole.Write(statePanel);

            // List consumers
            AnsiConsole.MarkupLine("\n[yellow]Consumers:[/]");
            var consumerTable = new Table();
            consumerTable.AddColumn("Name");
            consumerTable.AddColumn("Pending");
            consumerTable.AddColumn("Redelivered");
            consumerTable.AddColumn("Ack Policy");
            consumerTable.AddColumn("Filter");

            var hasConsumers = false;
            await foreach (var consumerName in stream.ListConsumerNamesAsync())
            {
                hasConsumers = true;
                try
                {
                    var consumer = await stream.GetConsumerAsync(consumerName);
                    var consumerInfo = consumer.Info;

                    consumerTable.AddRow(
                        consumerInfo.Config.Name ?? consumerName,
                        consumerInfo.NumPending.ToString("N0"),
                        consumerInfo.NumRedelivered.ToString("N0"),
                        consumerInfo.Config.AckPolicy.ToString(),
                        consumerInfo.Config.FilterSubject ?? string.Join(", ", consumerInfo.Config.FilterSubjects ?? Array.Empty<string>()) ?? "*");
                }
                catch
                {
                    consumerTable.AddRow(consumerName, "Error", "-", "-", "-");
                }
            }

            if (hasConsumers)
            {
                AnsiConsole.Write(consumerTable);
            }
            else
            {
                AnsiConsole.MarkupLine("[grey]No consumers found.[/]");
            }
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Error inspecting stream:[/] {ex.Message}");
        }
    }

    /// <summary>
    /// Publishes a message to JetStream with a deterministic message ID
    /// for idempotent delivery. Publishing the same message ID multiple
    /// times will be deduplicated by the server.
    /// </summary>
    public async Task PublishIdempotentAsync(string? orderId = null)
    {
        orderId ??= AnsiConsole.Ask<string>("Enter order ID:", $"ORD-{Random.Shared.Next(1000, 9999)}");

        var order = new OrderCreatedEvent(
            OrderId: orderId,
            CustomerId: $"CUST-{Random.Shared.Next(1000, 9999)}",
            Amount: Math.Round((decimal)(Random.Shared.NextDouble() * 1000), 2),
            CreatedAt: DateTime.UtcNow);

        // Create a deterministic message ID based on the order ID
        // This ensures the same order can only be published once within the dedup window
        var messageId = $"order-{orderId}-created-v1";

        AnsiConsole.MarkupLine($"[yellow]Publishing with message ID:[/] {messageId}");
        AnsiConsole.MarkupLine("[grey]Publishing the same message ID again will be deduplicated.[/]");

        try
        {
            await _publisher.PublishAsync("orders.created", order, messageId);
            AnsiConsole.MarkupLine($"[green]Published order:[/] {order.OrderId}");

            // Demonstrate idempotency by publishing again
            if (AnsiConsole.Confirm("Publish the same message again to demonstrate idempotency?", true))
            {
                await _publisher.PublishAsync("orders.created", order, messageId);
                AnsiConsole.MarkupLine("[green]Second publish completed.[/]");
                AnsiConsole.MarkupLine("[grey]Note: The second message was deduplicated by the server.[/]");
            }
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Publish failed:[/] {ex.Message}");
        }
    }

    /// <summary>
    /// Purges all messages from a stream.
    /// Use with caution in production!
    /// </summary>
    public async Task PurgeStreamAsync(string? streamName = null)
    {
        streamName ??= AnsiConsole.Ask<string>("Enter stream name to purge:", OrdersTopology.OrdersStreamName.Value);

        AnsiConsole.MarkupLine($"[red]WARNING: This will delete ALL messages in stream '{streamName}'![/]");
        if (!AnsiConsole.Confirm("Are you sure you want to continue?", false))
        {
            AnsiConsole.MarkupLine("[grey]Purge cancelled.[/]");
            return;
        }

        try
        {
            var stream = await _jsContext.GetStreamAsync(streamName);
            var beforeState = stream.Info.State;

            var response = await stream.PurgeAsync(new StreamPurgeRequest());

            AnsiConsole.MarkupLine($"[green]Stream purged successfully![/]");
            AnsiConsole.MarkupLine($"[grey]Messages before purge: {beforeState.Messages:N0}[/]");
            AnsiConsole.MarkupLine($"[grey]Messages purged: {response.Purged:N0}[/]");
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Purge failed:[/] {ex.Message}");
        }
    }

    /// <summary>
    /// Deletes a stream.
    /// Use with caution!
    /// </summary>
    public async Task DeleteStreamAsync(string? streamName = null)
    {
        streamName ??= AnsiConsole.Ask<string>("Enter stream name to delete:", OrdersTopology.OrdersStreamName.Value);

        AnsiConsole.MarkupLine($"[red]WARNING: This will PERMANENTLY DELETE stream '{streamName}' and all its messages![/]");
        if (!AnsiConsole.Confirm("Are you sure you want to continue?", false))
        {
            AnsiConsole.MarkupLine("[grey]Delete cancelled.[/]");
            return;
        }

        try
        {
            await _jsContext.DeleteStreamAsync(streamName);
            AnsiConsole.MarkupLine($"[green]Stream '{streamName}' deleted successfully![/]");
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Delete failed:[/] {ex.Message}");
        }
    }

    private static string FormatBytes(ulong bytes)
    {
        string[] sizes = ["B", "KB", "MB", "GB", "TB"];
        int order = 0;
        double size = bytes;

        while (size >= 1024 && order < sizes.Length - 1)
        {
            order++;
            size /= 1024;
        }

        return $"{size:0.##} {sizes[order]}";
    }
}

using FlySwattr.NATS.Abstractions;
using Spectre.Console;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace NatsTester.Demos;

public class JetStreamDemo
{
    private readonly IJetStreamPublisher _publisher;
    private readonly ITopologyManager _topologyManager;
    private readonly INatsJSContext _js;

    public JetStreamDemo(
        IJetStreamPublisher publisher, 
        ITopologyManager topologyManager,
        INatsJSContext js)
    {
        _publisher = publisher;
        _topologyManager = topologyManager;
        _js = js;
    }

    public async Task ShowMenuAsync()
    {
        while (true)
        {
            AnsiConsole.Clear();
            AnsiConsole.Write(new Rule("JetStream Demo") { Justification = Justify.Left });

            var choice = AnsiConsole.Prompt(
                new SelectionPrompt<string>()
                    .Title("Select Action:")
                    .AddChoices(new[] {
                        "Ensure Stream",
                        "List Streams",
                        "Inspect Stream",
                        "Purge Stream",
                        "Publish (Idempotent)",
                        "Back"
                    }));

            if (choice == "Back") break;

            try
            {
                switch (choice)
                {
                    case "Ensure Stream":
                        await EnsureStreamAsync();
                        break;
                    case "List Streams":
                        await ListStreamsAsync();
                        break;
                    case "Inspect Stream":
                        await InspectStreamAsync();
                        break;
                    case "Purge Stream":
                        await PurgeStreamAsync();
                        break;
                    case "Publish (Idempotent)":
                        await PublishAsync();
                        break;
                }
            }
            catch (Exception ex)
            {
                AnsiConsole.WriteException(ex);
            }
            
            AnsiConsole.WriteLine();
            AnsiConsole.MarkupLine("[grey]Press any key...[/]");
            Console.ReadKey(true);
        }
    }

    private async Task EnsureStreamAsync()
    {
        var name = AnsiConsole.Ask<string>("Stream Name:");
        var subject = AnsiConsole.Ask<string>("Subject (e.g. orders.*):");
        
        var spec = new StreamSpec
        {
            Name = StreamName.From(name),
            Subjects = new[] { subject },
            StorageType = FlySwattr.NATS.Abstractions.StorageType.File,
            Replicas = 1
        };

        await AnsiConsole.Status().StartAsync("Creating Stream...", async _ =>
        {
            await _topologyManager.EnsureStreamAsync(spec);
        });
        
        AnsiConsole.MarkupLine($"[green]Stream '{name}' created/ensured for subject '{subject}'[/]");
    }

    private async Task ListStreamsAsync()
    {
        var table = new Table();
        table.AddColumn("Name");
        table.AddColumn("Subjects");
        table.AddColumn("Messages");
        table.AddColumn("Bytes");

        await AnsiConsole.Status().StartAsync("Fetching streams...", async _ =>
        {
            await foreach (var stream in _js.ListStreamsAsync())
            {
                table.AddRow(
                    stream.Info.Config.Name ?? "N/A",
                    string.Join(", ", stream.Info.Config.Subjects ?? new List<string>()),
                    stream.Info.State.Messages.ToString(),
                    stream.Info.State.Bytes.ToString()
                );
            }
        });

        AnsiConsole.Write(table);
    }

    private async Task InspectStreamAsync()
    {
        var streamName = AnsiConsole.Ask<string>("Stream Name:");

        try
        {
            var stream = await _js.GetStreamAsync(streamName);
            var info = stream.Info;

            var grid = new Grid();
            grid.AddColumn();
            grid.AddColumn();

            grid.AddRow("Name", info.Config.Name ?? "N/A");
            grid.AddRow("Created", info.Created.ToString());
            grid.AddRow("Messages", info.State.Messages.ToString());
            grid.AddRow("Bytes", info.State.Bytes.ToString());
            grid.AddRow("First Seq", info.State.FirstSeq.ToString());
            grid.AddRow("Last Seq", info.State.LastSeq.ToString());
            
            AnsiConsole.Write(new Panel(grid).Header("Stream Details"));
        }
        catch (NatsJSApiException ex) when (ex.Error.Code == 404)
        {
             AnsiConsole.MarkupLine($"[red]Stream '{streamName}' not found.[/]");
        }
    }

    private async Task PurgeStreamAsync()
    {
        var streamName = AnsiConsole.Ask<string>("Stream Name:");

        if (!AnsiConsole.Confirm($"Are you sure you want to PURGE '{streamName}'? All messages will be lost."))
        {
            return;
        }

        try
        {
            var stream = await _js.GetStreamAsync(streamName);
            await stream.PurgeAsync(new StreamPurgeRequest());
            AnsiConsole.MarkupLine($"[green]Stream '{streamName}' purged successfully.[/]");
        }
        catch (NatsJSApiException ex) when (ex.Error.Code == 404)
        {
            AnsiConsole.MarkupLine($"[red]Stream '{streamName}' not found.[/]");
        }
    }

    private async Task PublishAsync()
    {
        var subject = AnsiConsole.Ask<string>("Subject:");
        var msg = AnsiConsole.Ask<string>("Message:");
        var msgId = AnsiConsole.Ask<string>("Message ID (Idempotency Key):");

        await _publisher.PublishAsync(subject, msg, msgId);
        AnsiConsole.MarkupLine($"[green]Published with ID '{msgId}'[/]");
        AnsiConsole.MarkupLine("[grey]Try publishing the exact same ID again - NATS should de-duplicate (only one ack).[/]");
    }
}

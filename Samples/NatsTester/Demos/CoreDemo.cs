using FlySwattr.NATS.Abstractions;
using Spectre.Console;

namespace NatsTester.Demos;

public class CoreDemo
{
    private readonly IMessageBus _bus;

    public CoreDemo(IMessageBus bus)
    {
        _bus = bus;
    }

    public async Task ShowMenuAsync()
    {
        while (true)
        {
            AnsiConsole.Clear();
            AnsiConsole.Write(new Rule("NATS Core Demo") { Justification = Justify.Left });

            var choice = AnsiConsole.Prompt(
                new SelectionPrompt<string>()
                    .Title("Select an [green]Action[/]:")
                    .AddChoices(new[] {
                        "Publish (Pub/Sub)",
                        "Publish with Headers",
                        "Publish to Queue (Load Balance)",
                        "Request / Response",
                        "Back"
                    }));

            if (choice == "Back") break;

            await HandleActionAsync(choice);
            
            AnsiConsole.WriteLine();
            AnsiConsole.MarkupLine("[grey]Press any key to continue...[/]");
            Console.ReadKey(true);
        }
    }

    private async Task HandleActionAsync(string action)
    {
        switch (action)
        {
            case "Publish (Pub/Sub)":
                await PublishAsync();
                break;
            case "Publish with Headers":
                await PublishWithHeadersAsync();
                break;
            case "Publish to Queue (Load Balance)":
                await PublishToQueueAsync();
                break;
            case "Request / Response":
                await RequestResponseAsync();
                break;
        }
    }

    private async Task PublishAsync()
    {
        var msg = AnsiConsole.Ask<string>("Enter message to publish:");
        await _bus.PublishAsync("core.demo", msg);
        AnsiConsole.MarkupLine($"[green]Published to 'core.demo':[/] {msg}");
    }

    private async Task PublishWithHeadersAsync()
    {
        var msg = AnsiConsole.Ask<string>("Enter message:");
        var headerKey = AnsiConsole.Ask<string>("Enter header key:");
        var headerValue = AnsiConsole.Ask<string>("Enter header value:");

        var headers = new MessageHeaders(new Dictionary<string, string> { { headerKey, headerValue } });
        
        await _bus.PublishAsync("core.headers", msg, headers);
        AnsiConsole.MarkupLine($"[green]Published to 'core.headers' with header {headerKey}={headerValue}[/]");
    }

    private async Task PublishToQueueAsync()
    {
        var count = AnsiConsole.Ask<int>("How many messages to send?");
        
        AnsiConsole.MarkupLine("[yellow]Sending messages... Watch Service1 and Service2 logs![/]");
        
        await AnsiConsole.Status().StartAsync("Publishing...", async ctx =>
        {
            for (int i = 0; i < count; i++)
            {
                await _bus.PublishAsync("core.queue.demo", $"Load Balance Msg #{i + 1}");
                ctx.Status($"Published message {i + 1}/{count}");
                await Task.Delay(50); // Small delay to see effect
            }
        });
        
        AnsiConsole.MarkupLine($"[green]Sent {count} messages to 'core.queue.demo'[/]");
    }

    private async Task RequestResponseAsync()
    {
        var request = AnsiConsole.Ask<string>("Enter request message:");
        
        AnsiConsole.MarkupLine("[yellow]Sending request to 'core.req'...[/]");
        
        try 
        {
            var response = await _bus.RequestAsync<string, string>("core.req", request, TimeSpan.FromSeconds(2));
            if (response != null)
            {
                AnsiConsole.MarkupLine($"[green]Received Response:[/] {response}");
            }
            else
            {
                AnsiConsole.MarkupLine("[red]No response received (Timeout or Null).[/]");
            }
        }
        catch (Exception ex)
        {
            AnsiConsole.WriteException(ex);
        }
    }
}

using Spectre.Console;
using NatsTester.Demos;

namespace NatsTester;

public class ConsoleMenu
{
    private readonly CoreDemo _coreDemo;
    private readonly StoreDemo _storeDemo;
    private readonly JetStreamDemo _jetStreamDemo;

    public ConsoleMenu(CoreDemo coreDemo, StoreDemo storeDemo, JetStreamDemo jetStreamDemo)
    {
        _coreDemo = coreDemo;
        _storeDemo = storeDemo;
        _jetStreamDemo = jetStreamDemo;

    }

    public async Task RunAsync()
    {
        while (true)
        {
            AnsiConsole.Clear();
            AnsiConsole.Write(
                new FigletText("FlySwattr NATS")
                {
                    Justification = Justify.Left,
                    Color = Color.Blue
                });

            var choice = AnsiConsole.Prompt(
                new SelectionPrompt<string>()
                    .Title("Select a [green]Demo Category[/]:")
                    .PageSize(10)
                    .AddChoices(new[] {
                        "NATS Core",
                        "Stores (KV & Object)",
                        "JetStream",
                        "DLQ (Dead Letter Queue)",
                        "Exit"
                    }));

            if (choice == "Exit") break;

            await HandleCategoryAsync(choice);
        }
    }

    private async Task HandleCategoryAsync(string category)
    {
        switch (category)
        {
            case "NATS Core":
                await _coreDemo.ShowMenuAsync();
                break;
            case "Stores (KV & Object)":
                await _storeDemo.ShowMenuAsync();
                break;
            case "JetStream":
                await _jetStreamDemo.ShowMenuAsync();
                break;
        }
    }
}

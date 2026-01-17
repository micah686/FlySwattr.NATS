using FlySwattr.NATS.Abstractions;
using Spectre.Console;

namespace ConsoleApp1;

public class Store(IKeyValueStore kvStore, IObjectStore objectStore)
{
    public async Task KeyValueMenuAsync(Func<bool> isRunning)
    {
        var inKvMenu = true;
        while (inKvMenu && isRunning())
        {
            var choice = AnsiConsole.Prompt(
                new SelectionPrompt<string>()
                    .Title("[blue]Key-Value Store[/] - Select an operation:")
                    .AddChoices(
                        "1. Put Value",
                        "2. Get Value",
                        "3. Delete Key",
                        "4. List Keys",
                        "5. Watch Changes",
                        "Back to Main Menu"));

            switch (choice)
            {
                case "1. Put Value": await KvPutAsync(); break;
                case "2. Get Value": await KvGetAsync(); break;
                case "3. Delete Key": await KvDeleteAsync(); break;
                case "4. List Keys": await KvListKeysAsync(); break;
                case "5. Watch Changes": await KvWatchAsync(); break;
                case "Back to Main Menu": inKvMenu = false; break;
            }
        }
    }

    private async Task KvPutAsync()
    {
        var key = AnsiConsole.Ask<string>("Enter [cyan]key[/]:");
        var value = AnsiConsole.Ask<string>("Enter [cyan]value[/]:");
        await kvStore.PutAsync(key, value);
        AnsiConsole.MarkupLine($"[green]OK: Stored[/] [cyan]{key}[/] = {value}");
    }

    private async Task KvGetAsync()
    {
        var key = AnsiConsole.Ask<string>("Enter [cyan]key[/] to retrieve:");
        var value = await kvStore.GetAsync<string>(key);
        if (value is not null)
            AnsiConsole.MarkupLine($"[green]Value:[/] {value}");
        else
            AnsiConsole.MarkupLine($"[yellow]Key not found: {key}[/]");
    }

    private async Task KvDeleteAsync()
    {
        var key = AnsiConsole.Ask<string>("Enter [cyan]key[/] to delete:");
        await kvStore.DeleteAsync(key);
        AnsiConsole.MarkupLine($"[green]Deleted key:[/] {key}");
    }

    private async Task KvListKeysAsync()
    {
        var table = new Table().AddColumn("Key");
        var hasKeys = false;
        await foreach (var key in kvStore.GetKeysAsync([">"]))
        {
            table.AddRow(key);
            hasKeys = true;
        }
        if (hasKeys) AnsiConsole.Write(table);
        else AnsiConsole.MarkupLine("[dim]No keys found.[/]");
    }

    private async Task KvWatchAsync()
    {
        AnsiConsole.MarkupLine("[yellow]Watching for changes (press any key to stop)...[/]");
        using var cts = new CancellationTokenSource();
        var watchTask = kvStore.WatchAsync<string>(">", async change =>
        {
            var op = change.IsDelete ? "DELETE" : "PUT";
            AnsiConsole.MarkupLine($"  {op} [cyan]{change.Key}[/] = {change.Value}");
        }, cts.Token);
        await Task.Run(() => Console.ReadKey(true));
        cts.Cancel();
        try { await watchTask; } catch (OperationCanceledException) { }
    }

    public async Task ObjectStoreMenuAsync(Func<bool> isRunning)
    {
        var inObjMenu = true;
        while (inObjMenu && isRunning())
        {
            var choice = AnsiConsole.Prompt(
                new SelectionPrompt<string>()
                    .Title("[blue]Object Store[/]")
                    .AddChoices(
                        "1. Upload",
                        "2. Info/Download",
                        "3. List",
                        "4. Delete",
                        "Back to Main Menu"));

            switch (choice)
            {
                case "1. Upload": await ObjUploadAsync(); break;
                case "2. Info/Download": await ObjDownloadAsync(); break;
                case "3. List": await ObjListAsync(); break;
                case "4. Delete": await ObjDeleteAsync(); break;
                case "Back to Main Menu": inObjMenu = false; break;
            }
        }
    }

    private async Task ObjUploadAsync()
    {
        var name = AnsiConsole.Ask<string>("Name:");
        var content = AnsiConsole.Ask<string>("Content:", "Sample Content");
        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content));
        await objectStore.PutAsync(name, stream);
        AnsiConsole.MarkupLine($"[green]Uploaded {name}[/]");
    }

    private async Task ObjDownloadAsync()
    {
        var name = AnsiConsole.Ask<string>("Name:");
        var info = await objectStore.GetInfoAsync(name);
        if (info == null) { AnsiConsole.MarkupLine("[red]Not found[/]"); return; }
        AnsiConsole.MarkupLine($"[cyan]{info.Key}[/] ({info.Size} bytes)");
        if (AnsiConsole.Confirm("Display content?"))
        {
            var ms = new MemoryStream();
            await objectStore.GetAsync(name, ms);
            var text = System.Text.Encoding.UTF8.GetString(ms.ToArray());
            AnsiConsole.WriteLine(text);
        }
    }

    private async Task ObjListAsync()
    {
        var objects = await objectStore.ListAsync();
        foreach (var obj in objects) AnsiConsole.WriteLine($"{obj.Key} ({obj.Size})");
    }

    private async Task ObjDeleteAsync()
    {
        var name = AnsiConsole.Ask<string>("Name:");
        await objectStore.DeleteAsync(name);
        AnsiConsole.MarkupLine($"[green]Deleted {name}[/]");
    }
}
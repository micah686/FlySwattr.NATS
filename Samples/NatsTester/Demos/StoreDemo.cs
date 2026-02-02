using System.Text;
using FlySwattr.NATS.Abstractions;
using Spectre.Console;

namespace NatsTester.Demos;

public class StoreDemo
{
    private readonly Func<string, IKeyValueStore> _kvFactory;
    private readonly Func<string, IObjectStore> _objFactory;
    private readonly ITopologyManager _topologyManager;

    private const string KvBucket = "demo-kv";
    private const string ObjBucket = "demo-obj";

    public StoreDemo(
        Func<string, IKeyValueStore> kvFactory,
        Func<string, IObjectStore> objFactory,
        ITopologyManager topologyManager)
    {
        _kvFactory = kvFactory;
        _objFactory = objFactory;
        _topologyManager = topologyManager;
    }

    public async Task ShowMenuAsync()
    {
        // Ensure buckets exist
        await AnsiConsole.Status().StartAsync("Ensuring Stores Exist...", async _ =>
        {
            await _topologyManager.EnsureBucketAsync(BucketName.From(KvBucket), StorageType.Memory);
            await _topologyManager.EnsureObjectStoreAsync(BucketName.From(ObjBucket), StorageType.File);
        });

        while (true)
        {
            AnsiConsole.Clear();
            AnsiConsole.Write(new Rule("Stores Demo") { Justification = Justify.Left });

            var choice = AnsiConsole.Prompt(
                new SelectionPrompt<string>()
                    .Title("Select a [green]Store Type[/]:")
                    .AddChoices(new[] {
                        "KV Store Operations",
                        "Object Store Operations",
                        "Back"
                    }));

            if (choice == "Back") break;

            if (choice == "KV Store Operations") await HandleKvOpsAsync();
            else await HandleObjOpsAsync();
        }
    }

    private async Task HandleKvOpsAsync()
    {
        var kvStore = _kvFactory(KvBucket);

        while (true)
        {
            AnsiConsole.Clear();
            AnsiConsole.Write(new Rule($"KV Store ({KvBucket})") { Justification = Justify.Left });

            var choice = AnsiConsole.Prompt(
                new SelectionPrompt<string>()
                    .Title("Select Action:")
                    .AddChoices(new[] {
                        "Put", "Get", "Delete", "List Keys", "Watch", "Back"
                    }));

            if (choice == "Back") break;

            try 
            {
                switch (choice)
                {
                    case "Put":
                        var key = AnsiConsole.Ask<string>("Key:");
                        var val = AnsiConsole.Ask<string>("Value:");
                        await kvStore.PutAsync(key, val);
                        AnsiConsole.MarkupLine($"[green]Put {key}={val}[/]");
                        break;
                    case "Get":
                        key = AnsiConsole.Ask<string>("Key:");
                        var res = await kvStore.GetAsync<string>(key);
                        AnsiConsole.MarkupLine($"[green]Got:[/] {res ?? "(null)"}");
                        break;
                    case "Delete":
                        key = AnsiConsole.Ask<string>("Key:");
                        await kvStore.DeleteAsync(key);
                        AnsiConsole.MarkupLine($"[green]Deleted {key}[/]");
                        break;
                    case "List Keys":
                        AnsiConsole.MarkupLine("[yellow]Listing keys...[/]");
                        await foreach (var k in kvStore.GetKeysAsync(new[] { ">" }))
                        {
                            AnsiConsole.WriteLine($"- {k}");
                        }
                        break;
                    case "Watch":
                        key = AnsiConsole.Ask<string>("Key to watch (or > for all):");
                        AnsiConsole.MarkupLine("[yellow]Watching... Press Ctrl+C to stop (actually just waits 10s for demo)[/]");
                        using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
                        {
                            try {
                                await kvStore.WatchAsync<string>(key, async (change) => {
                                    AnsiConsole.MarkupLine($"[blue]Change:[/] {change.Type} - Key: {change.Key}, Value: {change.Value}");
                                    await Task.CompletedTask;
                                }, cts.Token);
                            } catch (OperationCanceledException) {}
                        }
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

    private async Task HandleObjOpsAsync()
    {
        var objStore = _objFactory(ObjBucket);

        while (true)
        {
            AnsiConsole.Clear();
            AnsiConsole.Write(new Rule($"Object Store ({ObjBucket})") { Justification = Justify.Left });

            var choice = AnsiConsole.Prompt(
                new SelectionPrompt<string>()
                    .Title("Select Action:")
                    .AddChoices(new[] {
                        "Upload (String as File)", "Download", "Info", "Delete", "List", "Back"
                    }));

            if (choice == "Back") break;

            try
            {
                switch (choice)
                {
                    case "Upload (String as File)":
                        var name = AnsiConsole.Ask<string>("Object Name:");
                        var content = AnsiConsole.Ask<string>("Content:");
                        using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(content)))
                        {
                            await objStore.PutAsync(name, stream);
                        }
                        AnsiConsole.MarkupLine($"[green]Uploaded {name}[/]");
                        break;
                    case "Download":
                        name = AnsiConsole.Ask<string>("Object Name:");
                        using (var ms = new MemoryStream())
                        {
                            await objStore.GetAsync(name, ms);
                            var dlContent = Encoding.UTF8.GetString(ms.ToArray());
                            AnsiConsole.MarkupLine($"[green]Content:[/] {dlContent}");
                        }
                        break;
                    case "Info":
                        name = AnsiConsole.Ask<string>("Object Name:");
                        var info = await objStore.GetInfoAsync(name);
                        if (info != null)
                        {
                            var table = new Table();
                            table.AddColumn("Property");
                            table.AddColumn("Value");
                            table.AddRow("Name", info.Key);
                            table.AddRow("Size", info.Size.ToString());
                            table.AddRow("Modified", info.LastModified.ToString());
                            table.AddRow("Digest", info.Digest ?? "");
                            AnsiConsole.Write(table);
                        }
                        else
                        {
                            AnsiConsole.MarkupLine("[red]Object not found[/]");
                        }
                        break;
                    case "Delete":
                        name = AnsiConsole.Ask<string>("Object Name:");
                        await objStore.DeleteAsync(name);
                        AnsiConsole.MarkupLine($"[green]Deleted {name}[/]");
                        break;
                    case "List":
                        var objs = await objStore.ListAsync();
                        var listTable = new Table();
                        listTable.AddColumn("Name");
                        listTable.AddColumn("Size");
                        foreach(var o in objs) listTable.AddRow(o.Key, o.Size.ToString());
                        AnsiConsole.Write(listTable);
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
}

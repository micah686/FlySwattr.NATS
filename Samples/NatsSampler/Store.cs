using FlySwattr.NATS.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Shared;
using Spectre.Console;

namespace NatsSampler;

/// <summary>
/// Demonstrates Key-Value Store and Object Store operations.
/// These provide persistent storage backed by NATS JetStream.
/// </summary>
public class StoreOperations
{
    private readonly Func<string, IKeyValueStore> _kvFactory;
    private readonly Func<string, IObjectStore> _objFactory;
    private readonly string _kvBucket;
    private readonly string _objBucket;

    public StoreOperations(IServiceProvider services)
    {
        _kvFactory = services.GetRequiredService<Func<string, IKeyValueStore>>();
        _objFactory = services.GetRequiredService<Func<string, IObjectStore>>();
        _kvBucket = OrdersTopology.ConfigBucket.Value;
        _objBucket = OrdersTopology.DocumentsBucket.Value;
    }

    #region KV Store Operations

    /// <summary>
    /// Stores a value in the KV Store.
    /// Values are serialized as JSON and stored with versioning.
    /// </summary>
    public async Task KvPutAsync(string? key = null, string? value = null)
    {
        key ??= AnsiConsole.Ask<string>("Enter key:", "config.app.theme");
        value ??= AnsiConsole.Ask<string>("Enter value:", "dark");

        var kvStore = _kvFactory(_kvBucket);

        var config = new AppConfiguration(
            Key: key,
            Value: value,
            UpdatedAt: DateTime.UtcNow);

        try
        {
            await AnsiConsole.Status()
                .StartAsync($"Storing value in {_kvBucket}...", async ctx =>
                {
                    await kvStore.PutAsync(key, config);
                });

            AnsiConsole.MarkupLine($"[green]Stored:[/] {key} = {value}");
            AnsiConsole.MarkupLine($"[grey]Bucket: {_kvBucket}[/]");
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Error storing value:[/] {ex.Message}");
            AnsiConsole.MarkupLine("[grey]Note: NATS KV keys can only contain alphanumeric, dash, underscore, forward slash, and equals[/]");
        }
    }

    /// <summary>
    /// Retrieves a value from the KV Store.
    /// </summary>
    public async Task KvGetAsync(string? key = null)
    {
        key ??= AnsiConsole.Ask<string>("Enter key:", "config.app.theme");

        var kvStore = _kvFactory(_kvBucket);

        try
        {
            var config = await AnsiConsole.Status()
                .StartAsync($"Retrieving from {_kvBucket}...", async ctx =>
                {
                    return await kvStore.GetAsync<AppConfiguration>(key);
                });

            if (config != null)
            {
                AnsiConsole.MarkupLine($"[green]Found:[/] {key}");
                var panel = new Panel(new Rows(
                    new Markup($"[bold]Key:[/] {config.Key}"),
                    new Markup($"[bold]Value:[/] {config.Value}"),
                    new Markup($"[bold]Updated:[/] {config.UpdatedAt:O}")))
                    .Header("[cyan]Stored Value[/]")
                    .Border(BoxBorder.Rounded);
                AnsiConsole.Write(panel);
            }
            else
            {
                AnsiConsole.MarkupLine($"[yellow]Key not found:[/] {key}");
            }
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Error retrieving key:[/] {ex.Message}");
        }
    }

    /// <summary>
    /// Deletes a key from the KV Store.
    /// </summary>
    public async Task KvDeleteAsync(string? key = null)
    {
        key ??= AnsiConsole.Ask<string>("Enter key to delete:", "config.app.theme");

        var kvStore = _kvFactory(_kvBucket);

        try
        {
            await AnsiConsole.Status()
                .StartAsync($"Deleting from {_kvBucket}...", async ctx =>
                {
                    await kvStore.DeleteAsync(key);
                });

            AnsiConsole.MarkupLine($"[green]Deleted:[/] {key}");
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Error deleting key:[/] {ex.Message}");
        }
    }

    /// <summary>
    /// Lists keys matching a pattern using NATS wildcards.
    /// Patterns support: * (single token), > (multi-token suffix)
    /// </summary>
    public async Task KvListKeysAsync(string? pattern = null)
    {
        pattern ??= AnsiConsole.Ask<string>("Enter pattern (NATS wildcards: * and >):", "config.>");

        var kvStore = _kvFactory(_kvBucket);

        AnsiConsole.MarkupLine($"[yellow]Listing keys matching:[/] {pattern}");

        try
        {
            var keys = new List<string>();
            await foreach (var key in kvStore.GetKeysAsync([pattern]))
            {
                keys.Add(key);
            }

            if (keys.Count == 0)
            {
                AnsiConsole.MarkupLine("[grey]No keys found matching pattern.[/]");
                return;
            }

            var table = new Table();
            table.AddColumn("#");
            table.AddColumn("Key");

            for (var i = 0; i < keys.Count; i++)
            {
                table.AddRow((i + 1).ToString(), $"[cyan]{keys[i]}[/]");
            }

            AnsiConsole.Write(table);
            AnsiConsole.MarkupLine($"[grey]Total keys: {keys.Count}[/]");
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Error listing keys:[/] {ex.Message}");
        }
    }

    /// <summary>
    /// Watches a key for changes in real-time.
    /// Useful for configuration hot-reloading scenarios.
    /// </summary>
    public async Task KvWatchAsync(string? key = null, int durationSeconds = 10)
    {
        key ??= AnsiConsole.Ask<string>("Enter key to watch:", "config.app.theme");

        if (durationSeconds <= 0)
        {
            durationSeconds = AnsiConsole.Ask<int>("Watch for how many seconds?", 10);
        }

        var kvStore = _kvFactory(_kvBucket);

        AnsiConsole.MarkupLine($"[yellow]Watching key for {durationSeconds} seconds:[/] {key}");
        AnsiConsole.MarkupLine("[grey]Modify the key from another terminal to see changes.[/]");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(durationSeconds));
        var changeCount = 0;

        try
        {
            await kvStore.WatchAsync<AppConfiguration>(
                key,
                async change =>
                {
                    changeCount++;
                    var changeType = change.Type == KvChangeType.Put ? "[green]PUT[/]" : "[red]DELETE[/]";

                    AnsiConsole.MarkupLine($"\n[yellow]Change #{changeCount}[/] - {changeType}");
                    AnsiConsole.MarkupLine($"  Key: {change.Key}");
                    AnsiConsole.MarkupLine($"  Revision: {change.Revision}");

                    if (change.Type == KvChangeType.Put && change.Value != null)
                    {
                        AnsiConsole.MarkupLine($"  Value: {change.Value.Value}");
                        AnsiConsole.MarkupLine($"  Updated: {change.Value.UpdatedAt:O}");
                    }

                    await Task.CompletedTask;
                },
                cts.Token);
        }
        catch (OperationCanceledException)
        {
            // Expected when timeout occurs
        }

        AnsiConsole.MarkupLine($"\n[yellow]Watch ended. Observed {changeCount} changes.[/]");
    }

    #endregion

    #region Object Store Operations

    /// <summary>
    /// Uploads a file to the Object Store.
    /// Supports large files with streaming upload.
    /// </summary>
    public async Task ObjUploadAsync(string? filePath = null, string? objectKey = null)
    {
        filePath ??= AnsiConsole.Ask<string>("Enter file path to upload:", "sample.txt");

        // If file doesn't exist, create a sample file
        if (!File.Exists(filePath))
        {
            AnsiConsole.MarkupLine($"[yellow]File not found. Creating sample file: {filePath}[/]");
            await File.WriteAllTextAsync(filePath, $"Sample content created at {DateTime.UtcNow:O}\n\nThis is a test file for FlySwattr.NATS Object Store demo.");
        }

        objectKey ??= AnsiConsole.Ask<string>("Enter object key:", Path.GetFileName(filePath));

        var objStore = _objFactory(_objBucket);

        try
        {
            await using var fileStream = File.OpenRead(filePath);
            var fileSize = fileStream.Length;

            await AnsiConsole.Progress()
                .StartAsync(async ctx =>
                {
                    var task = ctx.AddTask($"[cyan]Uploading {objectKey}[/]");
                    task.MaxValue = 100;

                    var name = await objStore.PutAsync(objectKey, fileStream);
                    task.Value = 100;
                });

            AnsiConsole.MarkupLine($"[green]Uploaded:[/] {objectKey}");
            AnsiConsole.MarkupLine($"[grey]Size: {FormatBytes(fileSize)}[/]");
            AnsiConsole.MarkupLine($"[grey]Bucket: {_objBucket}[/]");
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Upload failed:[/] {ex.Message}");
        }
    }

    /// <summary>
    /// Downloads an object from the Object Store to a local file.
    /// </summary>
    public async Task ObjDownloadAsync(string? objectKey = null, string? targetPath = null)
    {
        objectKey ??= AnsiConsole.Ask<string>("Enter object key to download:", "sample.txt");
        targetPath ??= AnsiConsole.Ask<string>("Enter target path:", $"downloaded-{objectKey}");

        var objStore = _objFactory(_objBucket);

        try
        {
            await using var targetStream = File.Create(targetPath);

            await AnsiConsole.Progress()
                .StartAsync(async ctx =>
                {
                    var task = ctx.AddTask($"[cyan]Downloading {objectKey}[/]");
                    task.MaxValue = 100;

                    await objStore.GetAsync(objectKey, targetStream);
                    task.Value = 100;
                });

            var fileInfo = new FileInfo(targetPath);
            AnsiConsole.MarkupLine($"[green]Downloaded:[/] {targetPath}");
            AnsiConsole.MarkupLine($"[grey]Size: {FormatBytes(fileInfo.Length)}[/]");
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Download failed:[/] {ex.Message}");
        }
    }

    /// <summary>
    /// Gets metadata about an object without downloading it.
    /// </summary>
    public async Task ObjInfoAsync(string? objectKey = null)
    {
        objectKey ??= AnsiConsole.Ask<string>("Enter object key:", "sample.txt");

        var objStore = _objFactory(_objBucket);

        try
        {
            var info = await objStore.GetInfoAsync(objectKey);

            if (info == null)
            {
                AnsiConsole.MarkupLine($"[yellow]Object not found:[/] {objectKey}");
                return;
            }

            var panel = new Panel(new Rows(
                new Markup($"[bold]Key:[/] {info.Key}"),
                new Markup($"[bold]Bucket:[/] {info.Bucket}"),
                new Markup($"[bold]Size:[/] {FormatBytes(info.Size)}"),
                new Markup($"[bold]Modified:[/] {info.LastModified:O}"),
                new Markup($"[bold]Digest:[/] {info.Digest}"),
                new Markup($"[bold]Description:[/] {info.Description ?? "(none)"}"),
                new Markup($"[bold]Deleted:[/] {info.Deleted}")))
                .Header("[cyan]Object Info[/]")
                .Border(BoxBorder.Rounded);

            AnsiConsole.Write(panel);

            if ((info.Headers?.Count ?? 0) > 0)
            {
                AnsiConsole.MarkupLine("\n[yellow]Headers:[/]");
                var table = new Table();
                table.AddColumn("Key");
                table.AddColumn("Value");
                foreach (var header in info.Headers!)
                {
                    table.AddRow(header.Key, header.Value);
                }
                AnsiConsole.Write(table);
            }
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Error getting info:[/] {ex.Message}");
        }
    }

    /// <summary>
    /// Lists all objects in the Object Store.
    /// </summary>
    public async Task ObjListAsync()
    {
        var objStore = _objFactory(_objBucket);

        AnsiConsole.MarkupLine($"[yellow]Listing objects in {_objBucket}...[/]");

        try
        {
            var objects = await objStore.ListAsync();
            var objectList = objects.ToList();

            if (objectList.Count == 0)
            {
                AnsiConsole.MarkupLine("[grey]No objects found.[/]");
                return;
            }

            var table = new Table();
            table.AddColumn("Name");
            table.AddColumn("Size");
            table.AddColumn("Modified");
            table.AddColumn("Description");

            foreach (var obj in objectList.OrderBy(o => o.Key))
            {
                table.AddRow(
                    $"[cyan]{obj.Key}[/]",
                    FormatBytes(obj.Size),
                    obj.LastModified.ToString("g"),
                    obj.Description ?? "-");
            }

            AnsiConsole.Write(table);
            AnsiConsole.MarkupLine($"[grey]Total objects: {objectList.Count}[/]");
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Error listing objects:[/] {ex.Message}");
        }
    }

    /// <summary>
    /// Deletes an object from the Object Store.
    /// </summary>
    public async Task ObjDeleteAsync(string? objectKey = null)
    {
        objectKey ??= AnsiConsole.Ask<string>("Enter object key to delete:", "sample.txt");

        var objStore = _objFactory(_objBucket);

        try
        {
            await objStore.DeleteAsync(objectKey);
            AnsiConsole.MarkupLine($"[green]Deleted:[/] {objectKey}");
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Delete failed:[/] {ex.Message}");
        }
    }

    #endregion

    private static string FormatBytes(long bytes)
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

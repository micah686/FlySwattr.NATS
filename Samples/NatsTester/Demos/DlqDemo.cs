using FlySwattr.NATS.Abstractions;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Spectre.Console;

namespace NatsTester.Demos;

public class DlqDemo
{
    private const string DemoStreamName = "DLQ-DEMO-EVENTS";
    private const string DemoConsumerName = "dlq-demo-consumer";
    private const string DemoSubject = "dlq.demo.events";
    private const int MaxDeliverAttempts = 3;

    private readonly INatsJSContext _js;
    private readonly IJetStreamPublisher _publisher;
    private readonly IDlqStore _dlqStore;

    public DlqDemo(INatsJSContext js, IJetStreamPublisher publisher, IDlqStore dlqStore)
    {
        _js = js;
        _publisher = publisher;
        _dlqStore = dlqStore;
    }

    public async Task ShowMenuAsync()
    {
        while (true)
        {
            AnsiConsole.Clear();
            AnsiConsole.Write(new Rule("DLQ (Dead Letter Queue) Demo") { Justification = Justify.Left });
            AnsiConsole.MarkupLine("[grey]Demonstrates poison message handling and DLQ flow[/]");
            AnsiConsole.WriteLine();

            var choice = AnsiConsole.Prompt(
                new SelectionPrompt<string>()
                    .Title("Select Action:")
                    .AddChoices(new[]
                    {
                        "Simulate Poison Message",
                        "List DLQ Messages",
                        "Back"
                    }));

            if (choice == "Back") break;

            try
            {
                switch (choice)
                {
                    case "Simulate Poison Message":
                        await SimulatePoisonMessageAsync();
                        break;
                    case "List DLQ Messages":
                        await ListDlqMessagesAsync();
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

    private async Task SimulatePoisonMessageAsync()
    {
        AnsiConsole.MarkupLine("[yellow]Setting up DLQ demo topology...[/]");

        // 1. Ensure the demo stream exists
        await EnsureStreamAsync(DemoStreamName, DemoSubject);

        // 2. Ensure consumer with MaxDeliver=3
        await EnsureConsumerAsync(DemoStreamName, DemoConsumerName, DemoSubject);

        // 3. Publish a "poison" message
        var messageId = $"poison-{Guid.NewGuid():N}";
        var poisonPayload = $"POISON_MESSAGE_{DateTime.UtcNow:HHmmss}";

        AnsiConsole.MarkupLine($"[blue]Publishing poison message with ID: {messageId}[/]");
        await _publisher.PublishAsync(DemoSubject, poisonPayload, messageId);

        // 4. Consume and fail repeatedly until MaxDeliver is exceeded
        AnsiConsole.MarkupLine($"[yellow]Consuming message and simulating failures (MaxDeliver={MaxDeliverAttempts})...[/]");

        var consumer = await _js.GetConsumerAsync(DemoStreamName, DemoConsumerName);
        var deliveryCount = 0;
        ulong? originalSequence = null;
        var startTime = DateTime.UtcNow;
        var timeout = TimeSpan.FromSeconds(15);

        await AnsiConsole.Status().StartAsync("Processing...", async ctx =>
        {
            while (deliveryCount < MaxDeliverAttempts && DateTime.UtcNow - startTime < timeout)
            {
                try
                {
                    var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
                    await foreach (var msg in consumer.ConsumeAsync<string>(opts: new NatsJSConsumeOpts { MaxMsgs = 1 }, cancellationToken: cts.Token))
                    {
                        deliveryCount++;
                        originalSequence ??= msg.Metadata?.Sequence.Stream;

                        ctx.Status($"[red]Attempt {deliveryCount}/{MaxDeliverAttempts}: Simulating failure...[/]");
                        AnsiConsole.MarkupLine(
                            $"  [red]Attempt {deliveryCount}:[/] Received message, simulating processing failure...");

                        // NAK the message to trigger redelivery
                        await msg.NakAsync();

                        // Small delay to allow NATS to redeliver
                        await Task.Delay(500);
                        break;
                    }
                }
                catch (OperationCanceledException)
                {
                    // Timeout waiting for message - might be exhausted
                    break;
                }
            }
        });

        // 5. After max deliveries, the message is considered "poison" - store in DLQ
        if (deliveryCount >= MaxDeliverAttempts)
        {
            AnsiConsole.MarkupLine(
                $"[red]Message exhausted all {MaxDeliverAttempts} delivery attempts - moving to DLQ![/]");

            // Store the DLQ entry (simulating what DefaultDlqPoisonHandler does)
            var dlqEntry = new DlqMessageEntry
            {
                Id = $"{DemoStreamName}-{DemoConsumerName}-{originalSequence ?? 0}",
                OriginalStream = DemoStreamName,
                OriginalConsumer = DemoConsumerName,
                OriginalSubject = DemoSubject,
                OriginalSequence = originalSequence ?? 0,
                DeliveryCount = deliveryCount,
                StoredAt = DateTimeOffset.UtcNow,
                ErrorReason = "Simulated poison message - processing intentionally failed",
                Status = DlqMessageStatus.Pending
            };

            await _dlqStore.StoreAsync(dlqEntry);
            AnsiConsole.MarkupLine($"[green]DLQ entry stored with ID: {dlqEntry.Id}[/]");
        }
        else
        {
            AnsiConsole.MarkupLine($"[yellow]Only {deliveryCount} delivery attempts recorded (expected {MaxDeliverAttempts})[/]");
        }

        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[green]Poison message simulation complete![/]");
        AnsiConsole.MarkupLine("[grey]Use 'List DLQ Messages' to see the captured entry.[/]");
    }

    private async Task ListDlqMessagesAsync()
    {
        AnsiConsole.MarkupLine("[blue]Querying DLQ Store...[/]");
        AnsiConsole.WriteLine();

        var entries = await _dlqStore.ListAsync(limit: 50);

        if (entries.Count == 0)
        {
            AnsiConsole.MarkupLine("[yellow]No messages found in DLQ Store.[/]");
            AnsiConsole.MarkupLine("[grey]Run 'Simulate Poison Message' to generate a DLQ entry.[/]");
            return;
        }

        var table = new Table();
        table.AddColumn("ID");
        table.AddColumn("Stream");
        table.AddColumn("Consumer");
        table.AddColumn("Subject");
        table.AddColumn("Deliveries");
        table.AddColumn("Status");
        table.AddColumn("Stored At");
        table.AddColumn("Error Reason");

        foreach (var entry in entries)
        {
            table.AddRow(
                entry.Id,
                entry.OriginalStream,
                entry.OriginalConsumer,
                entry.OriginalSubject,
                entry.DeliveryCount.ToString(),
                entry.Status.ToString(),
                entry.StoredAt.ToString("yyyy-MM-dd HH:mm:ss"),
                Markup.Escape(entry.ErrorReason?.Length > 40
                    ? entry.ErrorReason[..40] + "..."
                    : entry.ErrorReason ?? "N/A")
            );
        }

        AnsiConsole.MarkupLine($"[green]Found {entries.Count} message(s) in DLQ Store:[/]");
        AnsiConsole.Write(table);
    }

    private async Task EnsureStreamAsync(string streamName, string subject)
    {
        try
        {
            await _js.GetStreamAsync(streamName);
            AnsiConsole.MarkupLine($"[grey]Stream '{streamName}' verified.[/]");
        }
        catch (NatsJSApiException ex) when (ex.Error.Code == 404)
        {
            await _js.CreateStreamAsync(new StreamConfig(streamName, [subject]));
            AnsiConsole.MarkupLine($"[green]Stream '{streamName}' created.[/]");
        }
    }

    private async Task EnsureConsumerAsync(string streamName, string consumerName, string filterSubject)
    {
        try
        {
            var config = new ConsumerConfig(consumerName)
            {
                DurableName = consumerName,
                AckPolicy = ConsumerConfigAckPolicy.Explicit,
                MaxDeliver = MaxDeliverAttempts,
                FilterSubject = filterSubject,
                AckWait = TimeSpan.FromSeconds(5) // Short ack wait for demo
            };

            await _js.CreateOrUpdateConsumerAsync(streamName, config);
            AnsiConsole.MarkupLine($"[grey]Consumer '{consumerName}' created/updated with MaxDeliver={MaxDeliverAttempts}.[/]");
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Warning creating consumer: {ex.Message}[/]");
        }
    }
}

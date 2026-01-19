using FlySwattr.NATS.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Shared;
using Spectre.Console;

namespace NatsSampler;

/// <summary>
/// Demonstrates core NATS operations including publish/subscribe,
/// request/reply, and custom headers.
/// </summary>
public class CoreOperations
{
    private readonly IMessageBus _messageBus;

    public CoreOperations(IServiceProvider services)
    {
        _messageBus = services.GetRequiredService<IMessageBus>();
    }

    /// <summary>
    /// Publishes a simple message to the orders.created subject.
    /// This demonstrates basic pub/sub messaging.
    /// </summary>
    public async Task PublishMessageAsync(string? message = null)
    {
        var orderId = message ?? AnsiConsole.Ask<string>("Enter order ID:", $"ORD-{Guid.NewGuid():N}");

        var order = new OrderCreatedEvent(
            OrderId: orderId,
            CustomerId: $"CUST-{Random.Shared.Next(1000, 9999)}",
            Amount: Math.Round((decimal)(Random.Shared.NextDouble() * 1000), 2),
            CreatedAt: DateTime.UtcNow);

        await AnsiConsole.Status()
            .StartAsync("Publishing message...", async ctx =>
            {
                await _messageBus.PublishAsync("orders.created", order);
            });

        AnsiConsole.MarkupLine($"[green]Published order:[/] {order.OrderId}");
        var json = System.Text.Json.JsonSerializer.Serialize(order, new System.Text.Json.JsonSerializerOptions { WriteIndented = true });
        AnsiConsole.Write(new Panel(Markup.Escape(json))
            .Header("Message Payload")
            .Border(BoxBorder.Rounded));
    }

    /// <summary>
    /// Publishes a message with custom headers.
    /// Headers can be used for routing, tracing, or metadata.
    /// </summary>
    public async Task PublishWithHeadersAsync(string? message = null)
    {
        var orderId = message ?? AnsiConsole.Ask<string>("Enter order ID:", $"ORD-{Guid.NewGuid():N}");

        var order = new OrderCreatedEvent(
            OrderId: orderId,
            CustomerId: $"CUST-{Random.Shared.Next(1000, 9999)}",
            Amount: Math.Round((decimal)(Random.Shared.NextDouble() * 1000), 2),
            CreatedAt: DateTime.UtcNow);

        // Create custom headers for tracing and routing
        var headers = new MessageHeaders(new Dictionary<string, string>
        {
            ["trace-id"] = Guid.NewGuid().ToString(),
            ["source"] = "nats-sampler",
            ["priority"] = "high",
            ["timestamp"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString()
        });

        await AnsiConsole.Status()
            .StartAsync("Publishing message with headers...", async ctx =>
            {
                await _messageBus.PublishAsync("orders.created", order, headers);
            });

        AnsiConsole.MarkupLine($"[green]Published order with headers:[/] {order.OrderId}");

        var table = new Table();
        table.AddColumn("Header");
        table.AddColumn("Value");
        foreach (var header in headers.Headers)
        {
            table.AddRow(header.Key, header.Value);
        }
        AnsiConsole.Write(table);
    }

    /// <summary>
    /// Demonstrates the request/reply pattern.
    /// Sends a query and waits for a response.
    /// Note: Requires NatsSubscriber to be running to respond.
    /// </summary>
    public async Task RequestReplyAsync(string? orderId = null)
    {
        orderId ??= AnsiConsole.Ask<string>("Enter order ID to query:", "ORD-12345");

        var query = new OrderQuery(orderId);
        
        // Debug: verify MemoryPack generator worked
        bool isMemoryPackable = typeof(OrderQuery).IsDefined(typeof(MemoryPack.MemoryPackableAttribute), false);
        AnsiConsole.MarkupLine($"[grey]Debug: OrderQuery is MemoryPackable: {isMemoryPackable}[/]");

        AnsiConsole.MarkupLine($"[yellow]Sending request for order:[/] {orderId}");
        AnsiConsole.MarkupLine("[grey]Note: Start NatsSubscriber to respond to requests.[/]");

        try
        {
            var response = await AnsiConsole.Status()
                .StartAsync("Waiting for response...", async ctx =>
                {
                    return await _messageBus.RequestAsync<OrderQuery, OrderQueryResponse>(
                        "orders.query",
                        query,
                        timeout: TimeSpan.FromSeconds(5));
                });

            if (response != null)
            {
                AnsiConsole.MarkupLine("[green]Received response:[/]");
                var responseJson = System.Text.Json.JsonSerializer.Serialize(response, new System.Text.Json.JsonSerializerOptions { WriteIndented = true });
                AnsiConsole.Write(new Panel(Markup.Escape(responseJson))
                    .Header("Response")
                    .Border(BoxBorder.Rounded));
            }
            else
            {
                AnsiConsole.MarkupLine("[yellow]No response received (timeout).[/]");
            }
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Request failed:[/] {ex.Message}");
        }
    }

    /// <summary>
    /// Subscribes to order events for a specified duration.
    /// Demonstrates wildcard subscriptions.
    /// </summary>
    public async Task SubscribeAsync(int durationSeconds = 10)
    {
        if (durationSeconds <= 0)
        {
            durationSeconds = AnsiConsole.Ask<int>("Listen for how many seconds?", 10);
        }

        AnsiConsole.MarkupLine($"[yellow]Subscribing to orders.> for {durationSeconds} seconds...[/]");
        AnsiConsole.MarkupLine("[grey]Publish messages from another terminal to see them here.[/]");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(durationSeconds));
        var messageCount = 0;

        try
        {
            // Start the subscription (this returns after subscription is established, not after timeout)
            await _messageBus.SubscribeAsync<OrderCreatedEvent>(
                "orders.>",
                async context =>
                {
                    messageCount++;

                    // Handle messages that can't be deserialized as OrderCreatedEvent
                    // (e.g., PoisonMessage published to orders.created for DLQ testing)
                    OrderCreatedEvent order;
                    try
                    {
                        order = context.Message;
                    }
                    catch (InvalidOperationException)
                    {
                        AnsiConsole.MarkupLine($"[yellow]Received message #{messageCount}:[/]");
                        AnsiConsole.MarkupLine($"  Subject: [cyan]{context.Subject}[/]");
                        AnsiConsole.MarkupLine($"  [grey](Message could not be deserialized as OrderCreatedEvent)[/]");
                        return;
                    }

                    AnsiConsole.MarkupLine($"[green]Received message #{messageCount}:[/]");
                    AnsiConsole.MarkupLine($"  Subject: [cyan]{context.Subject}[/]");
                    AnsiConsole.MarkupLine($"  Order ID: {order.OrderId}");
                    AnsiConsole.MarkupLine($"  Amount: ${order.Amount:F2}");

                    if (context.Headers.Headers.Count > 0)
                    {
                        AnsiConsole.MarkupLine("  Headers:");
                        foreach (var header in context.Headers.Headers)
                        {
                            AnsiConsole.MarkupLine($"    {header.Key}: {header.Value}");
                        }
                    }

                    await Task.CompletedTask;
                },
                queueGroup: null,
                cancellationToken: cts.Token);

            // Wait for the subscription duration (SubscribeAsync returns immediately after setup)
            await Task.Delay(Timeout.Infinite, cts.Token);
        }
        catch (OperationCanceledException)
        {
            // Expected when timeout occurs
        }

        AnsiConsole.MarkupLine($"[yellow]Subscription ended. Received {messageCount} messages.[/]");
    }
}

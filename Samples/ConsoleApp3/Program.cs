using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Extensions;
using FlySwattr.NATS.Topology.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shared;
using Spectre.Console;

AnsiConsole.MarkupLine("[magenta]ConsoleApp3 (Fulfillment Service) Starting...[/]");

// Build host with Enterprise NATS configuration
var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddEnterpriseNATSMessaging(options =>
{
    options.Core.Url = "nats://localhost:4222";
    options.EnableCaching = true;
    options.EnableResilience = true;
    options.EnableTopologyProvisioning = true;
});

// Register Orders topology for auto-provisioning
builder.Services.AddNatsTopologySource<OrdersTopology>();

var host = builder.Build();
var messageBus = host.Services.GetRequiredService<IMessageBus>();
var jsConsumer = host.Services.GetRequiredService<IJetStreamConsumer>();

AnsiConsole.MarkupLine("[green]Connected to NATS. Setting up subscriptions...[/]\n");

// ─────────────────────────────────────────────────────────────────────────────
// Core NATS: Queue Group Load Balancing
// ─────────────────────────────────────────────────────────────────────────────

// Subscribe to ShipOrderCommand with Queue Group (Load Balancing)
// This service is part of the same "ShippingService" queue group as App2
// NATS will distribute messages between App2 and App3
_ = messageBus.SubscribeAsync<ShipOrderCommand>("orders.ship", async ctx =>
{
    AnsiConsole.MarkupLine($"[magenta][[App3]][/] Shipping Order: [yellow]{ctx.Message.OrderId}[/]");
    
    // Display headers if present
    if (ctx.Headers.Headers.Count > 0)
    {
        foreach (var header in ctx.Headers.Headers)
        {
            AnsiConsole.MarkupLine($"  [dim]{header.Key}: {header.Value}[/]");
        }
    }
    
    // Simulate work
    await Task.Delay(500);
    AnsiConsole.MarkupLine($"[green]  ✓ Order {ctx.Message.OrderId} shipped![/]");
    Console.WriteLine();
}, queueGroup: "ShippingService", cancellationToken: default);

// ─────────────────────────────────────────────────────────────────────────────
// JetStream Consumer - Secondary consumer for load balancing
// ─────────────────────────────────────────────────────────────────────────────

AnsiConsole.MarkupLine("[blue]Starting JetStream consumer for ORDERS_STREAM (secondary)...[/]");

// Consume from JetStream - secondary consumer for load balancing demonstration
_ = jsConsumer.ConsumeAsync<OrderPlacedEvent>(
    StreamName.From("ORDERS_STREAM"),
    SubjectName.From("orders.>"),
    async ctx =>
    {
        var rule = new Rule("[magenta]JetStream Message (App3)[/]").LeftJustified();
        AnsiConsole.Write(rule);
        
        // Display compact JetStream metadata
        AnsiConsole.MarkupLine($"  [blue]Seq:[/] {ctx.Sequence} | [blue]Delivered:[/] {ctx.NumDelivered} | [blue]Subject:[/] {ctx.Subject}");
        AnsiConsole.MarkupLine($"  OrderId: [cyan]{ctx.Message.OrderId}[/]");
        AnsiConsole.MarkupLine($"  SKU: [cyan]{ctx.Message.Sku}[/]");
        
        // Display headers if present
        if (ctx.Headers.Headers.Count > 0)
        {
            var headerStr = string.Join(", ", ctx.Headers.Headers.Select(h => $"{h.Key}={h.Value}"));
            AnsiConsole.MarkupLine($"  [magenta]Headers:[/] {headerStr}");
        }
        
        // Check for poison - let App2 handle it primarily, but show we received it
        if (ctx.Headers.Headers.TryGetValue("x-poison", out var poisonValue) && poisonValue == "true")
        {
            AnsiConsole.MarkupLine("[yellow]  ⚠ Poison message received by App3 (App2 likely already handling)[/]");
        }
        
        // Normal processing
        AnsiConsole.MarkupLine("[green]  ✓ Processed by App3[/]");
        await ctx.AckAsync();
        Console.WriteLine();
    },
    new JetStreamConsumeOptions
    {
        QueueGroup = QueueGroup.From("orders-consumer-app3")
    },
    cancellationToken: default);

AnsiConsole.MarkupLine("[green]Subscriptions active. Waiting for messages...[/]");
AnsiConsole.MarkupLine("[dim]• Core NATS: orders.ship (ShippingService queue group)[/]");
AnsiConsole.MarkupLine("[dim]• JetStream: ORDERS_STREAM (orders-consumer-app3)[/]");
AnsiConsole.MarkupLine("[dim]Press Ctrl+C to exit.[/]\n");

// Keep the app running
await host.RunAsync();
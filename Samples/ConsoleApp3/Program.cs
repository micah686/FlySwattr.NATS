using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Extensions;
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
});

var host = builder.Build();
var messageBus = host.Services.GetRequiredService<IMessageBus>();

AnsiConsole.MarkupLine("[green]Connected to NATS. Setting up subscriptions...[/]\n");

// Subscribe to ShipOrderCommand with Queue Group (Load Balancing)
// This service is part of the same "ShippingService" queue group as App2
// NATS will distribute messages between App2 and App3
_ = messageBus.SubscribeAsync<ShipOrderCommand>("orders.ship", async ctx =>
{
    AnsiConsole.MarkupLine($"[magenta][[App3]][/] Shipping Order: [yellow]{ctx.Message.OrderId}[/]");
    
    // Simulate work
    await Task.Delay(500);
}, queueGroup: "ShippingService", cancellationToken: default);

AnsiConsole.MarkupLine("[green]Subscriptions active. Waiting for messages...[/]");
AnsiConsole.MarkupLine("[dim]Press Ctrl+C to exit.[/]\n");

// Keep the app running
await host.RunAsync();
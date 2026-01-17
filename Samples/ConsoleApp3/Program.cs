using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shared;

Console.WriteLine("ConsoleApp3 (Fulfillment Service) Starting...");

// Build host with NATS configuration
var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddFlySwattrNatsCore(options =>
{
    options.Url = "nats://localhost:4222";
});

var host = builder.Build();
var messageBus = host.Services.GetRequiredService<IMessageBus>();

Console.WriteLine("Connected to NATS. Setting up subscriptions...\n");

// Subscribe to ShipOrderCommand with Queue Group (Load Balancing)
// This service is part of the same "ShippingService" queue group as App2
// NATS will distribute messages between App2 and App3
_ = messageBus.SubscribeAsync<ShipOrderCommand>("orders.ship", async ctx =>
{
    Console.ForegroundColor = ConsoleColor.Magenta;
    Console.WriteLine($"[App3] Shipping Order: {ctx.Message.OrderId}");
    Console.ResetColor();
    
    // Simulate work
    await Task.Delay(500);
}, queueGroup: "ShippingService", cancellationToken: default);

Console.WriteLine("Subscriptions active. Waiting for messages...");
Console.WriteLine("Press Ctrl+C to exit.\n");

// Keep the app running
await host.RunAsync();
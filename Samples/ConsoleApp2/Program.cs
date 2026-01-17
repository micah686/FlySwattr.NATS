using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shared;

Console.WriteLine("ConsoleApp2 (Inventory Service) Starting...");

// Build host with NATS configuration
var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddFlySwattrNatsCore(options =>
{
    options.Url = "nats://localhost:4222";
});

var host = builder.Build();
var logger = host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("InventoryService");
var messageBus = host.Services.GetRequiredService<IMessageBus>();
var random = new Random();

Console.WriteLine("Connected to NATS. Setting up subscriptions...\n");

// 1. Subscribe to OrderPlacedEvent (Pub/Sub)
_ = messageBus.SubscribeAsync<OrderPlacedEvent>("orders.placed", async ctx =>
{
    Console.ForegroundColor = ConsoleColor.Green;
    Console.WriteLine($"[OrderPlacedEvent] New Order Received!");
    Console.WriteLine($"  OrderId: {ctx.Message.OrderId}");
    Console.WriteLine($"  SKU: {ctx.Message.Sku}");
    Console.ResetColor();
    Console.WriteLine();
}, cancellationToken: default);

// 2. Reply to CheckStockRequest (Request/Reply)
_ = messageBus.SubscribeAsync<CheckStockRequest>("stock.check", async ctx =>
{
    Console.ForegroundColor = ConsoleColor.Yellow;
    Console.WriteLine($"[CheckStockRequest] Checking stock for: {ctx.Message.Sku}");
    
    // Simulate stock check with random values
    var inStock = random.Next(0, 2) == 1;
    var quantity = inStock ? random.Next(1, 100) : 0;
    
    var response = new StockStatusResponse(ctx.Message.Sku, inStock, quantity);
    Console.WriteLine($"  Responding: InStock={inStock}, Quantity={quantity}");
    Console.ResetColor();
    Console.WriteLine();
    
    await ctx.RespondAsync(response);
}, cancellationToken: default);

// 3. Subscribe to ShipOrderCommand with Queue Group (Load Balancing)
_ = messageBus.SubscribeAsync<ShipOrderCommand>("orders.ship", async ctx =>
{
    Console.ForegroundColor = ConsoleColor.Cyan;
    Console.WriteLine($"[App2] Shipping Order: {ctx.Message.OrderId}");
    Console.ResetColor();
    
    // Simulate work
    await Task.Delay(500);
}, queueGroup: "ShippingService", cancellationToken: default);

Console.WriteLine("Subscriptions active. Waiting for messages...");
Console.WriteLine("Press Ctrl+C to exit.\n");

// Keep the app running
await host.RunAsync();
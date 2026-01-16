using ConsoleApp3;
using FlySwattr.NATS.Caching.Extensions;
using FlySwattr.NATS.Core.Extensions;
using FlySwattr.NATS.Topology.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

// ---- The Observer (Read-Only/Follower) ----
// This application demonstrates:
// - Queue Group Load Balancing (shares consumer with Worker)
// - FusionCache L1+L2 Caching
// - Core NATS Pub/Sub Subscription
// - Config Watching

Console.WriteLine("👁️ Starting ConsoleApp3: The Observer");

var builder = Host.CreateApplicationBuilder(args);

// Configure logging
builder.Logging.SetMinimumLevel(LogLevel.Information);
builder.Logging.AddFilter("FlySwattr", LogLevel.Debug);
builder.Logging.AddFilter("NATS", LogLevel.Warning);

// 1. Add NATS Core
builder.Services.AddFlySwattrNatsCore(opts =>
{
    opts.Url = "nats://localhost:4222";
});

// 2. Add Topology (for signal, not provisioning - Commander does that)
builder.Services.AddFlySwattrNatsTopology();

// 3. Add Caching (FusionCache L1+L2)
builder.Services.AddFlySwattrNatsCaching(opts =>
{
    opts.MemoryCacheDuration = TimeSpan.FromMinutes(5);
    opts.FailSafeMaxDuration = TimeSpan.FromHours(1);
});

// 4. Add background services
builder.Services.AddHostedService<ConfigWatcherService>();
builder.Services.AddHostedService<PingListenerService>();
builder.Services.AddHostedService<OrderObserverService>();

var host = builder.Build();

Console.WriteLine("📋 Observer services configured:");
Console.WriteLine("   - NATS Core (connection, JetStream, KV)");
Console.WriteLine("   - FusionCache (L1 memory + L2 NATS KV)");
Console.WriteLine("   - Config Watcher (watches KV changes)");
Console.WriteLine("   - Ping Listener (Core NATS pub/sub)");
Console.WriteLine("   - Order Observer (Queue Group load balancing)");
Console.WriteLine();
Console.WriteLine("Press Ctrl+C to exit.\n");

await host.RunAsync();
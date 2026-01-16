using ConsoleApp1;
using FlySwattr.NATS.Core.Extensions;
using FlySwattr.NATS.DistributedLock.Extensions;
using FlySwattr.NATS.Topology.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

// ---- The Commander (Manager/Producer) ----
// This application demonstrates:
// - Topology Provisioning (Streams, Consumers)
// - JetStream Publishing
// - Request/Reply Pattern
// - Core NATS Pub/Sub
// - Key/Value Store operations
// - Distributed Locking

Console.WriteLine("🚀 Starting ConsoleApp1: The Commander");

var builder = Host.CreateApplicationBuilder(args);

// Configure logging
builder.Logging.SetMinimumLevel(LogLevel.Information);
builder.Logging.AddFilter("FlySwattr", LogLevel.Debug);
builder.Logging.AddFilter("NATS", LogLevel.Warning);

// 1. Add NATS Core (connection, buses, stores)
builder.Services.AddFlySwattrNatsCore(opts =>
{
    opts.Url = "nats://localhost:4222";
});

// 2. Add Topology Management
builder.Services.AddFlySwattrNatsTopology();
builder.Services.AddNatsTopologySource<TopologyDefinition>();
builder.Services.AddNatsTopologyProvisioning(opts =>
{
    opts.TotalStartupTimeout = TimeSpan.FromSeconds(60);
    opts.MaxRetryAttempts = 15;
    opts.InitialRetryDelay = TimeSpan.FromSeconds(2);
});

// 3. Add Distributed Locking
builder.Services.AddFlySwattrNatsDistributedLock();

// 4. Add the Commander background service
builder.Services.AddHostedService<CommanderService>();

var host = builder.Build();

Console.WriteLine("📋 Commander services configured:");
Console.WriteLine("   - NATS Core (connection, JetStream, KV)");
Console.WriteLine("   - Topology Provisioning (streams, consumers)");
Console.WriteLine("   - Distributed Lock Provider");
Console.WriteLine("   - Commander Service");
Console.WriteLine();
Console.WriteLine("Press Ctrl+C to exit.\n");

await host.RunAsync();
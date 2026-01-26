using FlySwattr.NATS.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NatsTester;
using NatsTester.Demos;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((context, services) =>
    {
        services.AddLogging(logging => 
        {
            logging.ClearProviders(); // Keep the console clean for Spectre
            logging.AddDebug();
        });

        services.AddEnterpriseNATSMessaging(options =>
        {
            options.Core.Url = "nats://localhost:4222";
            options.EnableCaching = true;
            options.EnableResilience = true;
            options.EnableTopologyProvisioning = true;
        });

        services.AddSingleton<CoreDemo>();
        services.AddSingleton<StoreDemo>();
        services.AddSingleton<JetStreamDemo>();
        services.AddSingleton<DlqDemo>();
        services.AddSingleton<ConsoleMenu>();
    })
    .Build();

var menu = host.Services.GetRequiredService<ConsoleMenu>();
await menu.RunAsync();
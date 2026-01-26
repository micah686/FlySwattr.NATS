using FlySwattr.NATS.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Service2;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((context, services) =>
    {
        services.AddEnterpriseNATSMessaging(options =>
        {
            options.Core.Url = "nats://localhost:4222";
        });
        services.AddHostedService<Worker>();
    })
    .Build();

await host.RunAsync();
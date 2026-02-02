using FlySwattr.NATS.Abstractions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Service2;

public class Worker : BackgroundService
{
    private readonly IMessageBus _messageBus;
    private readonly ILogger<Worker> _logger;

    public Worker(IMessageBus messageBus, ILogger<Worker> logger)
    {
        _messageBus = messageBus;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Service2 is starting...");

        // Queue Group Subscriber
        await _messageBus.SubscribeAsync<string>(
            "core.queue.demo",
            async (ctx) =>
            {
                _logger.LogInformation("Service2 [QueueGroup] received: {Message}", ctx.Message);
                await Task.CompletedTask;
            },
            queueGroup: "demo-service",
            cancellationToken: stoppingToken);

        // Standard Pub/Sub Subscriber
        await _messageBus.SubscribeAsync<string>(
            "core.demo",
            async (ctx) =>
            {
                _logger.LogInformation("Service2 [Pub/Sub] received: {Message}", ctx.Message);
                await Task.CompletedTask;
            },
            cancellationToken: stoppingToken);
        
        // Req/Resp Subscriber
        await _messageBus.SubscribeAsync<string>(
            "core.req",
            async (ctx) =>
            {
                 _logger.LogInformation("Service2 [Req/Resp] received request: {Message}", ctx.Message);
                 await ctx.RespondAsync($"Response from Service2 to '{ctx.Message}'", cancellationToken: stoppingToken);
            },
            cancellationToken: stoppingToken);

        _logger.LogInformation("Service2 is ready.");

        // Keep the service alive
        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(1000, stoppingToken);
        }
    }
}

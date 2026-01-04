using System.Threading.Channels;
using FlySwattr.NATS.Abstractions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;
using Polly;

namespace FlySwattr.NATS.Hosting.Services;

public class NatsConsumerBackgroundService<T> : BackgroundService
{
    private readonly INatsJSConsumer _consumer;
    private readonly Func<IJsMessageContext<T>, Task> _handler;
    private readonly ILogger _logger;
    private readonly NatsJSConsumeOpts _consumeOpts;
    private readonly int _parallelism;
    
    // Optional Dependencies
    private readonly ResiliencePipeline _resiliencePipeline;
    private readonly IJetStreamPublisher? _dlqPublisher;
    private readonly DeadLetterPolicy? _dlqPolicy;
    private readonly IDlqNotificationService? _notificationService;

    public NatsConsumerBackgroundService(
        INatsJSConsumer consumer,
        Func<IJsMessageContext<T>, Task> handler,
        NatsJSConsumeOpts consumeOpts,
        ILogger logger,
        ResiliencePipeline? resiliencePipeline = null,
        IJetStreamPublisher? dlqPublisher = null,
        DeadLetterPolicy? dlqPolicy = null,
        IDlqNotificationService? notificationService = null)
    {
        _consumer = consumer;
        _handler = handler;
        _consumeOpts = consumeOpts;
        _logger = logger;
        _parallelism = consumeOpts.MaxMsgs?? 1;
        _resiliencePipeline = resiliencePipeline?? ResiliencePipeline.Empty;
        _dlqPublisher = dlqPublisher;
        _dlqPolicy = dlqPolicy;
        _notificationService = notificationService;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Channel-based backpressure pattern
        var channel = Channel.CreateBounded<IJsMessageContext<T>>(new BoundedChannelOptions(_parallelism)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = true
        });

        // Spawn Workers
        var workers = Enumerable.Range(0, _parallelism)
           .Select(_ => RunWorkerAsync(channel.Reader, stoppingToken))
           .ToArray();

        // Producer Loop
        try
        {
            await foreach (var msg in _consumer.ConsumeAsync<T>(opts: _consumeOpts, cancellationToken: stoppingToken))
            {
                // Note: We need a concrete context wrapper here. 
                // Since IJsMessageContext implementation is internal to Core usually, 
                // we might need to expose `JsMessageContext<T>` in Core or duplicate the wrapper here.
                // Assuming Core exposes `JsMessageContext<T>`.
                // await channel.Writer.WriteAsync(new JsMessageContext<T>(msg), stoppingToken);
                
                // For now, simpler abstraction access:
                await channel.Writer.WriteAsync(msg as IJsMessageContext<T>?? throw new InvalidCastException("Message wrapper required"), stoppingToken);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Fatal error in NATS consumer loop");
        }
        finally
        {
            channel.Writer.Complete();
            await Task.WhenAll(workers);
        }
    }

    private async Task RunWorkerAsync(ChannelReader<IJsMessageContext<T>> reader, CancellationToken ct)
    {
        await foreach (var msg in reader.ReadAllAsync(ct))
        {
            try
            {
                // Execute handler wrapped in the provided resilience pipeline
                await _resiliencePipeline.ExecuteAsync(async token => 
                {
                    await _handler(msg);
                }, ct);

                await msg.AckAsync(ct);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing message subject {Subject}", msg.Subject);
                // DLQ Logic would go here (simplified for brevity)
                await msg.NackAsync(delay: TimeSpan.FromSeconds(1), cancellationToken: ct);
            }
        }
    }
}
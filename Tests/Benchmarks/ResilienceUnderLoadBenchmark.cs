using BenchmarkDotNet.Attributes;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Resilience.Builders;
using FlySwattr.NATS.Resilience.Configuration;
using FlySwattr.NATS.Resilience.Decorators;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Polly;

namespace Benchmarks;

/// <summary>
/// Measures the interaction of the three resilience layers under contention:
/// pool bulkhead -> per-consumer semaphore -> consumer/publisher circuit breaker.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(iterationCount: 15)]
public class ResilienceUnderLoadBenchmark
{
    private BulkheadManager _bulkheadManager = null!;
    private ConsumerSemaphoreManager _semaphoreManager = null!;
    private HierarchicalResilienceBuilder _resilienceBuilder = null!;
    private ResilientJetStreamConsumer _consumer = null!;
    private ResilientJetStreamPublisher _publisher = null!;
    private FakeJetStreamConsumer _innerConsumer = null!;
    private FakeJetStreamPublisher _innerPublisher = null!;
    private JetStreamConsumeOptions _consumeOptions = null!;

    [GlobalSetup]
    public void Setup()
    {
        var bulkheadOptions = Options.Create(new BulkheadConfiguration
        {
            NamedPools = new Dictionary<string, int>
            {
                ["default"] = 8
            },
            QueueLimitMultiplier = 2
        });

        _bulkheadManager = new BulkheadManager(bulkheadOptions, NullLogger<BulkheadManager>.Instance);
        _semaphoreManager = new ConsumerSemaphoreManager(NullLogger<ConsumerSemaphoreManager>.Instance);
        _resilienceBuilder = new HierarchicalResilienceBuilder(
            NullLogger<HierarchicalResilienceBuilder>.Instance,
            new ConsumerCircuitBreakerOptions
            {
                FailureRatio = 0.5,
                MinimumThroughput = 4,
                SamplingDuration = TimeSpan.FromSeconds(30),
                BreakDuration = TimeSpan.FromMilliseconds(50)
            });

        _innerConsumer = new FakeJetStreamConsumer();
        _innerPublisher = new FakeJetStreamPublisher();

        _consumer = new ResilientJetStreamConsumer(
            _innerConsumer,
            _bulkheadManager,
            _semaphoreManager,
            _resilienceBuilder,
            NullLogger<ResilientJetStreamConsumer>.Instance);

        _publisher = new ResilientJetStreamPublisher(
            _innerPublisher,
            _resilienceBuilder,
            NullLogger<ResilientJetStreamPublisher>.Instance,
            new Polly.Retry.RetryStrategyOptions
            {
                MaxRetryAttempts = 0,
                ShouldHandle = new PredicateBuilder().Handle<Exception>()
            });

        _consumeOptions = new JetStreamConsumeOptions
        {
            BulkheadPool = "default",
            MaxConcurrency = 2
        };

        _innerConsumer.OnConsumeAsync = handler =>
            Task.WhenAll(Enumerable.Range(0, 16).Select(i => handler(new BenchmarkJsContext(i))));

        _innerPublisher.OnPublishAsync = (_, _, _, _, _) => Task.CompletedTask;
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _bulkheadManager.DisposeAsync();
        await _semaphoreManager.DisposeAsync();
        await _resilienceBuilder.DisposeAsync();
    }

    [Benchmark]
    public async Task<int> ConsumerBulkheadSemaphorePath()
    {
        var active = 0;
        var peak = 0;

        await _consumer.ConsumeAsync<int>(
            StreamName.From("BENCH"),
            SubjectName.From("bench.subject"),
            async _ =>
            {
                var current = Interlocked.Increment(ref active);
                UpdatePeak(ref peak, current);
                await Task.Delay(1);
                Interlocked.Decrement(ref active);
            },
            _consumeOptions);

        return peak;
    }

    [Benchmark]
    public async Task<int> PublisherCircuitBreakerClosedPath()
    {
        var completed = 0;
        await Task.WhenAll(Enumerable.Range(0, 32).Select(i =>
            _publisher.PublishAsync("bench.subject", i, $"msg-{i}")
                .ContinueWith(_ => Interlocked.Increment(ref completed))));

        return completed;
    }

    [Benchmark]
    public async Task<int> PublisherCircuitBreakerOpenFastFail()
    {
        var inner = new FakeJetStreamPublisher
        {
            OnPublishAsync = (_, _, _, _, _) => throw new InvalidOperationException("boom")
        };

        await using var builder = new HierarchicalResilienceBuilder(
            NullLogger<HierarchicalResilienceBuilder>.Instance,
            new ConsumerCircuitBreakerOptions
            {
                FailureRatio = 0.5,
                MinimumThroughput = 2,
                SamplingDuration = TimeSpan.FromSeconds(30),
                BreakDuration = TimeSpan.FromMilliseconds(100)
            });

        var publisher = new ResilientJetStreamPublisher(
            inner,
            builder,
            NullLogger<ResilientJetStreamPublisher>.Instance,
            new Polly.Retry.RetryStrategyOptions
            {
                MaxRetryAttempts = 0,
                ShouldHandle = new PredicateBuilder().Handle<Exception>()
            });

        var failures = 0;
        for (var i = 0; i < 8; i++)
        {
            try
            {
                await publisher.PublishAsync("bench.subject", i, $"msg-open-{i}");
            }
            catch
            {
                failures++;
            }
        }

        return failures;
    }

    private static void UpdatePeak(ref int peak, int candidate)
    {
        while (true)
        {
            var snapshot = peak;
            if (candidate <= snapshot)
            {
                return;
            }

            if (Interlocked.CompareExchange(ref peak, candidate, snapshot) == snapshot)
            {
                return;
            }
        }
    }

    private sealed class BenchmarkJsContext(int value) : IJsMessageContext<int>
    {
        public int Message => value;
        public string Subject => "bench.subject";
        public MessageHeaders Headers => new(new Dictionary<string, string>());
        public string? ReplyTo => null;
        public ulong Sequence => 1;
        public DateTimeOffset Timestamp => DateTimeOffset.UtcNow;
        public bool Redelivered => false;
        public uint NumDelivered => 1;
        public Task RespondAsync<TResponse>(TResponse response, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task AckAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task NackAsync(TimeSpan? delay = null, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task TermAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task InProgressAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
    }

    private sealed class FakeJetStreamConsumer : IJetStreamConsumer
    {
        public Func<Func<IJsMessageContext<int>, Task>, Task>? OnConsumeAsync { get; set; }

        public Task ConsumeAsync<T>(StreamName stream, SubjectName subject, Func<IJsMessageContext<T>, Task> handler, JetStreamConsumeOptions? options = null, CancellationToken cancellationToken = default)
        {
            if (typeof(T) != typeof(int) || OnConsumeAsync is null)
            {
                return Task.CompletedTask;
            }

            return OnConsumeAsync(ctx => handler((IJsMessageContext<T>)ctx));
        }

        public Task ConsumePullAsync<T>(StreamName stream, ConsumerName consumer, Func<IJsMessageContext<T>, Task> handler, JetStreamConsumeOptions? options = null, CancellationToken cancellationToken = default)
            => Task.CompletedTask;
    }

    private sealed class FakeJetStreamPublisher : IJetStreamPublisher
    {
        public Func<string, object?, string?, MessageHeaders?, CancellationToken, Task>? OnPublishAsync { get; set; }

        public Task PublishAsync<T>(string subject, T message, string? messageId, MessageHeaders? headers = null, CancellationToken cancellationToken = default)
            => OnPublishAsync?.Invoke(subject, message, messageId, headers, cancellationToken) ?? Task.CompletedTask;

        public Task PublishBatchAsync<T>(IReadOnlyList<BatchMessage<T>> messages, CancellationToken cancellationToken = default)
            => Task.CompletedTask;
    }
}

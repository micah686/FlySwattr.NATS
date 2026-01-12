using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Resilience.Builders;
using FlySwattr.NATS.Resilience.Configuration;
using FlySwattr.NATS.Resilience.Decorators;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Resilience.Decorators;

[Property("nTag", "Resilience")]
public class ResilientJetStreamConsumerTests : IAsyncDisposable
{
    private readonly IJetStreamConsumer _inner;
    private readonly BulkheadManager _bulkheadManager;
    private readonly ConsumerSemaphoreManager _semaphoreManager;
    private readonly HierarchicalResilienceBuilder _resilienceBuilder;
    private readonly ILogger<ResilientJetStreamConsumer> _logger;
    private readonly ResilientJetStreamConsumer _sut;

    public ResilientJetStreamConsumerTests()
    {
        _inner = Substitute.For<IJetStreamConsumer>();
        
        var options = Options.Create(new BulkheadConfiguration());
        var logger = Substitute.For<ILogger<BulkheadManager>>();
        _bulkheadManager = new BulkheadManager(options, logger);
        
        var semLogger = Substitute.For<ILogger<ConsumerSemaphoreManager>>();
        _semaphoreManager = new ConsumerSemaphoreManager(semLogger);
        
        var builderLogger = Substitute.For<ILogger<HierarchicalResilienceBuilder>>();
        _resilienceBuilder = new HierarchicalResilienceBuilder(builderLogger);
        
        _logger = Substitute.For<ILogger<ResilientJetStreamConsumer>>();

        _sut = new ResilientJetStreamConsumer(
            _inner,
            _bulkheadManager,
            _semaphoreManager,
            _resilienceBuilder,
            _logger);
    }

    public async ValueTask DisposeAsync()
    {
        await _bulkheadManager.DisposeAsync();
        await _semaphoreManager.DisposeAsync();
        await _resilienceBuilder.DisposeAsync();
    }

    [Test]
    public async Task ConsumeAsync_ShouldCallInner_WithCorrectParameters()
    {
        // Arrange
        var stream = StreamName.From("stream");
        var subject = SubjectName.From("subject");
        Func<IJsMessageContext<string>, Task> handler = _ => Task.CompletedTask;
        
        // Act
        await _sut.ConsumeAsync(stream, subject, handler);

        // Assert
        await _inner.Received(1).ConsumeAsync(
            stream, 
            subject, 
            Arg.Any<Func<IJsMessageContext<string>, Task>>(), 
            null, null, null, null, 
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ConsumeAsync_ShouldApplyMaxConcurrency_WhenSpecified()
    {
        // Arrange
        var stream = StreamName.From("stream");
        var subject = SubjectName.From("subject");
        var maxConcurrency = 5;
        
        // Act
        await _sut.ConsumeAsync<object>(stream, subject, _ => Task.CompletedTask, maxConcurrency: maxConcurrency);

        // Assert
        // Verify semaphore created
        var key = "stream/subject"; // Default key format in implementation
        _semaphoreManager.HasConsumerSemaphore(key).ShouldBeTrue();
        _semaphoreManager.GetCurrentCount(key).ShouldBe(maxConcurrency);
        
        await _inner.Received(1).ConsumeAsync(
            stream, 
            subject, 
            Arg.Any<Func<IJsMessageContext<object>, Task>>(), // Handler is wrapped
            null, null, maxConcurrency, null, 
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ConsumePullAsync_ShouldCallInner_WithCorrectParameters()
    {
        // Arrange
        var stream = StreamName.From("stream");
        var consumer = ConsumerName.From("consumer");
        Func<IJsMessageContext<string>, Task> handler = _ => Task.CompletedTask;
        
        // Act
        await _sut.ConsumePullAsync(stream, consumer, handler);

        // Assert
        await _inner.Received(1).ConsumePullAsync(
            stream, 
            consumer, 
            Arg.Any<Func<IJsMessageContext<string>, Task>>(), 
            10, null, null, null, 
            Arg.Any<CancellationToken>());
    }
}

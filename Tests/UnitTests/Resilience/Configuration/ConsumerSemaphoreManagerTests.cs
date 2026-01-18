using FlySwattr.NATS.Resilience.Configuration;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Resilience.Configuration;

[Property("nTag", "Resilience")]
public class ConsumerSemaphoreManagerTests : IAsyncDisposable
{
    private readonly ConsumerSemaphoreManager _sut;
    private readonly ILogger<ConsumerSemaphoreManager> _logger;

    public ConsumerSemaphoreManagerTests()
    {
        _logger = Substitute.For<ILogger<ConsumerSemaphoreManager>>();
        _sut = new ConsumerSemaphoreManager(_logger);
    }

    [Test]
    public void GetConsumerSemaphore_ShouldCreateNewSemaphore_WhenNotExists()
    {
        // Act
        var semaphore = _sut.GetConsumerSemaphore("stream/consumer", 5);

        // Assert
        semaphore.ShouldNotBeNull();
        semaphore.CurrentCount.ShouldBe(5);
        _sut.HasConsumerSemaphore("stream/consumer").ShouldBeTrue();
    }

    [Test]
    public void GetConsumerSemaphore_ShouldReturnExistingSemaphore_WhenCalledAgain()
    {
        // Arrange
        var semaphore1 = _sut.GetConsumerSemaphore("stream/consumer", 5);

        // Act
        var semaphore2 = _sut.GetConsumerSemaphore("stream/consumer", 5);

        // Assert
        semaphore1.ShouldBeSameAs(semaphore2);
    }

    [Test]
    public void GetConsumerSemaphore_ShouldThrowArgumentOutOfRangeException_WhenMaxConcurrencyLessThanOne()
    {
        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => _sut.GetConsumerSemaphore("stream/consumer", 0));
    }

    [Test]
    public void RemoveConsumerSemaphore_ShouldDisposeAndRemove()
    {
        // Arrange
        var semaphore = _sut.GetConsumerSemaphore("stream/consumer", 5);

        // Act
        _sut.RemoveConsumerSemaphore("stream/consumer");

        // Assert
        _sut.HasConsumerSemaphore("stream/consumer").ShouldBeFalse();
        _sut.GetCurrentCount("stream/consumer").ShouldBe(-1);
    }

    [Test]
    public async Task DisposeAsync_ShouldDisposeAllSemaphores()
    {
        // Arrange
        _sut.GetConsumerSemaphore("s1/c1", 5);
        _sut.GetConsumerSemaphore("s2/c2", 2);

        // Act
        await _sut.DisposeAsync();

        // Assert
        Assert.Throws<ObjectDisposedException>(() => _sut.GetConsumerSemaphore("s3/c3", 1));
    }

    [Test]
    public void GetCurrentCount_ShouldReturnCorrectValue()
    {
        // Arrange
        var semaphore = _sut.GetConsumerSemaphore("stream/consumer", 10);
        semaphore.Wait();

        // Act
        var count = _sut.GetCurrentCount("stream/consumer");

        // Assert
        count.ShouldBe(9);
    }

    public async ValueTask DisposeAsync()
    {
        await _sut.DisposeAsync();
    }
}

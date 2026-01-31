using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Core.Registries;

[Property("nTag", "Core")]
public class DlqPolicyRegistryTests
{
    private readonly ILogger<DlqPolicyRegistry> _logger;
    private readonly DlqPolicyRegistry _sut;

    public DlqPolicyRegistryTests()
    {
        _logger = Substitute.For<ILogger<DlqPolicyRegistry>>();
        _sut = new DlqPolicyRegistry(_logger);
    }

    #region Register Tests

    [Test]
    public void Register_ShouldAddPolicy_WhenPolicyDoesNotExist()
    {
        // Arrange
        var policy = CreateTestPolicy("stream1", "consumer1");

        // Act
        _sut.Register("stream1", "consumer1", policy);

        // Assert
        var result = _sut.Get("stream1", "consumer1");
        result.ShouldNotBeNull();
        result.ShouldBe(policy);
    }

    [Test]
    public void Register_ShouldUpdatePolicy_WhenPolicyAlreadyExists()
    {
        // Arrange
        var policy1 = CreateTestPolicy("stream1", "consumer1", "dlq-stream-v1");
        var policy2 = CreateTestPolicy("stream1", "consumer1", "dlq-stream-v2");

        _sut.Register("stream1", "consumer1", policy1);

        // Act
        _sut.Register("stream1", "consumer1", policy2);

        // Assert
        var result = _sut.Get("stream1", "consumer1");
        result.ShouldNotBeNull();
        result.TargetStream.Value.ShouldBe("dlq-stream-v2");
    }

    [Test]
    public void Register_ShouldLogWarning_WhenReplacingExistingPolicy()
    {
        // Arrange
        var policy1 = CreateTestPolicy("stream1", "consumer1");
        var policy2 = CreateTestPolicy("stream1", "consumer1");

        _sut.Register("stream1", "consumer1", policy1);

        // Act
        _sut.Register("stream1", "consumer1", policy2);

        // Assert
        _logger.Received(1).Log(
            LogLevel.Warning,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("stream1/consumer1")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    #endregion

    #region Unregister Tests

    [Test]
    public void Unregister_ShouldRemovePolicy_WhenPolicyExists()
    {
        // Arrange
        var policy = CreateTestPolicy("stream1", "consumer1");
        _sut.Register("stream1", "consumer1", policy);

        // Act
        _sut.Unregister("stream1", "consumer1");

        // Assert
        var result = _sut.Get("stream1", "consumer1");
        result.ShouldBeNull();
    }

    [Test]
    public void Unregister_ShouldNotThrow_WhenPolicyDoesNotExist()
    {
        // Act & Assert - should not throw
        Should.NotThrow(() => _sut.Unregister("nonexistent-stream", "nonexistent-consumer"));
    }

    #endregion

    #region Get Tests

    [Test]
    public void Get_ShouldReturnPolicy_WhenPolicyExists()
    {
        // Arrange
        var policy = CreateTestPolicy("stream1", "consumer1");
        _sut.Register("stream1", "consumer1", policy);

        // Act
        var result = _sut.Get("stream1", "consumer1");

        // Assert
        result.ShouldNotBeNull();
        result.ShouldBe(policy);
    }

    [Test]
    public void Get_ShouldReturnNull_WhenPolicyDoesNotExist()
    {
        // Act
        var result = _sut.Get("nonexistent-stream", "nonexistent-consumer");

        // Assert
        result.ShouldBeNull();
    }

    #endregion

    #region Concurrency Tests

    [Test]
    public async Task ConcurrentRegistrations_ShouldBeThreadSafe()
    {
        // Arrange
        const int concurrentOperations = 100;
        var tasks = new List<Task>();

        // Act - perform concurrent registrations
        for (var i = 0; i < concurrentOperations; i++)
        {
            var index = i;
            tasks.Add(Task.Run(() =>
            {
                var policy = CreateTestPolicy($"stream-{index}", $"consumer-{index}");
                _sut.Register($"stream-{index}", $"consumer-{index}", policy);
            }));
        }

        await Task.WhenAll(tasks);

        // Assert - all policies should be retrievable
        for (var i = 0; i < concurrentOperations; i++)
        {
            var result = _sut.Get($"stream-{i}", $"consumer-{i}");
            result.ShouldNotBeNull();
        }
    }

    [Test]
    public async Task ConcurrentMixedOperations_ShouldBeThreadSafe()
    {
        // Arrange - pre-register some policies
        for (var i = 0; i < 50; i++)
        {
            var policy = CreateTestPolicy($"stream-{i}", $"consumer-{i}");
            _sut.Register($"stream-{i}", $"consumer-{i}", policy);
        }

        var tasks = new List<Task>();

        // Act - perform concurrent mixed operations (register, unregister, get)
        for (var i = 0; i < 100; i++)
        {
            var index = i;
            tasks.Add(Task.Run(() =>
            {
                // Mix of operations based on index
                if (index % 3 == 0)
                {
                    // Register new
                    var policy = CreateTestPolicy($"new-stream-{index}", $"new-consumer-{index}");
                    _sut.Register($"new-stream-{index}", $"new-consumer-{index}", policy);
                }
                else if (index % 3 == 1)
                {
                    // Unregister existing
                    _sut.Unregister($"stream-{index % 50}", $"consumer-{index % 50}");
                }
                else
                {
                    // Get
                    _ = _sut.Get($"stream-{index % 50}", $"consumer-{index % 50}");
                }
            }));
        }

        // Assert - should complete without exceptions
        await Should.NotThrowAsync(() => Task.WhenAll(tasks));
    }

    #endregion

    #region Helpers

    private static DeadLetterPolicy CreateTestPolicy(string sourceStream, string sourceConsumer, string targetStream = "dlq-stream")
    {
        return new DeadLetterPolicy
        {
            SourceStream = sourceStream,
            SourceConsumer = sourceConsumer,
            TargetStream = StreamName.From(targetStream),
            TargetSubject = $"dlq.{sourceStream}.{sourceConsumer}"
        };
    }

    #endregion
}

using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Hosting.Services;
using Microsoft.Extensions.Logging;
using NSubstitute;
using TUnit.Core;

namespace UnitTests.Hosting.Services;

[Property("nTag", "Hosting")]
public class LoggingDlqAdvisoryHandlerTests
{
    [Test]
    public async Task HandleMaxDeliveriesExceededAsync_ShouldLogWarning()
    {
        // Arrange
        var logger = Substitute.For<ILogger<LoggingDlqAdvisoryHandler>>();
        logger.IsEnabled(LogLevel.Warning).Returns(true);
        
        var sut = new LoggingDlqAdvisoryHandler(logger);
        var advisory = new ConsumerMaxDeliveriesAdvisory(
            Type: "type",
            Id: "id",
            Timestamp: DateTimeOffset.UtcNow,
            Stream: "test-stream",
            Consumer: "test-consumer",
            StreamSeq: 123,
            Deliveries: 5,
            Domain: "test-domain"
        );

        // Act
        await sut.HandleMaxDeliveriesExceededAsync(advisory);

        // Assert - Use a simpler check that is known to work with NSubstitute and ILogger
        logger.Received(1).Log(
            LogLevel.Warning,
            Arg.Any<EventId>(),
            Arg.Any<Arg.AnyType>(),
            Arg.Any<Exception>(),
            Arg.Any<Func<Arg.AnyType, Exception?, string>>());
    }
}

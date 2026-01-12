using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core;
using Microsoft.Extensions.Logging;
using NSubstitute;
using TUnit.Core;

namespace UnitTests.Core.Services;

[Property("nTag", "Core")]
public class LoggingDlqNotificationServiceTests
{
    [Test]
    public async Task NotifyAsync_ShouldLogWarning()
    {
        // Arrange
        var logger = Substitute.For<ILogger<LoggingDlqNotificationService>>();
        logger.IsEnabled(LogLevel.Warning).Returns(true);
        var service = new LoggingDlqNotificationService(logger);
        var notification = new DlqNotification(
            MessageId: "123",
            OriginalStream: "test-stream",
            OriginalConsumer: "test-consumer",
            OriginalSubject: "test.subject",
            OriginalSequence: 42,
            DeliveryCount: 3,
            ErrorReason: "Something went wrong",
            OccurredAt: DateTimeOffset.UtcNow
        );

        // Act
        await service.NotifyAsync(notification);

        // Assert
        logger.Received().Log(
            LogLevel.Warning,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("DLQ Alert") && o.ToString()!.Contains("123")),
            null,
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Test]
    public async Task NotifyAsync_ShouldThrow_WhenNotificationIsNull()
    {
        // Arrange
        var logger = Substitute.For<ILogger<LoggingDlqNotificationService>>();
        var service = new LoggingDlqNotificationService(logger);

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => service.NotifyAsync(null!));
    }
}

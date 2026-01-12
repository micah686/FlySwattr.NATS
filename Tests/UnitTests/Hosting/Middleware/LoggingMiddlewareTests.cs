using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Hosting.Middleware;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Hosting.Middleware;

[Property("nTag", "Hosting")]
public class LoggingMiddlewareTests
{
    [Test]
    public async Task InvokeAsync_ShouldLogAndCallNext()
    {
        // Arrange
        var logger = Substitute.For<ILogger<LoggingMiddleware<string>>>();
        var middleware = new LoggingMiddleware<string>(logger);
        var context = Substitute.For<IJsMessageContext<string>>();
        context.Subject.Returns("test.subject");
        context.Sequence.Returns(123ul);
        
        var nextCalled = false;
        Func<Task> next = () => 
        {
            nextCalled = true;
            return Task.CompletedTask;
        };

        // Act
        await middleware.InvokeAsync(context, next, CancellationToken.None);

        // Assert
        nextCalled.ShouldBeTrue();
        
        // Verify Debug logs were attempted
        logger.Received().Log(
            LogLevel.Debug,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("Handling message")),
            null,
            Arg.Any<Func<object, Exception?, string>>());
            
        logger.Received().Log(
            LogLevel.Debug,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("Handled message")),
            null,
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Test]
    public async Task InvokeAsync_ShouldLogWarning_OnException()
    {
        // Arrange
        var logger = Substitute.For<ILogger<LoggingMiddleware<string>>>();
        var middleware = new LoggingMiddleware<string>(logger);
        var context = Substitute.For<IJsMessageContext<string>>();
        
        Func<Task> next = () => throw new Exception("Boom");

        // Act & Assert
        await Assert.ThrowsAsync<Exception>(() => middleware.InvokeAsync(context, next, CancellationToken.None));

        logger.Received().Log(
            LogLevel.Warning,
            Arg.Any<EventId>(),
            Arg.Is<object>(o => o.ToString()!.Contains("Message failed")),
            Arg.Any<Exception>(),
            Arg.Any<Func<object, Exception?, string>>());
    }
}

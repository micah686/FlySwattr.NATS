using FluentValidation;
using FluentValidation.Results;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Abstractions.Exceptions;
using FlySwattr.NATS.Hosting.Middleware;
using NSubstitute;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Hosting.Middleware;

[Property("nTag", "Hosting")]
public class ValidationMiddlewareTests
{
    [Test]
    public async Task InvokeAsync_ShouldCallNext_WhenNoValidators()
    {
        // Arrange
        var validators = Enumerable.Empty<IValidator<string>>();
        var middleware = new ValidationMiddleware<string>(validators);
        var context = Substitute.For<IJsMessageContext<string>>();
        
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
    }

    [Test]
    public async Task InvokeAsync_ShouldCallNext_WhenValidationPasses()
    {
        // Arrange
        var validator = Substitute.For<IValidator<string>>();
        validator.ValidateAsync(Arg.Any<ValidationContext<string>>(), Arg.Any<CancellationToken>())
            .Returns(new ValidationResult());

        var validators = new[] { validator };
        var middleware = new ValidationMiddleware<string>(validators);
        var context = Substitute.For<IJsMessageContext<string>>();
        context.Message.Returns("valid");

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
    }

    [Test]
    public async Task InvokeAsync_ShouldThrowMessageValidationException_WhenValidationFails()
    {
        // Arrange
        var validator = Substitute.For<IValidator<string>>();
        var failure = new ValidationFailure("Prop", "Error");
        validator.ValidateAsync(Arg.Any<ValidationContext<string>>(), Arg.Any<CancellationToken>())
            .Returns(new ValidationResult(new[] { failure }));

        var validators = new[] { validator };
        var middleware = new ValidationMiddleware<string>(validators);
        var context = Substitute.For<IJsMessageContext<string>>();
        context.Message.Returns("invalid");
        context.Subject.Returns("test.subject");

        Func<Task> next = () => Task.CompletedTask;

        // Act & Assert
        var ex = await Assert.ThrowsAsync<MessageValidationException>(() => 
            middleware.InvokeAsync(context, next, CancellationToken.None));
        
        ex?.Subject.ShouldBe("test.subject");
        ex?.Errors.ShouldContain("Prop: Error");
    }
}

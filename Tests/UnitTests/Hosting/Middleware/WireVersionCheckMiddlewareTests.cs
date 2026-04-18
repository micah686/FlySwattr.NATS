using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Configuration;
using FlySwattr.NATS.Hosting.Middleware;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Hosting.Middleware;

/// <summary>
/// Unit tests for WireVersionCheckMiddleware covering:
/// - LogWarning action (default, continue processing)
/// - Terminate action (kill message)
/// - Nack action (requeue message)
/// - Edge cases (missing header, invalid format)
/// </summary>
[Property("nTag", "Hosting")]
public class WireVersionCheckMiddlewareTests
{
    private IJsMessageContext<TestMessage> _context = null!;
    private ILogger<WireVersionCheckMiddleware<TestMessage>> _logger = null!;

    [Before(Test)]
    public void Setup()
    {
        _context = Substitute.For<IJsMessageContext<TestMessage>>();
        _context.Subject.Returns("test.subject");
        _context.Sequence.Returns(42UL);
        _context.Headers.Returns(new MessageHeaders(new Dictionary<string, string>()));
        _logger = Substitute.For<ILogger<WireVersionCheckMiddleware<TestMessage>>>();
        _logger.IsEnabled(default).ReturnsForAnyArgs(true);
    }

    #region MismatchAction.LogWarning Tests

    /// <summary>
    /// When version is below minimum and action is LogWarning (default),
    /// the middleware should log a warning and continue to next.
    /// </summary>
    [Test]
    public async Task InvokeAsync_VersionBelowMinimum_WithLogWarning_ShouldContinue()
    {
        // Arrange
        var options = new WireCompatibilityOptions
        {
            VersionHeaderName = "X-Protocol-Version",
            MinAcceptedVersion = 2,
            MismatchAction = VersionMismatchAction.LogWarning
        };

        var middleware = new WireVersionCheckMiddleware<TestMessage>(
            Options.Create(options),
            _logger);

        _context.Headers.Returns(new MessageHeaders(new Dictionary<string, string>
        {
            ["X-Protocol-Version"] = "1"  // Below minimum
        }));

        var nextCalled = false;
        Task Next() { nextCalled = true; return Task.CompletedTask; }

        // Act
        await middleware.InvokeAsync(_context, Next, CancellationToken.None);

        // Assert
        nextCalled.ShouldBeTrue("Should call next when action is LogWarning");
        await _context.DidNotReceive().TermAsync(Arg.Any<CancellationToken>());
        await _context.DidNotReceive().NackAsync(Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>());
    }

    #endregion

    #region MismatchAction.Terminate Tests

    /// <summary>
    /// When version is below minimum and action is Terminate,
    /// the middleware should terminate the message and not call next.
    /// </summary>
    [Test]
    public async Task InvokeAsync_VersionBelowMinimum_WithTerminate_ShouldTerminate()
    {
        // Arrange
        var options = new WireCompatibilityOptions
        {
            VersionHeaderName = "X-Protocol-Version",
            MinAcceptedVersion = 3,
            MismatchAction = VersionMismatchAction.Terminate
        };

        var middleware = new WireVersionCheckMiddleware<TestMessage>(
            Options.Create(options),
            _logger);

        _context.Headers.Returns(new MessageHeaders(new Dictionary<string, string>
        {
            ["X-Protocol-Version"] = "2"  // Below minimum
        }));

        _context.TermAsync(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);

        var nextCalled = false;
        Task Next() { nextCalled = true; return Task.CompletedTask; }

        // Act
        await middleware.InvokeAsync(_context, Next, CancellationToken.None);

        // Assert
        await _context.Received(1).TermAsync(Arg.Any<CancellationToken>());
        nextCalled.ShouldBeFalse("Should not call next when terminating");
    }

    #endregion

    #region MismatchAction.Nack Tests

    /// <summary>
    /// When version is below minimum and action is Nack,
    /// the middleware should nack the message with 10-second delay and not call next.
    /// </summary>
    [Test]
    public async Task InvokeAsync_VersionBelowMinimum_WithNack_ShouldNack()
    {
        // Arrange
        var options = new WireCompatibilityOptions
        {
            VersionHeaderName = "X-Protocol-Version",
            MinAcceptedVersion = 3,
            MismatchAction = VersionMismatchAction.Nack
        };

        var middleware = new WireVersionCheckMiddleware<TestMessage>(
            Options.Create(options),
            _logger);

        _context.Headers.Returns(new MessageHeaders(new Dictionary<string, string>
        {
            ["X-Protocol-Version"] = "1"  // Below minimum
        }));

        _context.NackAsync(Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);

        var nextCalled = false;
        Task Next() { nextCalled = true; return Task.CompletedTask; }

        // Act
        await middleware.InvokeAsync(_context, Next, CancellationToken.None);

        // Assert
        await _context.Received(1).NackAsync(
            Arg.Is<TimeSpan>(ts => ts == TimeSpan.FromSeconds(10)),
            Arg.Any<CancellationToken>());
        nextCalled.ShouldBeFalse("Should not call next when nacking");
    }

    #endregion

    #region Edge Cases

    /// <summary>
    /// When the version header is missing, middleware should allow processing to continue.
    /// This supports migration from pre-versioning deployments.
    /// </summary>
    [Test]
    public async Task InvokeAsync_NoVersionHeader_ShouldContinue()
    {
        // Arrange
        var options = new WireCompatibilityOptions
        {
            VersionHeaderName = "X-Protocol-Version",
            MinAcceptedVersion = 2,
            MismatchAction = VersionMismatchAction.Terminate  // Even with strict action
        };

        var middleware = new WireVersionCheckMiddleware<TestMessage>(
            Options.Create(options),
            _logger);

        // No version header in message
        _context.Headers.Returns(new MessageHeaders(new Dictionary<string, string>()));

        var nextCalled = false;
        Task Next() { nextCalled = true; return Task.CompletedTask; }

        // Act
        await middleware.InvokeAsync(_context, Next, CancellationToken.None);

        // Assert
        nextCalled.ShouldBeTrue("Should continue when no version header (backward compatibility)");
        await _context.DidNotReceive().TermAsync(Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// When the version header cannot be parsed, middleware should allow processing to continue.
    /// </summary>
    [Test]
    public async Task InvokeAsync_InvalidVersionFormat_ShouldContinue()
    {
        // Arrange
        var options = new WireCompatibilityOptions
        {
            VersionHeaderName = "X-Protocol-Version",
            MinAcceptedVersion = 2,
            MismatchAction = VersionMismatchAction.Terminate
        };

        var middleware = new WireVersionCheckMiddleware<TestMessage>(
            Options.Create(options),
            _logger);

        _context.Headers.Returns(new MessageHeaders(new Dictionary<string, string>
        {
            ["X-Protocol-Version"] = "not-a-number"  // Invalid format
        }));

        var nextCalled = false;
        Task Next() { nextCalled = true; return Task.CompletedTask; }

        // Act
        await middleware.InvokeAsync(_context, Next, CancellationToken.None);

        // Assert
        nextCalled.ShouldBeTrue("Should continue when version header is invalid");
        await _context.DidNotReceive().TermAsync(Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// When version matches exactly the minimum, it should be accepted.
    /// </summary>
    [Test]
    public async Task InvokeAsync_VersionEqualsMinimum_ShouldContinue()
    {
        // Arrange
        var options = new WireCompatibilityOptions
        {
            VersionHeaderName = "X-Protocol-Version",
            MinAcceptedVersion = 2,
            MismatchAction = VersionMismatchAction.Terminate
        };

        var middleware = new WireVersionCheckMiddleware<TestMessage>(
            Options.Create(options),
            _logger);

        _context.Headers.Returns(new MessageHeaders(new Dictionary<string, string>
        {
            ["X-Protocol-Version"] = "2"  // Exactly at minimum
        }));

        var nextCalled = false;
        Task Next() { nextCalled = true; return Task.CompletedTask; }

        // Act
        await middleware.InvokeAsync(_context, Next, CancellationToken.None);

        // Assert
        nextCalled.ShouldBeTrue("Should continue when version equals minimum");
        await _context.DidNotReceive().TermAsync(Arg.Any<CancellationToken>());
    }

    #endregion

    public class TestMessage
    {
        public string Data { get; set; } = "test";
    }
}

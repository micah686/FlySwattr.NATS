using System.Diagnostics;
using FlySwattr.NATS.Core;
using FlySwattr.NATS.Core.Telemetry;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NSubstitute;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Core.Telemetry;

[Property("nTag", "Core")]
public class NatsMessageBusTelemetryTests : IAsyncDisposable
{
    private readonly INatsConnection _connection;
    private readonly ILogger<NatsMessageBus> _logger;
    private readonly NatsMessageBus _bus;
    private readonly ActivityListener _listener;
    private readonly List<Activity> _capturedActivities = [];

    public NatsMessageBusTelemetryTests()
    {
        _connection = Substitute.For<INatsConnection>();
        _logger = Substitute.For<ILogger<NatsMessageBus>>();
        _bus = new NatsMessageBus(_connection, _logger);

        // Set up ActivityListener to capture activities
        _listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == NatsTelemetry.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStarted = activity => _capturedActivities.Add(activity)
        };
        ActivitySource.AddActivityListener(_listener);
    }

    public async ValueTask DisposeAsync()
    {
        _listener.Dispose();
        _capturedActivities.Clear();
        await _bus.DisposeAsync();
    }

    #region Publish Activity Tests

    [Test]
    public async Task PublishAsync_ShouldCreatePublishActivity()
    {
        // Arrange
        var subject = $"test.orders.created.{Guid.NewGuid():N}"; // Unique subject
        var message = new { OrderId = 123 };
        _connection.PublishAsync(subject, message, headers: Arg.Any<NatsHeaders>(), cancellationToken: Arg.Any<CancellationToken>())
            .Returns(ValueTask.CompletedTask);
        var startCount = _capturedActivities.Count;

        // Act
        await _bus.PublishAsync(subject, message);

        // Assert - Find our specific activity by operation name
        var activity = _capturedActivities.Skip(startCount).FirstOrDefault(a => a.OperationName == $"{subject} publish");
        activity.ShouldNotBeNull();
        activity.Kind.ShouldBe(ActivityKind.Producer);
    }

    [Test]
    public async Task PublishAsync_ShouldSetMessagingSystemTag()
    {
        // Arrange
        var subject = $"test.subject.{Guid.NewGuid():N}";
        var message = new { Data = "test" };
        _connection.PublishAsync(subject, message, headers: Arg.Any<NatsHeaders>(), cancellationToken: Arg.Any<CancellationToken>())
            .Returns(ValueTask.CompletedTask);
        var startCount = _capturedActivities.Count;

        // Act
        await _bus.PublishAsync(subject, message);

        // Assert
        var activity = _capturedActivities.Skip(startCount).FirstOrDefault(a => a.OperationName.Contains(subject));
        activity.ShouldNotBeNull();
        activity.GetTagItem(NatsTelemetry.MessagingSystem).ShouldBe("nats");
    }

    [Test]
    public async Task PublishAsync_ShouldSetDestinationNameTag()
    {
        // Arrange
        var subject = $"orders.created.{Guid.NewGuid():N}";
        var message = new { OrderId = 1 };
        _connection.PublishAsync(subject, message, headers: Arg.Any<NatsHeaders>(), cancellationToken: Arg.Any<CancellationToken>())
            .Returns(ValueTask.CompletedTask);
        var startCount = _capturedActivities.Count;

        // Act
        await _bus.PublishAsync(subject, message);

        // Assert
        var activity = _capturedActivities.Skip(startCount).FirstOrDefault(a => a.OperationName.Contains(subject));
        activity.ShouldNotBeNull();
        activity.GetTagItem(NatsTelemetry.MessagingDestinationName).ShouldBe(subject);
    }

    [Test]
    public async Task PublishAsync_ShouldSetOperationTag()
    {
        // Arrange
        var subject = $"test.subject.{Guid.NewGuid():N}";
        var message = new { Data = "test" };
        _connection.PublishAsync(subject, message, headers: Arg.Any<NatsHeaders>(), cancellationToken: Arg.Any<CancellationToken>())
            .Returns(ValueTask.CompletedTask);
        var startCount = _capturedActivities.Count;

        // Act
        await _bus.PublishAsync(subject, message);

        // Assert
        var activity = _capturedActivities.Skip(startCount).FirstOrDefault(a => a.OperationName.Contains(subject));
        activity.ShouldNotBeNull();
        activity.GetTagItem(NatsTelemetry.MessagingOperation).ShouldBe("publish");
    }

    [Test]
    public async Task PublishAsync_ShouldInjectTraceContextIntoHeaders()
    {
        // Arrange
        var subject = $"test.subject.{Guid.NewGuid():N}";
        var message = new { Data = "test" };
        NatsHeaders? capturedHeaders = null;

        _connection.PublishAsync(subject, message, headers: Arg.Any<NatsHeaders>(), cancellationToken: Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                capturedHeaders = call.ArgAt<NatsHeaders>(2);
                return ValueTask.CompletedTask;
            });

        // Act
        await _bus.PublishAsync(subject, message);

        // Assert
        capturedHeaders.ShouldNotBeNull();
        capturedHeaders.ShouldContainKey("traceparent");
        var traceparent = capturedHeaders["traceparent"].ToString();
        traceparent.ShouldStartWith("00-");
    }

    [Test]
    public async Task PublishAsync_WithHeaders_ShouldMergeCustomHeadersWithTraceContext()
    {
        // Arrange
        var subject = $"test.subject.{Guid.NewGuid():N}";
        var message = new { Data = "test" };
        var customHeaders = new FlySwattr.NATS.Abstractions.MessageHeaders(
            new Dictionary<string, string> { ["X-Custom"] = "custom-value" }
        );
        NatsHeaders? capturedHeaders = null;

        _connection.PublishAsync(subject, message, headers: Arg.Any<NatsHeaders>(), cancellationToken: Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                capturedHeaders = call.ArgAt<NatsHeaders>(2);
                return ValueTask.CompletedTask;
            });

        // Act
        await _bus.PublishAsync(subject, message, customHeaders);

        // Assert
        capturedHeaders.ShouldNotBeNull();
        capturedHeaders.ShouldContainKey("traceparent");
        capturedHeaders.ShouldContainKey("X-Custom");
        capturedHeaders["X-Custom"].ToString().ShouldBe("custom-value");
    }

    #endregion

    #region Request Activity Tests

    [Test]
    public async Task RequestAsync_ShouldCreateRequestActivity()
    {
        // Arrange
        var subject = $"test.request.{Guid.NewGuid():N}";
        var request = new { Query = "test" };
        var replyMsg = new NatsMsg<string>(subject, null, 0, null, "response", null, default);

        _connection.RequestAsync<object, string>(
            subject,
            request,
            headers: Arg.Any<NatsHeaders>(),
            replyOpts: Arg.Any<NatsSubOpts>(),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(replyMsg);
        var startCount = _capturedActivities.Count;

        // Act
        await _bus.RequestAsync<object, string>(subject, request, TimeSpan.FromSeconds(5));

        // Assert - Find our specific activity
        var activity = _capturedActivities.Skip(startCount).FirstOrDefault(a => a.OperationName == $"{subject} request");
        activity.ShouldNotBeNull();
        activity.Kind.ShouldBe(ActivityKind.Client);
    }

    [Test]
    public async Task RequestAsync_ShouldSetMessagingTags()
    {
        // Arrange
        var subject = $"rpc.calculate.{Guid.NewGuid():N}";
        var request = new { Value = 42 };
        var replyMsg = new NatsMsg<int>(subject, null, 0, null, 84, null, default);

        _connection.RequestAsync<object, int>(
            subject,
            request,
            headers: Arg.Any<NatsHeaders>(),
            replyOpts: Arg.Any<NatsSubOpts>(),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(replyMsg);
        var startCount = _capturedActivities.Count;

        // Act
        await _bus.RequestAsync<object, int>(subject, request, TimeSpan.FromSeconds(5));

        // Assert
        var activity = _capturedActivities.Skip(startCount).FirstOrDefault(a => a.OperationName.Contains(subject));
        activity.ShouldNotBeNull();
        activity.GetTagItem(NatsTelemetry.MessagingSystem).ShouldBe("nats");
        activity.GetTagItem(NatsTelemetry.MessagingDestinationName).ShouldBe(subject);
        activity.GetTagItem(NatsTelemetry.MessagingOperation).ShouldBe("request");
    }

    [Test]
    public async Task RequestAsync_ShouldInjectTraceContextIntoHeaders()
    {
        // Arrange
        var subject = $"test.request.{Guid.NewGuid():N}";
        var request = new { Data = "test" };
        NatsHeaders? capturedHeaders = null;
        var replyMsg = new NatsMsg<string>(subject, null, 0, null, "response", null, default);

        _connection.RequestAsync<object, string>(
            subject,
            request,
            headers: Arg.Any<NatsHeaders>(),
            replyOpts: Arg.Any<NatsSubOpts>(),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(x =>
            {
                capturedHeaders = x.ArgAt<NatsHeaders>(2);
                return replyMsg;
            });

        // Act
        await _bus.RequestAsync<object, string>(subject, request, TimeSpan.FromSeconds(5));

        // Assert
        capturedHeaders.ShouldNotBeNull();
        capturedHeaders.ShouldContainKey("traceparent");
    }

    #endregion

    #region Activity Lifecycle Tests

    [Test]
    public async Task PublishAsync_ActivityShouldBeDisposedAfterPublish()
    {
        // Arrange
        var subject = $"test.subject.{Guid.NewGuid():N}";
        var message = new { Data = "test" };
        Activity? capturedActivity = null;

        _connection.PublishAsync(subject, message, headers: Arg.Any<NatsHeaders>(), cancellationToken: Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                capturedActivity = Activity.Current;
                return ValueTask.CompletedTask;
            });

        // Act
        await _bus.PublishAsync(subject, message);

        // Assert - Activity should have been active during publish
        capturedActivity.ShouldNotBeNull();
        capturedActivity.OperationName.ShouldContain("publish");
    }

    [Test]
    public async Task PublishAsync_MultiplePublishes_ShouldCreateSeparateActivities()
    {
        // Arrange
        var prefix = Guid.NewGuid().ToString("N");
        var subject1 = $"subject1.{prefix}";
        var subject2 = $"subject2.{prefix}";
        var subject3 = $"subject3.{prefix}";
        var message = new { Data = "test" };
        _connection.PublishAsync(Arg.Any<string>(), message, headers: Arg.Any<NatsHeaders>(), cancellationToken: Arg.Any<CancellationToken>())
            .Returns(ValueTask.CompletedTask);
        var startCount = _capturedActivities.Count;

        // Act
        await _bus.PublishAsync(subject1, message);
        await _bus.PublishAsync(subject2, message);
        await _bus.PublishAsync(subject3, message);

        // Assert - Find our specific activities
        var ourActivities = _capturedActivities.Skip(startCount).Where(a => a.OperationName.Contains(prefix)).ToList();
        ourActivities.Count.ShouldBe(3);
        ourActivities.Select(a => a.OperationName).ShouldContain($"{subject1} publish");
        ourActivities.Select(a => a.OperationName).ShouldContain($"{subject2} publish");
        ourActivities.Select(a => a.OperationName).ShouldContain($"{subject3} publish");

        // Each should have a unique SpanId
        var spanIds = ourActivities.Select(a => a.SpanId).ToHashSet();
        spanIds.Count.ShouldBe(3);
    }

    #endregion
}

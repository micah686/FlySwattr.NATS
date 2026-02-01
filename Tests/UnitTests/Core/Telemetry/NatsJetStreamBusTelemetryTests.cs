using System.Buffers;
using System.Diagnostics;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core;
using FlySwattr.NATS.Core.Telemetry;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NSubstitute;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Core.Telemetry;

[Property("nTag", "Core")]
public class NatsJetStreamBusTelemetryTests : IAsyncDisposable
{
    private readonly INatsJSContext _jsContext;
    private readonly ILogger<NatsJetStreamBus> _logger;
    private readonly IMessageSerializer _serializer;
    private readonly NatsJetStreamBus _bus;
    private readonly ActivityListener _listener;
    private readonly List<Activity> _capturedActivities = [];

    public NatsJetStreamBusTelemetryTests()
    {
        _jsContext = Substitute.For<INatsJSContext>();
        _logger = Substitute.For<ILogger<NatsJetStreamBus>>();
        _serializer = Substitute.For<IMessageSerializer>();
        _serializer.GetContentType<TestMessage>().Returns("application/json");

        _serializer.When(x => x.Serialize(Arg.Any<IBufferWriter<byte>>(), Arg.Any<TestMessage>()))
                   .Do(x => x.Arg<IBufferWriter<byte>>().Write(new byte[] { 1 }));

        _bus = new NatsJetStreamBus(_jsContext, _logger, _serializer);

        // Set up ActivityListener to capture activities
        _listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == NatsTelemetry.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStarted = activity => _capturedActivities.Add(activity)
        };
        ActivitySource.AddActivityListener(_listener);
    }

    private record TestMessage(string Data);

    public async ValueTask DisposeAsync()
    {
        _listener.Dispose();
        _capturedActivities.Clear();
        await _bus.DisposeAsync();
    }

    // Helper to set up the mock for PublishAsync
    private void SetupPublishMock(Action<NatsHeaders>? captureHeaders = null)
    {
        _jsContext.PublishAsync(
            Arg.Any<string>(),
            Arg.Any<TestMessage>(),
            Arg.Any<INatsSerialize<TestMessage>>(),
            Arg.Any<NatsJSPubOpts>(),
            Arg.Any<NatsHeaders>(),
            Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                captureHeaders?.Invoke(call.ArgAt<NatsHeaders>(4));
                return new PubAckResponse();
            });
    }

    #region Publish Activity Tests

    [Test]
    public async Task PublishAsync_ShouldCreatePublishActivity()
    {
        // Arrange
        var subject = $"orders.created.{Guid.NewGuid():N}";
        var message = new TestMessage("test-data");
        var messageId = "order-123-created";
        SetupPublishMock();
        var startCount = _capturedActivities.Count;

        // Act
        await _bus.PublishAsync(subject, message, messageId);

        // Assert - Find our specific activity
        var activity = _capturedActivities.Skip(startCount).FirstOrDefault(a => a.OperationName == $"{subject} publish");
        activity.ShouldNotBeNull();
        activity.Kind.ShouldBe(ActivityKind.Producer);
    }

    [Test]
    public async Task PublishAsync_ShouldSetMessagingSystemTag()
    {
        // Arrange
        var subject = $"events.user.created.{Guid.NewGuid():N}";
        var message = new TestMessage("test-data");
        var messageId = "user-456-created";
        SetupPublishMock();
        var startCount = _capturedActivities.Count;

        // Act
        await _bus.PublishAsync(subject, message, messageId);

        // Assert
        var activity = _capturedActivities.Skip(startCount).FirstOrDefault(a => a.OperationName.Contains(subject));
        activity.ShouldNotBeNull();
        activity.GetTagItem(NatsTelemetry.MessagingSystem).ShouldBe("nats");
    }

    [Test]
    public async Task PublishAsync_ShouldSetDestinationNameTag()
    {
        // Arrange
        var subject = $"payments.processed.{Guid.NewGuid():N}";
        var message = new TestMessage("test-data");
        var messageId = "payment-789-processed";
        SetupPublishMock();
        var startCount = _capturedActivities.Count;

        // Act
        await _bus.PublishAsync(subject, message, messageId);

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
        var message = new TestMessage("test-data");
        var messageId = "test-msg-1";
        SetupPublishMock();
        var startCount = _capturedActivities.Count;

        // Act
        await _bus.PublishAsync(subject, message, messageId);

        // Assert
        var activity = _capturedActivities.Skip(startCount).FirstOrDefault(a => a.OperationName.Contains(subject));
        activity.ShouldNotBeNull();
        activity.GetTagItem(NatsTelemetry.MessagingOperation).ShouldBe("publish");
    }

    [Test]
    public async Task PublishAsync_ShouldSetMessageIdTag()
    {
        // Arrange
        var subject = $"orders.shipped.{Guid.NewGuid():N}";
        var message = new TestMessage("test-data");
        var messageId = $"order-100-shipped-{Guid.NewGuid():N}";
        SetupPublishMock();
        var startCount = _capturedActivities.Count;

        // Act
        await _bus.PublishAsync(subject, message, messageId);

        // Assert
        var activity = _capturedActivities.Skip(startCount).FirstOrDefault(a => a.OperationName.Contains(subject));
        activity.ShouldNotBeNull();
        activity.GetTagItem(NatsTelemetry.MessagingMessageId).ShouldBe(messageId);
    }

    [Test]
    public async Task PublishAsync_ShouldInjectTraceContextIntoHeaders()
    {
        // Arrange
        var subject = $"test.subject.{Guid.NewGuid():N}";
        var message = new TestMessage("test-data");
        var messageId = "test-msg-1";
        NatsHeaders? capturedHeaders = null;
        SetupPublishMock(headers => capturedHeaders = headers);

        // Act
        await _bus.PublishAsync(subject, message, messageId);

        // Assert
        capturedHeaders.ShouldNotBeNull();
        capturedHeaders.ShouldContainKey("traceparent");
        var traceparent = capturedHeaders["traceparent"].ToString();
        traceparent.ShouldStartWith("00-");
    }

    [Test]
    public async Task PublishAsync_ShouldSetNatsMsgIdHeader()
    {
        // Arrange
        var subject = $"test.subject.{Guid.NewGuid():N}";
        var message = new TestMessage("test-data");
        var messageId = $"idempotency-key-{Guid.NewGuid():N}";
        NatsHeaders? capturedHeaders = null;
        SetupPublishMock(headers => capturedHeaders = headers);

        // Act
        await _bus.PublishAsync(subject, message, messageId);

        // Assert
        capturedHeaders.ShouldNotBeNull();
        capturedHeaders.ShouldContainKey("Nats-Msg-Id");
        capturedHeaders["Nats-Msg-Id"].ToString().ShouldBe(messageId);
    }

    [Test]
    public async Task PublishAsync_ShouldSetContentTypeHeader()
    {
        // Arrange
        var subject = $"test.subject.{Guid.NewGuid():N}";
        var message = new TestMessage("test-data");
        var messageId = "test-msg-1";
        NatsHeaders? capturedHeaders = null;
        SetupPublishMock(headers => capturedHeaders = headers);

        // Act
        await _bus.PublishAsync(subject, message, messageId);

        // Assert
        capturedHeaders.ShouldNotBeNull();
        capturedHeaders.ShouldContainKey("Content-Type");
        capturedHeaders["Content-Type"].ToString().ShouldBe("application/json");
    }

    [Test]
    public async Task PublishAsync_WithCustomHeaders_ShouldMergeAllHeaders()
    {
        // Arrange
        var subject = $"test.subject.{Guid.NewGuid():N}";
        var message = new TestMessage("test-data");
        var messageId = "test-msg-1";
        var customHeaders = new MessageHeaders(new Dictionary<string, string>
        {
            ["X-Correlation-Id"] = "corr-123",
            ["X-Tenant-Id"] = "tenant-abc"
        });
        NatsHeaders? capturedHeaders = null;
        SetupPublishMock(headers => capturedHeaders = headers);

        // Act
        await _bus.PublishAsync(subject, message, messageId, customHeaders);

        // Assert
        capturedHeaders.ShouldNotBeNull();
        capturedHeaders.ShouldContainKey("traceparent");
        capturedHeaders.ShouldContainKey("Nats-Msg-Id");
        capturedHeaders.ShouldContainKey("X-Correlation-Id");
        capturedHeaders["X-Correlation-Id"].ToString().ShouldBe("corr-123");
        capturedHeaders.ShouldContainKey("X-Tenant-Id");
        capturedHeaders["X-Tenant-Id"].ToString().ShouldBe("tenant-abc");
    }

    #endregion

    #region Publish Validation Tests

    [Test]
    public async Task PublishAsync_WithoutMessageId_ShouldThrowArgumentException()
    {
        // Arrange
        var subject = "test.subject";
        var message = new TestMessage("test");

        // Act & Assert
        await Should.ThrowAsync<ArgumentException>(
            async () => await _bus.PublishAsync(subject, message, (string?)null)
        );
    }

    [Test]
    public async Task PublishAsync_WithEmptyMessageId_ShouldThrowArgumentException()
    {
        // Arrange
        var subject = "test.subject";
        var message = new TestMessage("test");

        // Act & Assert
        await Should.ThrowAsync<ArgumentException>(
            async () => await _bus.PublishAsync(subject, message, "")
        );
    }

    [Test]
    public async Task PublishAsync_WithWhitespaceMessageId_ShouldThrowArgumentException()
    {
        // Arrange
        var subject = "test.subject";
        var message = new TestMessage("test");

        // Act & Assert
        await Should.ThrowAsync<ArgumentException>(
            async () => await _bus.PublishAsync(subject, message, "   ")
        );
    }

    #endregion

    #region Activity Tracing Tests

    [Test]
    public async Task PublishAsync_TraceparentShouldMatchActivityId()
    {
        // Arrange
        var subject = $"test.subject.{Guid.NewGuid():N}";
        var message = new TestMessage("test-data");
        var messageId = "test-msg-1";
        NatsHeaders? capturedHeaders = null;
        SetupPublishMock(headers => capturedHeaders = headers);
        var startCount = _capturedActivities.Count;

        // Act
        await _bus.PublishAsync(subject, message, messageId);

        // Assert
        var activity = _capturedActivities.Skip(startCount).FirstOrDefault(a => a.OperationName.Contains(subject));
        activity.ShouldNotBeNull();
        capturedHeaders.ShouldNotBeNull();

        var traceparent = capturedHeaders["traceparent"].ToString();
        traceparent.ShouldContain(activity.TraceId.ToHexString());
        traceparent.ShouldContain(activity.SpanId.ToHexString());
    }

    [Test]
    public async Task PublishAsync_MultiplePublishes_ShouldHaveSameTraceIdWhenInSameParentContext()
    {
        // Arrange
        var prefix = Guid.NewGuid().ToString("N");
        var subject1 = $"subject1.{prefix}";
        var subject2 = $"subject2.{prefix}";
        var message = new TestMessage("test-data");
        SetupPublishMock();

        // Create a parent activity to establish a trace context
        using var parentActivity = NatsTelemetry.ActivitySource.StartActivity($"parent-operation.{prefix}", ActivityKind.Internal);
        parentActivity.ShouldNotBeNull();
        var startCount = _capturedActivities.Count;

        // Act
        await _bus.PublishAsync(subject1, message, "msg-1");
        await _bus.PublishAsync(subject2, message, "msg-2");

        // Assert - find our specific activities
        var ourActivities = _capturedActivities.Skip(startCount).Where(a => a.OperationName.Contains(prefix) && a.OperationName.Contains("publish")).ToList();
        ourActivities.Count.ShouldBe(2);
        ourActivities[0].TraceId.ShouldBe(parentActivity.TraceId);
        ourActivities[1].TraceId.ShouldBe(parentActivity.TraceId);

        // But they should have different SpanIds
        ourActivities[0].SpanId.ShouldNotBe(ourActivities[1].SpanId);
    }

    [Test]
    public async Task PublishAsync_WithoutParentActivity_ShouldCreateNewTrace()
    {
        // Arrange
        var prefix = Guid.NewGuid().ToString("N");
        var subject1 = $"subject1.{prefix}";
        var subject2 = $"subject2.{prefix}";
        var message = new TestMessage("test-data");
        SetupPublishMock();
        var startCount = _capturedActivities.Count;

        // Act - publish without a parent activity
        await _bus.PublishAsync(subject1, message, "msg-1");
        await _bus.PublishAsync(subject2, message, "msg-2");

        // Assert - find our specific activities
        var ourActivities = _capturedActivities.Skip(startCount).Where(a => a.OperationName.Contains(prefix)).ToList();
        ourActivities.Count.ShouldBe(2);
        ourActivities[0].TraceId.ShouldNotBe(ourActivities[1].TraceId);
    }

    #endregion
}

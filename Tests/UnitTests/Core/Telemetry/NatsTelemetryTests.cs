using System.Diagnostics;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Telemetry;
using NATS.Client.Core;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Core.Telemetry;

[Property("nTag", "Core")]
public class NatsTelemetryTests : IDisposable
{
    private readonly ActivityListener _listener;
    private readonly List<Activity> _capturedActivities = [];

    public NatsTelemetryTests()
    {
        // Set up an ActivityListener to capture activities from FlySwattr.NATS
        _listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == NatsTelemetry.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStarted = activity => _capturedActivities.Add(activity)
        };
        ActivitySource.AddActivityListener(_listener);
    }

    public void Dispose()
    {
        _listener.Dispose();
        _capturedActivities.Clear();
    }

    #region ActivitySource Tests

    [Test]
    public void ActivitySourceName_ShouldBeFlySwattrNats()
    {
        NatsTelemetry.ActivitySourceName.ShouldBe("FlySwattr.NATS");
    }

    [Test]
    public void ActivitySource_ShouldBeCreatedWithCorrectName()
    {
        NatsTelemetry.ActivitySource.ShouldNotBeNull();
        NatsTelemetry.ActivitySource.Name.ShouldBe("FlySwattr.NATS");
    }

    [Test]
    public void StartActivity_ShouldCreateActivityWithCorrectName()
    {
        // Act
        using var activity = NatsTelemetry.ActivitySource.StartActivity("test.subject publish", ActivityKind.Producer);

        // Assert
        activity.ShouldNotBeNull();
        activity.OperationName.ShouldBe("test.subject publish");
        activity.Kind.ShouldBe(ActivityKind.Producer);
    }

    [Test]
    public void StartActivity_Consumer_ShouldHaveCorrectKind()
    {
        // Act
        using var activity = NatsTelemetry.ActivitySource.StartActivity("test.subject receive", ActivityKind.Consumer);

        // Assert
        activity.ShouldNotBeNull();
        activity.Kind.ShouldBe(ActivityKind.Consumer);
    }

    [Test]
    public void StartActivity_Client_ShouldHaveCorrectKind()
    {
        // Act
        using var activity = NatsTelemetry.ActivitySource.StartActivity("test.subject request", ActivityKind.Client);

        // Assert
        activity.ShouldNotBeNull();
        activity.Kind.ShouldBe(ActivityKind.Client);
    }

    #endregion

    #region Semantic Convention Constants Tests

    [Test]
    public void MessagingSystem_ShouldFollowSemanticConventions()
    {
        NatsTelemetry.MessagingSystem.ShouldBe("messaging.system");
    }

    [Test]
    public void MessagingSystemName_ShouldBeNats()
    {
        NatsTelemetry.MessagingSystemName.ShouldBe("nats");
    }

    [Test]
    public void MessagingDestinationName_ShouldFollowSemanticConventions()
    {
        NatsTelemetry.MessagingDestinationName.ShouldBe("messaging.destination.name");
    }

    [Test]
    public void MessagingOperation_ShouldFollowSemanticConventions()
    {
        NatsTelemetry.MessagingOperation.ShouldBe("messaging.operation");
    }

    [Test]
    public void MessagingMessageId_ShouldFollowSemanticConventions()
    {
        NatsTelemetry.MessagingMessageId.ShouldBe("messaging.message.id");
    }

    [Test]
    public void MessagingConversationId_ShouldFollowSemanticConventions()
    {
        NatsTelemetry.MessagingConversationId.ShouldBe("messaging.conversation_id");
    }

    [Test]
    public void NatsStream_ShouldHaveCorrectAttributeName()
    {
        NatsTelemetry.NatsStream.ShouldBe("messaging.nats.stream");
    }

    [Test]
    public void NatsConsumer_ShouldHaveCorrectAttributeName()
    {
        NatsTelemetry.NatsConsumer.ShouldBe("messaging.nats.consumer");
    }

    [Test]
    public void NatsSubject_ShouldHaveCorrectAttributeName()
    {
        NatsTelemetry.NatsSubject.ShouldBe("messaging.nats.subject");
    }

    #endregion

    #region InjectTraceContext Tests

    [Test]
    public void InjectTraceContext_WithNullActivity_ShouldNotModifyHeaders()
    {
        // Arrange
        var headers = new NatsHeaders();

        // Act
        NatsTelemetry.InjectTraceContext(null, headers);

        // Assert
        headers.ShouldBeEmpty();
    }

    [Test]
    public void InjectTraceContext_WithActivityWithNoId_ShouldNotModifyHeaders()
    {
        // Arrange
        var headers = new NatsHeaders();
        // Create an activity that won't be sampled (no listener for this source)
        using var tempSource = new ActivitySource("temp-test-source");
        using var activity = tempSource.StartActivity("test"); // Will be null without listener

        // Act
        NatsTelemetry.InjectTraceContext(activity, headers);

        // Assert
        headers.ShouldBeEmpty();
    }

    [Test]
    public void InjectTraceContext_WithValidActivity_ShouldInjectTraceparent()
    {
        // Arrange
        var headers = new NatsHeaders();
        using var activity = NatsTelemetry.ActivitySource.StartActivity("test", ActivityKind.Producer);
        activity.ShouldNotBeNull();

        // Act
        NatsTelemetry.InjectTraceContext(activity, headers);

        // Assert
        headers.ShouldContainKey("traceparent");
        headers["traceparent"].ToString().ShouldNotBeNullOrWhiteSpace();
    }

    [Test]
    public void InjectTraceContext_TraceparentFormat_ShouldFollowW3CFormat()
    {
        // Arrange
        var headers = new NatsHeaders();
        using var activity = NatsTelemetry.ActivitySource.StartActivity("test", ActivityKind.Producer);
        activity.ShouldNotBeNull();

        // Act
        NatsTelemetry.InjectTraceContext(activity, headers);

        // Assert
        var traceparent = headers["traceparent"].ToString();
        // W3C format: version-traceid-parentid-traceflags (e.g., "00-abc123...-def456...-01")
        traceparent.ShouldStartWith("00-"); // Version 00
        var parts = traceparent!.Split('-');
        parts.Length.ShouldBe(4);
        parts[0].ShouldBe("00"); // Version
        parts[1].Length.ShouldBe(32); // TraceId (16 bytes = 32 hex chars)
        parts[2].Length.ShouldBe(16); // SpanId (8 bytes = 16 hex chars)
    }

    [Test]
    public void InjectTraceContext_WithTraceState_ShouldInjectTracestate()
    {
        // Arrange
        var headers = new NatsHeaders();
        using var activity = NatsTelemetry.ActivitySource.StartActivity("test", ActivityKind.Producer);
        activity.ShouldNotBeNull();
        activity.TraceStateString = "vendor1=value1,vendor2=value2";

        // Act
        NatsTelemetry.InjectTraceContext(activity, headers);

        // Assert
        headers.ShouldContainKey("tracestate");
        headers["tracestate"].ToString().ShouldBe("vendor1=value1,vendor2=value2");
    }

    [Test]
    public void InjectTraceContext_WithoutTraceState_ShouldNotInjectTracestate()
    {
        // Arrange
        var headers = new NatsHeaders();
        using var activity = NatsTelemetry.ActivitySource.StartActivity("test", ActivityKind.Producer);
        activity.ShouldNotBeNull();
        // TraceStateString is null by default

        // Act
        NatsTelemetry.InjectTraceContext(activity, headers);

        // Assert
        headers.ShouldContainKey("traceparent");
        headers.ShouldNotContainKey("tracestate");
    }

    #endregion

    #region ExtractTraceContext (NatsHeaders) Tests

    [Test]
    public void ExtractTraceContext_NatsHeaders_WithNullHeaders_ShouldReturnDefault()
    {
        // Act
        var context = NatsTelemetry.ExtractTraceContext((NatsHeaders?)null);

        // Assert
        context.ShouldBe(default(ActivityContext));
        context.TraceId.ShouldBe(default(ActivityTraceId));
        context.SpanId.ShouldBe(default(ActivitySpanId));
    }

    [Test]
    public void ExtractTraceContext_NatsHeaders_WithEmptyHeaders_ShouldReturnDefault()
    {
        // Arrange
        var headers = new NatsHeaders();

        // Act
        var context = NatsTelemetry.ExtractTraceContext(headers);

        // Assert
        context.ShouldBe(default(ActivityContext));
    }

    [Test]
    public void ExtractTraceContext_NatsHeaders_WithValidTraceparent_ShouldReturnContext()
    {
        // Arrange
        var headers = new NatsHeaders
        {
            ["traceparent"] = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        };

        // Act
        var context = NatsTelemetry.ExtractTraceContext(headers);

        // Assert
        context.TraceId.ToHexString().ShouldBe("0af7651916cd43dd8448eb211c80319c");
        context.SpanId.ToHexString().ShouldBe("b7ad6b7169203331");
        context.TraceFlags.ShouldBe(ActivityTraceFlags.Recorded);
    }

    [Test]
    public void ExtractTraceContext_NatsHeaders_WithTraceparentAndTracestate_ShouldReturnFullContext()
    {
        // Arrange
        var headers = new NatsHeaders
        {
            ["traceparent"] = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            ["tracestate"] = "vendor1=value1"
        };

        // Act
        var context = NatsTelemetry.ExtractTraceContext(headers);

        // Assert
        context.TraceId.ToHexString().ShouldBe("0af7651916cd43dd8448eb211c80319c");
        context.SpanId.ToHexString().ShouldBe("b7ad6b7169203331");
        context.TraceState.ShouldBe("vendor1=value1");
    }

    [Test]
    public void ExtractTraceContext_NatsHeaders_WithInvalidTraceparent_ShouldReturnDefault()
    {
        // Arrange
        var headers = new NatsHeaders
        {
            ["traceparent"] = "invalid-traceparent-format"
        };

        // Act
        var context = NatsTelemetry.ExtractTraceContext(headers);

        // Assert
        context.ShouldBe(default(ActivityContext));
    }

    [Test]
    public void ExtractTraceContext_NatsHeaders_WithMalformedTraceparent_ShouldReturnDefault()
    {
        // Arrange - missing parts
        var headers = new NatsHeaders
        {
            ["traceparent"] = "00-abc123"
        };

        // Act
        var context = NatsTelemetry.ExtractTraceContext(headers);

        // Assert
        context.ShouldBe(default(ActivityContext));
    }

    #endregion

    #region ExtractTraceContext (MessageHeaders) Tests

    [Test]
    public void ExtractTraceContext_MessageHeaders_WithNullHeaders_ShouldReturnDefault()
    {
        // Act
        var context = NatsTelemetry.ExtractTraceContext((MessageHeaders?)null);

        // Assert
        context.ShouldBe(default(ActivityContext));
    }

    [Test]
    public void ExtractTraceContext_MessageHeaders_WithEmptyHeaders_ShouldReturnDefault()
    {
        // Arrange
        var headers = MessageHeaders.Empty;

        // Act
        var context = NatsTelemetry.ExtractTraceContext(headers);

        // Assert
        context.ShouldBe(default(ActivityContext));
    }

    [Test]
    public void ExtractTraceContext_MessageHeaders_WithValidTraceparent_ShouldReturnContext()
    {
        // Arrange
        var headers = new MessageHeaders(new Dictionary<string, string>
        {
            ["traceparent"] = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        });

        // Act
        var context = NatsTelemetry.ExtractTraceContext(headers);

        // Assert
        context.TraceId.ToHexString().ShouldBe("0af7651916cd43dd8448eb211c80319c");
        context.SpanId.ToHexString().ShouldBe("b7ad6b7169203331");
    }

    [Test]
    public void ExtractTraceContext_MessageHeaders_WithTraceparentAndTracestate_ShouldReturnFullContext()
    {
        // Arrange
        var headers = new MessageHeaders(new Dictionary<string, string>
        {
            ["traceparent"] = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            ["tracestate"] = "vendor1=value1,vendor2=value2"
        });

        // Act
        var context = NatsTelemetry.ExtractTraceContext(headers);

        // Assert
        context.TraceState.ShouldBe("vendor1=value1,vendor2=value2");
    }

    [Test]
    public void ExtractTraceContext_MessageHeaders_WithInvalidTraceparent_ShouldReturnDefault()
    {
        // Arrange
        var headers = new MessageHeaders(new Dictionary<string, string>
        {
            ["traceparent"] = "not-valid"
        });

        // Act
        var context = NatsTelemetry.ExtractTraceContext(headers);

        // Assert
        context.ShouldBe(default(ActivityContext));
    }

    #endregion

    #region Round-Trip Tests (Inject then Extract)

    [Test]
    public void InjectThenExtract_NatsHeaders_ShouldPreserveTraceId()
    {
        // Arrange
        var headers = new NatsHeaders();
        using var activity = NatsTelemetry.ActivitySource.StartActivity("test", ActivityKind.Producer);
        activity.ShouldNotBeNull();

        // Act
        NatsTelemetry.InjectTraceContext(activity, headers);
        var extractedContext = NatsTelemetry.ExtractTraceContext(headers);

        // Assert
        extractedContext.TraceId.ShouldBe(activity.TraceId);
    }

    [Test]
    public void InjectThenExtract_NatsHeaders_ShouldPreserveSpanId()
    {
        // Arrange
        var headers = new NatsHeaders();
        using var activity = NatsTelemetry.ActivitySource.StartActivity("test", ActivityKind.Producer);
        activity.ShouldNotBeNull();

        // Act
        NatsTelemetry.InjectTraceContext(activity, headers);
        var extractedContext = NatsTelemetry.ExtractTraceContext(headers);

        // Assert
        extractedContext.SpanId.ShouldBe(activity.SpanId);
    }

    [Test]
    public void InjectThenExtract_NatsHeaders_ShouldPreserveTraceState()
    {
        // Arrange
        var headers = new NatsHeaders();
        using var activity = NatsTelemetry.ActivitySource.StartActivity("test", ActivityKind.Producer);
        activity.ShouldNotBeNull();
        activity.TraceStateString = "key1=value1,key2=value2";

        // Act
        NatsTelemetry.InjectTraceContext(activity, headers);
        var extractedContext = NatsTelemetry.ExtractTraceContext(headers);

        // Assert
        extractedContext.TraceState.ShouldBe("key1=value1,key2=value2");
    }

    #endregion

    #region Parent-Child Activity Relationship Tests

    [Test]
    public void ChildActivity_WithExtractedContext_ShouldHaveCorrectParent()
    {
        // Arrange - Simulate producer creating parent activity
        var headers = new NatsHeaders();
        using var parentActivity = NatsTelemetry.ActivitySource.StartActivity("producer publish", ActivityKind.Producer);
        parentActivity.ShouldNotBeNull();
        NatsTelemetry.InjectTraceContext(parentActivity, headers);

        // Act - Simulate consumer extracting context and creating child activity
        var parentContext = NatsTelemetry.ExtractTraceContext(headers);
        using var childActivity = NatsTelemetry.ActivitySource.StartActivity("consumer receive", ActivityKind.Consumer, parentContext);
        childActivity.ShouldNotBeNull();

        // Assert - Child should share the same TraceId
        childActivity.TraceId.ShouldBe(parentActivity.TraceId);
        // Child's ParentSpanId should be the parent's SpanId
        childActivity.ParentSpanId.ShouldBe(parentActivity.SpanId);
        // Child should have its own unique SpanId
        childActivity.SpanId.ShouldNotBe(parentActivity.SpanId);
    }

    [Test]
    public void ChildActivity_ShouldInheritTraceFlags()
    {
        // Arrange
        var headers = new NatsHeaders();
        using var parentActivity = NatsTelemetry.ActivitySource.StartActivity("parent", ActivityKind.Producer);
        parentActivity.ShouldNotBeNull();
        NatsTelemetry.InjectTraceContext(parentActivity, headers);

        // Act
        var parentContext = NatsTelemetry.ExtractTraceContext(headers);
        using var childActivity = NatsTelemetry.ActivitySource.StartActivity("child", ActivityKind.Consumer, parentContext);
        childActivity.ShouldNotBeNull();

        // Assert
        childActivity.ActivityTraceFlags.ShouldBe(parentActivity.ActivityTraceFlags);
    }

    #endregion

    #region Activity Tags Tests

    [Test]
    public void Activity_SetTag_ShouldStoreTagCorrectly()
    {
        // Arrange
        using var activity = NatsTelemetry.ActivitySource.StartActivity("test", ActivityKind.Producer);
        activity.ShouldNotBeNull();

        // Act
        activity.SetTag(NatsTelemetry.MessagingSystem, NatsTelemetry.MessagingSystemName);
        activity.SetTag(NatsTelemetry.MessagingDestinationName, "orders.created");
        activity.SetTag(NatsTelemetry.MessagingOperation, "publish");

        // Assert
        activity.GetTagItem(NatsTelemetry.MessagingSystem).ShouldBe("nats");
        activity.GetTagItem(NatsTelemetry.MessagingDestinationName).ShouldBe("orders.created");
        activity.GetTagItem(NatsTelemetry.MessagingOperation).ShouldBe("publish");
    }

    [Test]
    public void Activity_SetNatsSpecificTags_ShouldStoreCorrectly()
    {
        // Arrange
        using var activity = NatsTelemetry.ActivitySource.StartActivity("test", ActivityKind.Consumer);
        activity.ShouldNotBeNull();

        // Act
        activity.SetTag(NatsTelemetry.NatsStream, "ORDERS");
        activity.SetTag(NatsTelemetry.NatsConsumer, "orders-processor");
        activity.SetTag(NatsTelemetry.NatsSubject, "orders.created");

        // Assert
        activity.GetTagItem(NatsTelemetry.NatsStream).ShouldBe("ORDERS");
        activity.GetTagItem(NatsTelemetry.NatsConsumer).ShouldBe("orders-processor");
        activity.GetTagItem(NatsTelemetry.NatsSubject).ShouldBe("orders.created");
    }

    [Test]
    public void Activity_SetMessageId_ShouldStoreCorrectly()
    {
        // Arrange
        using var activity = NatsTelemetry.ActivitySource.StartActivity("test", ActivityKind.Producer);
        activity.ShouldNotBeNull();
        var messageId = "order-123-created";

        // Act
        activity.SetTag(NatsTelemetry.MessagingMessageId, messageId);

        // Assert
        activity.GetTagItem(NatsTelemetry.MessagingMessageId).ShouldBe("order-123-created");
    }

    [Test]
    public void Activity_SetConversationId_ShouldStoreReplyTo()
    {
        // Arrange
        using var activity = NatsTelemetry.ActivitySource.StartActivity("test", ActivityKind.Client);
        activity.ShouldNotBeNull();
        var replyTo = "_INBOX.abc123";

        // Act
        activity.SetTag(NatsTelemetry.MessagingConversationId, replyTo);

        // Assert
        activity.GetTagItem(NatsTelemetry.MessagingConversationId).ShouldBe("_INBOX.abc123");
    }

    #endregion

    #region Activity Status Tests

    [Test]
    public void Activity_SetStatusError_ShouldSetErrorStatus()
    {
        // Arrange
        using var activity = NatsTelemetry.ActivitySource.StartActivity("test", ActivityKind.Consumer);
        activity.ShouldNotBeNull();

        // Act
        activity.SetStatus(ActivityStatusCode.Error, "Handler failed");

        // Assert
        activity.Status.ShouldBe(ActivityStatusCode.Error);
        activity.StatusDescription.ShouldBe("Handler failed");
    }

    [Test]
    public void Activity_SetStatusOk_ShouldSetOkStatus()
    {
        // Arrange
        using var activity = NatsTelemetry.ActivitySource.StartActivity("test", ActivityKind.Consumer);
        activity.ShouldNotBeNull();

        // Act
        activity.SetStatus(ActivityStatusCode.Ok);

        // Assert
        activity.Status.ShouldBe(ActivityStatusCode.Ok);
    }

    #endregion

    #region Captured Activities Tests

    [Test]
    public void ActivityListener_ShouldCaptureActivities()
    {
        // Arrange
        var operationName = $"captured.test.{Guid.NewGuid():N}";
        var startCount = _capturedActivities.Count;

        // Act
        using (var activity = NatsTelemetry.ActivitySource.StartActivity(operationName, ActivityKind.Producer))
        {
            activity.ShouldNotBeNull();
        }

        // Assert - Find our specific activity
        var ourActivity = _capturedActivities.Skip(startCount).FirstOrDefault(a => a.OperationName == operationName);
        ourActivity.ShouldNotBeNull();
    }

    [Test]
    public void ActivityListener_ShouldCaptureNestedActivities()
    {
        // Arrange
        var prefix = Guid.NewGuid().ToString("N");
        var parentName = $"parent.{prefix}";
        var childName = $"child.{prefix}";
        var startCount = _capturedActivities.Count;

        // Act
        using (var parent = NatsTelemetry.ActivitySource.StartActivity(parentName, ActivityKind.Producer))
        {
            parent.ShouldNotBeNull();
            using (var child = NatsTelemetry.ActivitySource.StartActivity(childName, ActivityKind.Consumer))
            {
                child.ShouldNotBeNull();
            }
        }

        // Assert - Find our specific activities
        var ourActivities = _capturedActivities.Skip(startCount).Where(a => a.OperationName.Contains(prefix)).ToList();
        ourActivities.Count.ShouldBe(2);
        ourActivities.ShouldContain(a => a.OperationName == parentName);
        ourActivities.ShouldContain(a => a.OperationName == childName);
    }

    #endregion
}

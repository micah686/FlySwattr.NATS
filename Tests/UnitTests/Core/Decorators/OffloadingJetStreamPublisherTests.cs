using System.Buffers;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core;
using FlySwattr.NATS.Core.Configuration;
using FlySwattr.NATS.Core.Decorators;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Core.Decorators;

[Property("nTag", "Core")]
public class OffloadingJetStreamPublisherTests
{
    private readonly IJetStreamPublisher _innerPublisher;
    private readonly IRawJetStreamPublisher _rawPublisher;
    private readonly IObjectStore _objectStore;
    private readonly IMessageSerializer _serializer;
    private readonly ILogger<OffloadingJetStreamPublisher> _logger;
    private readonly PayloadOffloadingOptions _options;

    public OffloadingJetStreamPublisherTests()
    {
        _innerPublisher = Substitute.For<IJetStreamPublisher, IRawJetStreamPublisher>();
        _rawPublisher = (IRawJetStreamPublisher)_innerPublisher;
        _objectStore = Substitute.For<IObjectStore>();
        _serializer = Substitute.For<IMessageSerializer>();
        _logger = Substitute.For<ILogger<OffloadingJetStreamPublisher>>();
        _options = new PayloadOffloadingOptions
        {
            ThresholdBytes = 1000,
            ObjectKeyPrefix = "claimcheck"
        };
    }

    private OffloadingJetStreamPublisher CreateSut()
        => new(_innerPublisher, _rawPublisher, _objectStore, _serializer, Options.Create(_options), _logger);

    private void WritePayload<T>(int size)
    {
        _serializer.When(s => s.Serialize(Arg.Any<IBufferWriter<byte>>(), Arg.Any<T>()))
            .Do(callInfo =>
            {
                var writer = callInfo.Arg<IBufferWriter<byte>>();
                var span = writer.GetSpan(size);
                span[..size].Fill(0x01);
                writer.Advance(size);
            });
    }

    [Test]
    public async Task PublishAsync_ShouldPassThrough_WhenPayloadBelowThreshold()
    {
        WritePayload<TestMessage>(_options.ThresholdBytes - 1);
        var sut = CreateSut();
        var message = new TestMessage { Data = "small" };

        await sut.PublishAsync("test.subject", message, "msg-1");

        await _innerPublisher.Received(1).PublishAsync("test.subject", message, "msg-1", Arg.Any<MessageHeaders?>(), Arg.Any<CancellationToken>());
        await _rawPublisher.DidNotReceive().PublishRawAsync(Arg.Any<string>(), Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<string?>(), Arg.Any<MessageHeaders?>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PublishAsync_ShouldOffloadAndPublishHeaderOnlyMessage_WhenPayloadExceedsThreshold()
    {
        WritePayload<TestMessage>(_options.ThresholdBytes + 1);
        _serializer.GetContentType<TestMessage>().Returns("application/json");
        var sut = CreateSut();

        await sut.PublishAsync("test.subject", new TestMessage { Data = "large" }, "msg-1");

        await _objectStore.Received(1).PutAsync(
            Arg.Is<string>(key => key.StartsWith("claimcheck/test.subject/")),
            Arg.Any<Stream>(),
            Arg.Any<CancellationToken>());

        await _rawPublisher.Received(1).PublishRawAsync(
            "test.subject",
            Arg.Is<ReadOnlyMemory<byte>>(payload => payload.IsEmpty),
            "msg-1",
            Arg.Is<MessageHeaders>(headers =>
                headers.Headers["Content-Type"] == "application/json" &&
                headers.Headers[_options.ClaimCheckHeaderName].StartsWith("objstore://claimcheck/test.subject/")),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PublishAsync_ShouldDeleteOrphanedObject_WhenRawPublishFails()
    {
        WritePayload<TestMessage>(_options.ThresholdBytes + 1);
        var sut = CreateSut();
        string? objectKey = null;

        _objectStore.PutAsync(Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                objectKey = callInfo.Arg<string>();
                return Task.FromResult(objectKey);
            });

        _rawPublisher.PublishRawAsync(
            Arg.Any<string>(),
            Arg.Any<ReadOnlyMemory<byte>>(),
            Arg.Any<string?>(),
            Arg.Any<MessageHeaders?>(),
            Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("publish failed"));

        var ex = await Should.ThrowAsync<InvalidOperationException>(() =>
            sut.PublishAsync("test.subject", new TestMessage { Data = "large" }, "msg-1"));

        ex.Message.ShouldBe("publish failed");
        await _objectStore.Received(1).DeleteAsync(objectKey!, Arg.Any<CancellationToken>());
    }

    public sealed class TestMessage
    {
        public string Data { get; set; } = string.Empty;
    }
}

using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core;
using FlySwattr.NATS.Core.Configuration;
using FlySwattr.NATS.Core.Decorators;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Core.Decorators;

[Property("nTag", "Core")]
public class OffloadingJetStreamConsumerTests
{
    private readonly IRawJetStreamConsumer _rawConsumer;
    private readonly IObjectStore _objectStore;
    private readonly IMessageSerializer _serializer;
    private readonly ILogger<OffloadingJetStreamConsumer> _logger;
    private readonly PayloadOffloadingOptions _options;

    public OffloadingJetStreamConsumerTests()
    {
        _rawConsumer = Substitute.For<IRawJetStreamConsumer>();
        _objectStore = Substitute.For<IObjectStore>();
        _serializer = Substitute.For<IMessageSerializer>();
        _logger = Substitute.For<ILogger<OffloadingJetStreamConsumer>>();
        _options = new PayloadOffloadingOptions { ClaimCheckHeaderName = "X-ClaimCheck-Ref" };
    }

    private OffloadingJetStreamConsumer CreateSut()
        => new(_rawConsumer, _objectStore, _serializer, Options.Create(_options), _logger);

    [Test]
    public async Task ConsumeAsync_ShouldUseRawPipeline()
    {
        var sut = CreateSut();

        await sut.ConsumeAsync<TestMessage>(
            StreamName.From("stream"),
            SubjectName.From("subject"),
            _ => Task.CompletedTask);

        await _rawConsumer.Received(1).ConsumeRawAsync(
            StreamName.From("stream"),
            SubjectName.From("subject"),
            Arg.Any<Func<IJsMessageContext<byte[]>, Task>>(),
            Arg.Any<JetStreamConsumeOptions?>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ResolveClaimCheckAsync_ShouldHydrateFromHeaderBeforeDeserializingBusinessType()
    {
        var sut = CreateSut();
        var objectKey = "claimcheck/subject/abc123";
        var payload = new byte[] { 1, 2, 3 };
        var expected = new TestMessage { Data = "resolved" };
        var context = new TestByteContext(Array.Empty<byte>(), new Dictionary<string, string>
        {
            [_options.ClaimCheckHeaderName] = $"objstore://{objectKey}"
        });

        _objectStore.GetAsync(objectKey, Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var stream = callInfo.ArgAt<Stream>(1);
                stream.Write(payload);
                return Task.CompletedTask;
            });
        _serializer.Deserialize<TestMessage>(Arg.Is<ReadOnlyMemory<byte>>(m => m.ToArray().SequenceEqual(payload))).Returns(expected);

        var resolved = await sut.ResolveClaimCheckAsync<TestMessage>(context, CancellationToken.None);

        resolved.Message.ShouldBe(expected);
        await _objectStore.Received(1).GetAsync(objectKey, Arg.Any<Stream>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ResolveClaimCheckAsync_ShouldDeserializeInlinePayload_WhenNoClaimCheckHeaderPresent()
    {
        var sut = CreateSut();
        var payload = new byte[] { 4, 5, 6 };
        var expected = new TestMessage { Data = "inline" };
        var context = new TestByteContext(payload);

        _serializer.Deserialize<TestMessage>(Arg.Is<ReadOnlyMemory<byte>>(m => m.ToArray().SequenceEqual(payload))).Returns(expected);

        var resolved = await sut.ResolveClaimCheckAsync<TestMessage>(context, CancellationToken.None);

        resolved.Message.ShouldBe(expected);
        await _objectStore.DidNotReceive().GetAsync(Arg.Any<string>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ResolveClaimCheckAsync_ShouldRejectPathTraversalObjectKey()
    {
        var sut = CreateSut();
        var context = new TestByteContext(Array.Empty<byte>(), new Dictionary<string, string>
        {
            [_options.ClaimCheckHeaderName] = "objstore://../secrets"
        });

        await Should.ThrowAsync<ArgumentException>(() =>
            sut.ResolveClaimCheckAsync<TestMessage>(context, CancellationToken.None));
    }

    private sealed class TestByteContext : IJsMessageContext<byte[]>
    {
        public TestByteContext(byte[] message, Dictionary<string, string>? headers = null)
        {
            Message = message;
            Headers = headers == null ? MessageHeaders.Empty : new MessageHeaders(headers);
        }

        public byte[] Message { get; }
        public string Subject => "subject";
        public MessageHeaders Headers { get; }
        public string? ReplyTo => null;
        public ulong Sequence => 1;
        public DateTimeOffset Timestamp => DateTimeOffset.UtcNow;
        public bool Redelivered => false;
        public uint NumDelivered => 1;
        public Task AckAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task NackAsync(TimeSpan? delay = null, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task TermAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task InProgressAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task RespondAsync<TResponse>(TResponse response, CancellationToken cancellationToken = default) => Task.CompletedTask;
    }

    public sealed class TestMessage
    {
        public string Data { get; set; } = string.Empty;
    }
}

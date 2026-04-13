using System.Buffers;
using FlySwattr.NATS.Core.Serializers;
using MemoryPack;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Core.Decorators;

[Property("nTag", "Core")]
public class OffloadingJetStreamConsumerSecurityTests
{
    private readonly HybridNatsSerializer _serializer = new();

    [Test]
    public void Deserialize_ShouldUseRequestedGenericType_NotClaimCheckMetadata()
    {
        var writer = new ArrayBufferWriter<byte>();
        _serializer.Serialize(writer, new SafeMessage { Content = "safe" });

        var result = _serializer.Deserialize<SafeMessage>(writer.WrittenMemory);

        result.Content.ShouldBe("safe");
    }

    public class SafeMessage
    {
        public string Content { get; set; } = string.Empty;
    }
}

[MemoryPackable]
public partial class SafeMemoryPackMessage
{
    public string? Content { get; set; }
}

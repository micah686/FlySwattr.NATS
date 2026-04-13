using System.Buffers;
using BenchmarkDotNet.Attributes;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Services;
using FlySwattr.NATS.Core.Serializers;
using MemoryPack;
using Microsoft.Extensions.Logging.Abstractions;

namespace Benchmarks;

/// <summary>
/// Measures the end-to-end throughput of the claim-check pipeline:
/// serialize → size-check → offload to mock object store → build headers.
/// Uses an in-memory mock to isolate compute costs from I/O.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(iterationCount: 20)]
public class ClaimCheckPipelineBenchmark
{
    private IMessageSerializer _serializer = null!;
    private IMessageTypeAliasRegistry _aliasRegistry = null!;
    private LargeBenchmarkMessage _smallMessage = null!;
    private LargeBenchmarkMessage _largeMessage = null!;

    [GlobalSetup]
    public void Setup()
    {
        _serializer = new HybridNatsSerializer(
            maxPayloadSize: 10 * 1024 * 1024,
            enforceSchemaFingerprint: true,
            logger: NullLogger<HybridNatsSerializer>.Instance);

        _aliasRegistry = new MessageTypeAliasRegistry();
        _aliasRegistry.Register<LargeBenchmarkMessage>("LargeBenchmarkMessage");

        // Small message: under typical threshold
        _smallMessage = new LargeBenchmarkMessage
        {
            Id = "small-1",
            Data = new byte[512] // 512 bytes
        };

        // Large message: above typical 1MB threshold
        _largeMessage = new LargeBenchmarkMessage
        {
            Id = "large-1",
            Data = new byte[2 * 1024 * 1024] // 2MB
        };
        new Random(42).NextBytes(_largeMessage.Data);
    }

    [Benchmark]
    public int SerializeAndCheckSize_SmallMessage()
    {
        var bufferWriter = new ArrayBufferWriter<byte>();
        _serializer.Serialize(bufferWriter, _smallMessage);
        var payloadSize = MemoryPackSchemaEnvelopeSerializer.GetLogicalPayloadSize(bufferWriter.WrittenSpan);
        return payloadSize;
    }

    [Benchmark]
    public int SerializeAndCheckSize_LargeMessage()
    {
        var bufferWriter = new ArrayBufferWriter<byte>();
        _serializer.Serialize(bufferWriter, _largeMessage);
        var payloadSize = MemoryPackSchemaEnvelopeSerializer.GetLogicalPayloadSize(bufferWriter.WrittenSpan);
        return payloadSize;
    }

    [Benchmark]
    public Dictionary<string, string> BuildClaimCheckHeaders()
    {
        var alias = _aliasRegistry.GetAlias(typeof(LargeBenchmarkMessage));
        var contentType = _serializer.GetContentType<LargeBenchmarkMessage>();
        var objectKey = MessageSecurity.ValidateObjectStoreKey("claimcheck/orders/abc123");
        var claimCheckRef = $"objstore://{objectKey}";

        return new Dictionary<string, string>
        {
            ["X-ClaimCheck-Ref"] = claimCheckRef,
            ["X-ClaimCheck-Type"] = alias,
            ["Content-Type"] = contentType
        };
    }

    [Benchmark]
    public (int PayloadSize, bool ShouldOffload) FullPipelineDecision_SmallMessage()
    {
        var bufferWriter = new ArrayBufferWriter<byte>();
        _serializer.Serialize(bufferWriter, _smallMessage);
        var payloadSize = MemoryPackSchemaEnvelopeSerializer.GetLogicalPayloadSize(bufferWriter.WrittenSpan);
        const int threshold = 1024 * 1024; // 1MB
        var shouldOffload = payloadSize > threshold;

        if (shouldOffload)
        {
            var objectKey = MessageSecurity.ValidateObjectStoreKey($"claimcheck/orders/{Guid.NewGuid():N}");
            var alias = _aliasRegistry.GetAlias(typeof(LargeBenchmarkMessage));
            _ = new Dictionary<string, string>
            {
                ["X-ClaimCheck-Ref"] = $"objstore://{objectKey}",
                ["X-ClaimCheck-Type"] = alias,
                ["Content-Type"] = _serializer.GetContentType<LargeBenchmarkMessage>()
            };
        }

        return (payloadSize, shouldOffload);
    }

    [Benchmark]
    public (int PayloadSize, bool ShouldOffload) FullPipelineDecision_LargeMessage()
    {
        var bufferWriter = new ArrayBufferWriter<byte>();
        _serializer.Serialize(bufferWriter, _largeMessage);
        var payloadSize = MemoryPackSchemaEnvelopeSerializer.GetLogicalPayloadSize(bufferWriter.WrittenSpan);
        const int threshold = 1024 * 1024; // 1MB
        var shouldOffload = payloadSize > threshold;

        if (shouldOffload)
        {
            var objectKey = MessageSecurity.ValidateObjectStoreKey($"claimcheck/orders/{Guid.NewGuid():N}");
            var alias = _aliasRegistry.GetAlias(typeof(LargeBenchmarkMessage));
            _ = new Dictionary<string, string>
            {
                ["X-ClaimCheck-Ref"] = $"objstore://{objectKey}",
                ["X-ClaimCheck-Type"] = alias,
                ["Content-Type"] = _serializer.GetContentType<LargeBenchmarkMessage>()
            };
        }

        return (payloadSize, shouldOffload);
    }
}

[MemoryPackable]
public partial record LargeBenchmarkMessage
{
    public string Id { get; init; } = string.Empty;
    public byte[] Data { get; init; } = [];
}

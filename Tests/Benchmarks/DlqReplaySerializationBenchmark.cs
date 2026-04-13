using System.Buffers;
using BenchmarkDotNet.Attributes;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Services;
using FlySwattr.NATS.Core.Serializers;
using MemoryPack;
using Microsoft.Extensions.Logging.Abstractions;

namespace Benchmarks;

/// <summary>
/// Measures the cost of the DLQ replay re-serialization path:
/// serialize → resolve type alias → deserialize → (simulated) re-publish.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(iterationCount: 20)]
public class DlqReplaySerializationBenchmark
{
    private IMessageSerializer _serializer = null!;
    private IMessageTypeAliasRegistry _aliasRegistry = null!;
    private byte[] _serializedPayload = null!;

    [GlobalSetup]
    public void Setup()
    {
        _serializer = new HybridNatsSerializer(
            maxPayloadSize: 10 * 1024 * 1024,
            enforceSchemaFingerprint: true,
            logger: NullLogger<HybridNatsSerializer>.Instance);

        _aliasRegistry = new MessageTypeAliasRegistry();
        _aliasRegistry.Register<SampleBenchmarkMessage>("SampleBenchmarkMessage");

        // Pre-serialize a sample message
        var bufferWriter = new ArrayBufferWriter<byte>();
        _serializer.Serialize(bufferWriter, new SampleBenchmarkMessage
        {
            OrderId = "ORD-12345",
            CustomerId = "CUST-67890",
            Amount = 99.99m,
            CreatedAt = DateTimeOffset.UtcNow,
            Items = Enumerable.Range(0, 10).Select(i => $"Item-{i}").ToList()
        });
        _serializedPayload = bufferWriter.WrittenSpan.ToArray();
    }

    [Benchmark]
    public byte[] Serialize_SampleMessage()
    {
        var bufferWriter = new ArrayBufferWriter<byte>();
        _serializer.Serialize(bufferWriter, new SampleBenchmarkMessage
        {
            OrderId = "ORD-12345",
            CustomerId = "CUST-67890",
            Amount = 99.99m,
            CreatedAt = DateTimeOffset.UtcNow,
            Items = Enumerable.Range(0, 10).Select(i => $"Item-{i}").ToList()
        });
        return bufferWriter.WrittenSpan.ToArray();
    }

    [Benchmark]
    public SampleBenchmarkMessage Deserialize_SampleMessage()
    {
        return _serializer.Deserialize<SampleBenchmarkMessage>(_serializedPayload);
    }

    [Benchmark]
    public string ResolveTypeAlias()
    {
        return _aliasRegistry.GetAlias(typeof(SampleBenchmarkMessage));
    }

    [Benchmark]
    public Type? ResolveTypeFromAlias()
    {
        return _aliasRegistry.Resolve("SampleBenchmarkMessage");
    }

    [Benchmark]
    public SampleBenchmarkMessage FullReplayRoundTrip()
    {
        // 1. Resolve alias
        var alias = _aliasRegistry.GetAlias(typeof(SampleBenchmarkMessage));
        
        // 2. Resolve back to type
        var type = _aliasRegistry.Resolve(alias);
        
        // 3. Deserialize using the original payload
        var deserialized = _serializer.Deserialize<SampleBenchmarkMessage>(_serializedPayload);
        
        // 4. Re-serialize (simulating re-publish)
        var bufferWriter = new ArrayBufferWriter<byte>();
        _serializer.Serialize(bufferWriter, deserialized);
        
        return deserialized;
    }
}

[MemoryPackable]
public partial record SampleBenchmarkMessage
{
    public string OrderId { get; init; } = string.Empty;
    public string CustomerId { get; init; } = string.Empty;
    public decimal Amount { get; init; }
    public DateTimeOffset CreatedAt { get; init; }
    public List<string> Items { get; init; } = [];
}

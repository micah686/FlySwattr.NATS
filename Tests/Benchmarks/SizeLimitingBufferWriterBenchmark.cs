using System.Buffers;
using BenchmarkDotNet.Attributes;
using FlySwattr.NATS.Core.Serializers;

namespace Benchmarks;

/// <summary>
/// Measures the per-call overhead of SizeLimitingBufferWriter's safety checks
/// (GetMemory/GetSpan/Advance) compared to a raw ArrayBufferWriter.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(iterationCount: 20)]
public class SizeLimitingBufferWriterBenchmark
{
    private const int MaxSize = 1024 * 1024; // 1MB
    private const int ChunkSize = 1024;      // 1KB per write
    private const int Iterations = 100;      // 100 writes per benchmark run

    [Benchmark(Baseline = true)]
    public int RawArrayBufferWriter()
    {
        var writer = new ArrayBufferWriter<byte>(ChunkSize);
        var total = 0;

        for (var i = 0; i < Iterations; i++)
        {
            var span = writer.GetSpan(ChunkSize);
            span[..ChunkSize].Fill(42);
            writer.Advance(ChunkSize);
            total += ChunkSize;
        }

        return total;
    }

    [Benchmark]
    public int SizeLimitingBufferWriter_GetSpan_Advance()
    {
        var inner = new ArrayBufferWriter<byte>(ChunkSize);
        var writer = new SizeLimitingBufferWriter(inner, MaxSize);
        var total = 0;

        for (var i = 0; i < Iterations; i++)
        {
            var span = writer.GetSpan(ChunkSize);
            span[..ChunkSize].Fill(42);
            writer.Advance(ChunkSize);
            total += ChunkSize;
        }

        return total;
    }

    [Benchmark]
    public int SizeLimitingBufferWriter_GetMemory_Advance()
    {
        var inner = new ArrayBufferWriter<byte>(ChunkSize);
        var writer = new SizeLimitingBufferWriter(inner, MaxSize);
        var total = 0;

        for (var i = 0; i < Iterations; i++)
        {
            var memory = writer.GetMemory(ChunkSize);
            memory.Span[..ChunkSize].Fill(42);
            writer.Advance(ChunkSize);
            total += ChunkSize;
        }

        return total;
    }
}

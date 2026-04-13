using BenchmarkDotNet.Attributes;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Services;

namespace Benchmarks;

/// <summary>
/// Measures the per-call cost of MessageSecurity validation methods
/// that run on every publish and consume path.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(iterationCount: 20)]
public class MessageSecurityValidationBenchmark
{
    private const string ValidSubject = "orders.created.us-east-1";
    private const string ValidObjectKey = "claimcheck/orders/abc123def456";
    private MessageHeaders? _headersWithReserved;
    private MessageHeaders? _headersClean;
    private readonly string[] _reservedHeaders = ["Content-Type", "Nats-Msg-Id", "traceparent", "tracestate", "X-FlySwattr-Version"];
    private Exception _testException = null!;

    [GlobalSetup]
    public void Setup()
    {
        _headersClean = new MessageHeaders(new Dictionary<string, string>
        {
            ["X-Custom-1"] = "value1",
            ["X-Custom-2"] = "value2"
        });

        _headersWithReserved = null; // No headers (common path)

        _testException = new InvalidOperationException("Something bad happened with order ID 12345 and customer data should not appear here");
    }

    [Benchmark]
    public void ValidatePublishSubject()
    {
        MessageSecurity.ValidatePublishSubject(ValidSubject);
    }

    [Benchmark]
    public void RejectReservedHeaders_NullHeaders()
    {
        MessageSecurity.RejectReservedHeaders(_headersWithReserved, _reservedHeaders);
    }

    [Benchmark]
    public void RejectReservedHeaders_CleanHeaders()
    {
        MessageSecurity.RejectReservedHeaders(_headersClean, _reservedHeaders);
    }

    [Benchmark]
    public string ValidateObjectStoreKey()
    {
        return MessageSecurity.ValidateObjectStoreKey(ValidObjectKey);
    }

    [Benchmark]
    public string SanitizeExceptionMessage()
    {
        return MessageSecurity.SanitizeExceptionMessage(_testException);
    }
}

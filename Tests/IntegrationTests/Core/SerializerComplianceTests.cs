using System.Buffers;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Serializers;
using IntegrationTests.Infrastructure;
using NATS.Client.Core;
using Shouldly;
using TUnit.Core;

namespace IntegrationTests.Core;

/// <summary>
/// Serializer Compliance Tests - Verify that any IMessageSerializer implementation correctly 
/// roundtrips data through the NATS infrastructure. These tests focus on the interface contract
/// rather than specific serialization format implementation details.
/// </summary>
[Property("nTag", "Core")]
public class SerializerComplianceTests
{
    /// <summary>
    /// Verifies that a custom serializer implementation can correctly roundtrip data
    /// through the NATS pub/sub infrastructure via the INatsSerializerRegistry interface.
    /// </summary>
    [Test]
    public async Task Verify_CustomSerializer_Roundtrip_ThroughNats()
    {
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();
        
        // Setup Registry with custom serializer implementation
        var registry = new HybridNatsSerializerRegistry();
        
        var opts = new NatsOpts { Url = fixture.ConnectionString, SerializerRegistry = registry };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();
        
        var tcs = new TaskCompletionSource<SerializerTestData>();
        
        // Start background listener
        _ = Task.Run(async () => 
        {
            await foreach (var msg in conn.SubscribeAsync<SerializerTestData>("serializer.compliance.test"))
            {
                tcs.SetResult(msg.Data!);
                break;
            }
        });
        
        await Task.Delay(500);
        
        var payload = new SerializerTestData { Id = 99, Name = "Compliance Test" };
        await conn.PublishAsync("serializer.compliance.test", payload);
        
        var result = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        
        await Assert.That(result).IsNotNull();
        await Assert.That(result.Id).IsEqualTo(99);
        await Assert.That(result.Name).IsEqualTo("Compliance Test");
    }
    
    /// <summary>
    /// Verifies that the IMessageSerializer interface contract handles primitives correctly.
    /// </summary>
    [Test]
    public async Task Verify_Serializer_Handles_Primitives()
    {
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();
        
        var registry = new HybridNatsSerializerRegistry();
        
        var opts = new NatsOpts { Url = fixture.ConnectionString, SerializerRegistry = registry };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();
        
        var tcs = new TaskCompletionSource<int>();
        
        _ = Task.Run(async () => 
        {
            await foreach (var msg in conn.SubscribeAsync<int>("serializer.primitive.test"))
            {
                tcs.SetResult(msg.Data);
                break;
            }
        });
        
        await Task.Delay(500);
        
        await conn.PublishAsync("serializer.primitive.test", 42);
        
        var result = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        
        await Assert.That(result).IsEqualTo(42);
    }

    /// <summary>
    /// Verifies that the IMessageSerializer interface contract handles complex nested objects.
    /// </summary>
    [Test]
    public async Task Verify_Serializer_Handles_NestedObjects()
    {
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();
        
        var registry = new HybridNatsSerializerRegistry();
        
        var opts = new NatsOpts { Url = fixture.ConnectionString, SerializerRegistry = registry };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();
        
        var tcs = new TaskCompletionSource<NestedSerializerTestData>();
        
        _ = Task.Run(async () => 
        {
            await foreach (var msg in conn.SubscribeAsync<NestedSerializerTestData>("serializer.nested.test"))
            {
                tcs.SetResult(msg.Data!);
                break;
            }
        });
        
        await Task.Delay(500);
        
        var payload = new NestedSerializerTestData 
        { 
            Inner = new SerializerTestData { Id = 123, Name = "Nested" },
            Tags = ["tag1", "tag2"]
        };
        await conn.PublishAsync("serializer.nested.test", payload);
        
        var result = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        
        await Assert.That(result).IsNotNull();
        await Assert.That(result.Inner).IsNotNull();
        await Assert.That(result.Inner!.Id).IsEqualTo(123);
        await Assert.That(result.Inner!.Name).IsEqualTo("Nested");
        await Assert.That(result.Tags).IsEquivalentTo(new[] { "tag1", "tag2" });
    }
}

/// <summary>
/// Adapter to use HybridNatsSerializer with NATS.Client.Core's INatsSerializerRegistry.
/// </summary>
public class HybridNatsSerializerRegistry : INatsSerializerRegistry
{
    private readonly HybridNatsSerializer _serializer = new();
    
    public INatsSerialize<T> GetSerializer<T>() => new HybridNatsSerializerAdapter<T>(_serializer);
    public INatsDeserialize<T> GetDeserializer<T>() => new HybridNatsSerializerAdapter<T>(_serializer);
}

/// <summary>
/// Adapter to convert IMessageSerializer to NATS.Client.Core serialization interfaces.
/// </summary>
public class HybridNatsSerializerAdapter<T> : INatsSerialize<T>, INatsDeserialize<T>
{
    private readonly IMessageSerializer _serializer;
    
    public HybridNatsSerializerAdapter(IMessageSerializer serializer)
    {
        _serializer = serializer;
    }
    
    public void Serialize(IBufferWriter<byte> bufferWriter, T value)
    {
        _serializer.Serialize(bufferWriter, value);
    }

    public T? Deserialize(in ReadOnlySequence<byte> buffer)
    {
        return _serializer.Deserialize<T>(buffer.ToArray());
    }
}

/// <summary>
/// Test data class for serializer compliance tests.
/// </summary>
public record SerializerTestData
{
    public int Id { get; init; }
    public required string Name { get; init; }
}

/// <summary>
/// Test data class with nested objects for serializer compliance tests.
/// </summary>
public record NestedSerializerTestData
{
    public SerializerTestData? Inner { get; init; }
    public List<string>? Tags { get; init; }
}

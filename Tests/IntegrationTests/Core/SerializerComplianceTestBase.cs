using System.Buffers;
using System.Text.Json;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Serializers;
using IntegrationTests.Infrastructure;
using MemoryPack;
using NATS.Client.Core;
using Shouldly;
using TUnit.Core;

namespace IntegrationTests.Core;

/// <summary>
/// Abstract base class for Serializer Compliance Tests.
/// Verifies that any serializer implementation correctly roundtrips data through NATS.
/// </summary>
public abstract class SerializerComplianceTestBase
{
    protected abstract INatsSerializerRegistry GetRegistry();

    [Test]
    public async Task Verify_Serializer_Roundtrip_ThroughNats()
    {
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();
        
        var registry = GetRegistry();
        
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
    
    [Test]
    public async Task Verify_Serializer_Handles_Primitives()
    {
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();
        
        var registry = GetRegistry();
        
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
}

[Property("nTag", "Core")]
[Property("nTag", "Serializer")]
[InheritsTests]
public class HybridSerializerComplianceTests : SerializerComplianceTestBase
{
    protected override INatsSerializerRegistry GetRegistry()
    {
        // Use the internal adapter that wraps our HybridNatsSerializer
        return new AdapterRegistry(new HybridNatsSerializer());
    }
}

[Property("nTag", "Core")]
[Property("nTag", "Serializer")]
[InheritsTests]
public class JsonSerializerComplianceTests : SerializerComplianceTestBase
{
    protected override INatsSerializerRegistry GetRegistry()
    {
        return new AdapterRegistry(new JsonMessageSerializer());
    }
}

[Property("nTag", "Core")]
[Property("nTag", "Serializer")]
[InheritsTests]
public class MemoryPackSerializerComplianceTests : SerializerComplianceTestBase
{
    protected override INatsSerializerRegistry GetRegistry()
    {
        return new AdapterRegistry(new MemoryPackMessageSerializer());
    }
}

#region Adapters and Test Data

public class AdapterRegistry : INatsSerializerRegistry
{
    private readonly IMessageSerializer _serializer;
    
    public AdapterRegistry(IMessageSerializer serializer)
    {
        _serializer = serializer;
    }
    
    public INatsSerialize<T> GetSerializer<T>() => new AdapterSerializer<T>(_serializer);
    public INatsDeserialize<T> GetDeserializer<T>() => new AdapterSerializer<T>(_serializer);
}

public class AdapterSerializer<T> : INatsSerialize<T>, INatsDeserialize<T>
{
    private readonly IMessageSerializer _serializer;
    
    public AdapterSerializer(IMessageSerializer serializer)
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

public class JsonMessageSerializer : IMessageSerializer
{
    public void Serialize<T>(IBufferWriter<byte> writer, T message)
    {
        using var jsonWriter = new Utf8JsonWriter(writer);
        JsonSerializer.Serialize(jsonWriter, message);
    }

    public T Deserialize<T>(ReadOnlyMemory<byte> data)
    {
        return JsonSerializer.Deserialize<T>(data.Span)!;
    }

    public string GetContentType<T>() => "application/json";
}

public class MemoryPackMessageSerializer : IMessageSerializer
{
    public void Serialize<T>(IBufferWriter<byte> writer, T message)
    {
        MemoryPackSerializer.Serialize(writer, message);
    }

    public T Deserialize<T>(ReadOnlyMemory<byte> data)
    {
        return MemoryPackSerializer.Deserialize<T>(data.Span)!;
    }

    public string GetContentType<T>() => "application/x-memorypack";
}

[MemoryPackable]
public partial record SerializerTestData
{
    public int Id { get; init; }
    public required string Name { get; init; }
}
#endregion
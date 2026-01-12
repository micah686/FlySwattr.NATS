using System.Buffers;
using IntegrationTests.Infrastructure;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;
using Shouldly;
using TUnit.Core;

namespace IntegrationTests.Core;

[Property("nTag", "Core")]
public class CoreNatsSerializationTests
{
    [MessagePackObject]
    public record MsgPackData
    {
        [Key(0)] public int Id { get; init; }
        [Key(1)] public required string Name { get; init; }
    }

    [Test]
    public async Task Verify_MessagePack_Serialization()
    {
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();
        
        // Setup Registry with MessagePack (Local implementation since Core uses MemoryPack)
        var registry = new MessagePackSerializerRegistry();
        
        var opts = new NatsOpts { Url = fixture.ConnectionString, SerializerRegistry = registry };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();
        
        var tcs = new TaskCompletionSource<MsgPackData>();
        
        // Start background listener
        _ = Task.Run(async () => 
        {
            await foreach (var msg in conn.SubscribeAsync<MsgPackData>("msgpack.test"))
            {
                tcs.SetResult(msg.Data!);
                break;
            }
        });
        
        await Task.Delay(500);
        
        var payload = new MsgPackData { Id = 99, Name = "MP" };
        await conn.PublishAsync("msgpack.test", payload);
        
        var result = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        
        await Assert.That(result).IsNotNull();
        await Assert.That(result.Id).IsEqualTo(99);
        await Assert.That(result.Name).IsEqualTo("MP");
    }

    public class MessagePackSerializerRegistry : INatsSerializerRegistry
    {
        public INatsSerialize<T> GetSerializer<T>() => new MessagePackNatsSerializer<T>();
        public INatsDeserialize<T> GetDeserializer<T>() => new MessagePackNatsSerializer<T>();
    }

    public class MessagePackNatsSerializer<T> : INatsSerialize<T>, INatsDeserialize<T>
    {
        public void Serialize(IBufferWriter<byte> bufferWriter, T value)
        {
            MessagePackSerializer.Serialize(bufferWriter, value);
        }

        public T? Deserialize(in ReadOnlySequence<byte> buffer)
        {
            return MessagePackSerializer.Deserialize<T>(buffer);
        }
    }
}

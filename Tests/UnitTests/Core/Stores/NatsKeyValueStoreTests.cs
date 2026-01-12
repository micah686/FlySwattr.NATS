using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Stores;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.KeyValueStore;
using NSubstitute;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Core.Stores;

[Property("nTag", "Core")]
public class NatsKeyValueStoreTests : IAsyncDisposable
{
    private readonly INatsKVContext _kvContext;
    private readonly INatsKVStore _natsKvStore;
    private readonly ILogger<NatsKeyValueStore> _logger;
    private readonly NatsKeyValueStore _sut;
    private readonly string _bucket = "test-bucket";

    public NatsKeyValueStoreTests()
    {
        _kvContext = Substitute.For<INatsKVContext>();
        _natsKvStore = Substitute.For<INatsKVStore>();
        _logger = Substitute.For<ILogger<NatsKeyValueStore>>();
        
        _kvContext.GetStoreAsync(_bucket, Arg.Any<CancellationToken>())
            .Returns(_natsKvStore);

        _sut = new NatsKeyValueStore(_kvContext, _bucket, _logger);
    }

    public async ValueTask DisposeAsync()
    {
        await _sut.DisposeAsync();
    }

    [Test]
    public async Task PutAsync_ShouldCallStorePut()
    {
        // Arrange
        var key = "key1";
        var value = "value1";
        
        // Act
        await _sut.PutAsync(key, value);

        // Assert
        await _natsKvStore.Received(1).PutAsync(key, value, Arg.Any<INatsSerialize<string>?>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetAsync_ShouldCallStoreGet()
    {
        // Arrange
        var key = "key1";
        var value = "store-value";
        var entry = CreateTestEntry(key, value, 1);
        
        _natsKvStore.GetEntryAsync<string>(key, Arg.Any<ulong>(), Arg.Any<INatsDeserialize<string>?>(), Arg.Any<CancellationToken>())
            .Returns(entry);

        // Act
        var result = await _sut.GetAsync<string>(key);

        // Assert
        result.ShouldBe(value);
        await _natsKvStore.Received(1).GetEntryAsync<string>(key, Arg.Any<ulong>(), Arg.Any<INatsDeserialize<string>?>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetAsync_ShouldReturnNull_WhenKeyNotFound()
    {
        // Arrange
        var key = "missing-key";
        _natsKvStore.GetEntryAsync<string>(key, Arg.Any<ulong>(), Arg.Any<INatsDeserialize<string>?>(), Arg.Any<CancellationToken>())
            .Returns(x => ValueTask.FromException<NatsKVEntry<string>>(new NatsKVKeyNotFoundException()));

        // Act
        var result = await _sut.GetAsync<string>(key);

        // Assert
        result.ShouldBeNull();
    }

    [Test]
    public async Task DeleteAsync_ShouldCallStoreDelete()
    {
        // Arrange
        var key = "key1";

        // Act
        await _sut.DeleteAsync(key);

        // Assert
        await _natsKvStore.Received(1).DeleteAsync(key, Arg.Any<NatsKVDeleteOpts?>(), Arg.Any<CancellationToken>());
    }

    private NatsKVEntry<T> CreateTestEntry<T>(string key, T value, ulong revision, NatsKVOperation op = NatsKVOperation.Put)
    {
        try 
        {
            var type = typeof(NatsKVEntry<T>);
            object boxed = default(NatsKVEntry<T>)!; 
            
            void SetField(string name, object? val)
            {
                var field = type.GetField($"<{name}>k__BackingField", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
                if (field != null) field.SetValue(boxed, val);
            }

            SetField(nameof(NatsKVEntry<T>.Value), value);
            SetField(nameof(NatsKVEntry<T>.Key), key);
            SetField(nameof(NatsKVEntry<T>.Revision), revision);
            SetField(nameof(NatsKVEntry<T>.Operation), op);

            return (NatsKVEntry<T>)boxed;
        }
        catch
        {
            return default;
        }
    }
}
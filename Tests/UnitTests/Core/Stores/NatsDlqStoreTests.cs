using System.Text.Json;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Core.Stores;

[Property("nTag", "Core")]
public class NatsDlqStoreTests
{
    private readonly IKeyValueStore _kvStore;
    private readonly ILogger<NatsDlqStore> _logger;
    private readonly NatsDlqStore _sut;
    private const string BucketName = "fs-dlq-entries";

    public NatsDlqStoreTests()
    {
        _kvStore = Substitute.For<IKeyValueStore>();
        _logger = Substitute.For<ILogger<NatsDlqStore>>();
        
        Func<string, IKeyValueStore> storeFactory = bucket => 
        {
            if (bucket == BucketName) return _kvStore;
            throw new ArgumentException($"Unexpected bucket name: {bucket}");
        };

        _sut = new NatsDlqStore(storeFactory, _logger);
    }

    [Test]
    public async Task StoreAsync_ShouldPersistEntry()
    {
        // Arrange
        var entry = CreateTestEntry("msg-1");
        var expectedJson = JsonSerializer.Serialize(entry);

        // Act
        await _sut.StoreAsync(entry);

        // Assert
        // IKeyValueStore.PutAsync signature: Task PutAsync<T>(string key, T value, CancellationToken cancellationToken = default);
        await _kvStore.Received(1).PutAsync(entry.Id, Arg.Is<string>(s => s == expectedJson), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task StoreAsync_ShouldThrow_WhenEntryIsNull()
    {
        // Act
        Func<Task> act = async () => await _sut.StoreAsync(null!);

        // Assert
        await act.ShouldThrowAsync<ArgumentNullException>();
    }

    [Test]
    public async Task StoreAsync_ShouldThrow_WhenIdEmpty()
    {
        // Arrange
        var entry = CreateTestEntry("");

        // Act
        Func<Task> act = async () => await _sut.StoreAsync(entry);

        // Assert
        await act.ShouldThrowAsync<ArgumentException>();
    }

    [Test]
    public async Task GetAsync_ShouldReturnStoredEntry()
    {
        // Arrange
        var id = "msg-1";
        var entry = CreateTestEntry(id);
        var json = JsonSerializer.Serialize(entry);
        
        _kvStore.GetAsync<string>(id, Arg.Any<CancellationToken>()).Returns(json);

        // Act
        var result = await _sut.GetAsync(id);

        // Assert
        result.ShouldNotBeNull();
        result!.Id.ShouldBe(id);
        result.OriginalStream.ShouldBe(entry.OriginalStream);
    }

    [Test]
    public async Task GetAsync_ShouldReturnNull_WhenNotFound()
    {
        // Arrange
        var id = "missing";
        _kvStore.GetAsync<string>(id, Arg.Any<CancellationToken>()).Returns((string?)null);

        // Act
        var result = await _sut.GetAsync(id);

        // Assert
        result.ShouldBeNull();
    }

    [Test]
    public async Task GetAsync_ShouldThrow_WhenIdEmpty()
    {
        // Act
        Func<Task> act = async () => await _sut.GetAsync("");

        // Assert
        await act.ShouldThrowAsync<ArgumentException>();
    }

    [Test]
    public async Task ListAsync_Behavior_ShouldReturnEmptyList()
    {
        // Act
        var result = await _sut.ListAsync();

        // Assert
        result.ShouldBeEmpty();
    }

    private DlqMessageEntry CreateTestEntry(string id)
    {
        return new DlqMessageEntry
        {
            Id = id,
            OriginalStream = "test-stream",
            OriginalConsumer = "test-consumer",
            OriginalSubject = "test.subject",
            OriginalSequence = 1,
            DeliveryCount = 3,
            StoredAt = DateTimeOffset.UtcNow,
            ErrorReason = "Test error"
        };
    }
}
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

    #region StoreAsync Tests

    [Test]
    public async Task StoreAsync_ShouldPersistEntry_WithHierarchicalKey()
    {
        // Arrange
        var entry = CreateTestEntry("msg-1", "orders-stream", "processor-consumer");
        var expectedKey = "orders-stream.processor-consumer.msg-1";
        var expectedJson = JsonSerializer.Serialize(entry);

        // Act
        await _sut.StoreAsync(entry);

        // Assert - key should be hierarchical: stream.consumer.id
        await _kvStore.Received(1).PutAsync(
            expectedKey, 
            Arg.Is<string>(s => s == expectedJson), 
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task StoreAsync_ShouldSanitizeDotsInStreamName()
    {
        // Arrange
        var entry = CreateTestEntry("msg-1", "orders.v2.stream", "consumer");
        var expectedKey = "orders_v2_stream.consumer.msg-1";

        // Act
        await _sut.StoreAsync(entry);

        // Assert - dots in stream name should be replaced with underscores
        await _kvStore.Received(1).PutAsync(
            expectedKey, 
            Arg.Any<string>(), 
            Arg.Any<CancellationToken>());
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

    #endregion

    #region GetAsync Tests

    [Test]
    public async Task GetAsync_ShouldReturnStoredEntry()
    {
        // Arrange - key is now hierarchical
        var key = "test-stream.test-consumer.msg-1";
        var entry = CreateTestEntry("msg-1");
        var json = JsonSerializer.Serialize(entry);
        
        _kvStore.GetAsync<string>(key, Arg.Any<CancellationToken>()).Returns(json);

        // Act
        var result = await _sut.GetAsync(key);

        // Assert
        result.ShouldNotBeNull();
        result!.Id.ShouldBe("msg-1");
        result.OriginalStream.ShouldBe(entry.OriginalStream);
    }

    [Test]
    public async Task GetAsync_ShouldReturnNull_WhenNotFound()
    {
        // Arrange
        var key = "missing-stream.missing-consumer.missing-id";
        _kvStore.GetAsync<string>(key, Arg.Any<CancellationToken>()).Returns((string?)null);

        // Act
        var result = await _sut.GetAsync(key);

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

    #endregion

    #region ListAsync Tests

    [Test]
    public async Task ListAsync_ShouldFilterByStream()
    {
        // Arrange
        var entries = new[]
        {
            CreateTestEntry("msg-1", "orders", "consumer1"),
            CreateTestEntry("msg-2", "orders", "consumer2")
        };
        
        SetupGetKeysAsync(["orders.>"], ["orders.consumer1.msg-1", "orders.consumer2.msg-2"]);
        SetupGetAsync("orders.consumer1.msg-1", entries[0]);
        SetupGetAsync("orders.consumer2.msg-2", entries[1]);

        // Act
        var result = await _sut.ListAsync(filterStream: "orders");

        // Assert
        result.Count.ShouldBe(2);
        result.ShouldAllBe(e => e.OriginalStream == "orders");
    }

    [Test]
    public async Task ListAsync_ShouldFilterByConsumer()
    {
        // Arrange
        var entry = CreateTestEntry("msg-1", "orders", "processor");
        
        SetupGetKeysAsync(["*.processor.*"], ["orders.processor.msg-1"]);
        SetupGetAsync("orders.processor.msg-1", entry);

        // Act
        var result = await _sut.ListAsync(filterConsumer: "processor");

        // Assert
        result.Count.ShouldBe(1);
        result[0].OriginalConsumer.ShouldBe("processor");
    }

    [Test]
    public async Task ListAsync_ShouldFilterByBothStreamAndConsumer()
    {
        // Arrange
        var entry = CreateTestEntry("msg-1", "orders", "processor");
        
        SetupGetKeysAsync(["orders.processor.*"], ["orders.processor.msg-1"]);
        SetupGetAsync("orders.processor.msg-1", entry);

        // Act
        var result = await _sut.ListAsync(filterStream: "orders", filterConsumer: "processor");

        // Assert
        result.Count.ShouldBe(1);
        result[0].OriginalStream.ShouldBe("orders");
        result[0].OriginalConsumer.ShouldBe("processor");
    }

    [Test]
    public async Task ListAsync_ShouldReturnAllWhenNoFilters()
    {
        // Arrange
        var entries = new[]
        {
            CreateTestEntry("msg-1", "stream1", "consumer1"),
            CreateTestEntry("msg-2", "stream2", "consumer2")
        };
        
        SetupGetKeysAsync([">"], ["stream1.consumer1.msg-1", "stream2.consumer2.msg-2"]);
        SetupGetAsync("stream1.consumer1.msg-1", entries[0]);
        SetupGetAsync("stream2.consumer2.msg-2", entries[1]);

        // Act
        var result = await _sut.ListAsync();

        // Assert
        result.Count.ShouldBe(2);
    }

    [Test]
    public async Task ListAsync_ShouldRespectLimit()
    {
        // Arrange
        SetupGetKeysAsync([">"], ["s.c.msg-1", "s.c.msg-2", "s.c.msg-3", "s.c.msg-4", "s.c.msg-5"]);
        for (var i = 1; i <= 5; i++)
        {
            SetupGetAsync($"s.c.msg-{i}", CreateTestEntry($"msg-{i}", "s", "c"));
        }

        // Act
        var result = await _sut.ListAsync(limit: 3);

        // Assert
        result.Count.ShouldBe(3);
    }

    [Test]
    public async Task ListAsync_ShouldReturnEmpty_WhenNoKeysMatch()
    {
        // Arrange
        SetupGetKeysAsync(["nonexistent.>"], []);

        // Act
        var result = await _sut.ListAsync(filterStream: "nonexistent");

        // Assert
        result.ShouldBeEmpty();
    }

    #endregion

    #region UpdateStatusAsync Tests

    [Test]
    public async Task UpdateStatusAsync_ShouldUpdateAndPersist()
    {
        // Arrange
        var key = "stream.consumer.msg-1";
        var entry = CreateTestEntry("msg-1");
        SetupGetAsync(key, entry);

        // Act
        var result = await _sut.UpdateStatusAsync(key, DlqMessageStatus.Resolved);

        // Assert
        result.ShouldBeTrue();
        await _kvStore.Received(1).PutAsync(
            key,
            Arg.Is<string>(s => s.Contains("\"Status\":2")), // Resolved = 2
            Arg.Any<CancellationToken>());
    }

    #endregion

    #region DeleteAsync Tests

    [Test]
    public async Task DeleteAsync_ShouldDeleteExistingEntry()
    {
        // Arrange
        var key = "stream.consumer.msg-1";
        var entry = CreateTestEntry("msg-1");
        SetupGetAsync(key, entry);

        // Act
        var result = await _sut.DeleteAsync(key);

        // Assert
        result.ShouldBeTrue();
        await _kvStore.Received(1).DeleteAsync(key, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task DeleteAsync_ShouldReturnFalse_WhenNotFound()
    {
        // Arrange
        var key = "missing.consumer.id";
        _kvStore.GetAsync<string>(key, Arg.Any<CancellationToken>()).Returns((string?)null);

        // Act
        var result = await _sut.DeleteAsync(key);

        // Assert
        result.ShouldBeFalse();
        await _kvStore.DidNotReceive().DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    #endregion

    #region Helpers

    private DlqMessageEntry CreateTestEntry(string id, string stream = "test-stream", string consumer = "test-consumer")
    {
        return new DlqMessageEntry
        {
            Id = id,
            OriginalStream = stream,
            OriginalConsumer = consumer,
            OriginalSubject = "test.subject",
            OriginalSequence = 1,
            DeliveryCount = 3,
            StoredAt = DateTimeOffset.UtcNow,
            ErrorReason = "Test error"
        };
    }

    private void SetupGetKeysAsync(string[] expectedPatterns, string[] keysToReturn)
    {
        _kvStore.GetKeysAsync(
            Arg.Is<IEnumerable<string>>(p => p.SequenceEqual(expectedPatterns)),
            Arg.Any<CancellationToken>()
        ).Returns(keysToReturn.ToAsyncEnumerable());
    }

    private void SetupGetAsync(string key, DlqMessageEntry entry)
    {
        var json = JsonSerializer.Serialize(entry);
        _kvStore.GetAsync<string>(key, Arg.Any<CancellationToken>()).Returns(json);
    }

    #endregion
}
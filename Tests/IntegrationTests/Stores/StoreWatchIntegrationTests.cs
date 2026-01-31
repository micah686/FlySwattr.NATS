using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Stores;
using IntegrationTests.Infrastructure;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.KeyValueStore;
using NATS.Client.ObjectStore;
using NATS.Client.Serializers.Json;
using Shouldly;
using TUnit.Core;

namespace IntegrationTests.Stores;

/// <summary>
/// Integration tests for WatchAsync functionality in NatsKeyValueStore and NatsObjectStore.
/// </summary>
[Property("nTag", "Integration")]
public class StoreWatchIntegrationTests
{
    #region NatsKeyValueStore WatchAsync Tests

    /// <summary>
    /// Verify that WatchAsync receives Put events when a key is created or updated.
    /// </summary>
    [Test]
    public async Task NatsKeyValueStore_WatchAsync_OnPut_NotifiesHandler()
    {
        // Arrange
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts
        {
            Url = fixture.ConnectionString,
            SerializerRegistry = NatsJsonSerializerRegistry.Default
        };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();

        var js = new NatsJSContext(conn);
        var kv = new NatsKVContext(js);

        var bucket = "KV_WATCH_PUT_TEST";
        await kv.CreateStoreAsync(new NatsKVConfig(bucket)
        {
            Storage = NatsKVStorageType.Memory
        });

        await using var store = new NatsKeyValueStore(kv, bucket, NullLogger<NatsKeyValueStore>.Instance);

        var receivedEvents = new List<KvChangeEvent<string>>();
        var tcs = new TaskCompletionSource<bool>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        // Start watching
        var watchTask = store.WatchAsync<string>("test-key", async evt =>
        {
            receivedEvents.Add(evt);
            if (receivedEvents.Count >= 2)
            {
                tcs.TrySetResult(true);
            }
            await Task.CompletedTask;
        }, cts.Token);

        await Task.Delay(500); // Let watcher start

        // Act: Create and update a key
        await store.PutAsync("test-key", "value-1");
        await Task.Delay(200);
        await store.PutAsync("test-key", "value-2");

        // Wait for events
        await tcs.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // Cancel watcher
        await cts.CancelAsync();
        try { await watchTask; } catch (OperationCanceledException) { }

        // Assert
        receivedEvents.Count.ShouldBeGreaterThanOrEqualTo(2);

        var putEvents = receivedEvents.Where(e => e.Type == KvChangeType.Put).ToList();
        putEvents.Count.ShouldBeGreaterThanOrEqualTo(2);
        putEvents.Any(e => e.Value == "value-1").ShouldBeTrue();
        putEvents.Any(e => e.Value == "value-2").ShouldBeTrue();
    }

    /// <summary>
    /// Verify that WatchAsync receives Delete events when a key is deleted.
    /// </summary>
    [Test]
    public async Task NatsKeyValueStore_WatchAsync_OnDelete_NotifiesHandler()
    {
        // Arrange
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts
        {
            Url = fixture.ConnectionString,
            SerializerRegistry = NatsJsonSerializerRegistry.Default
        };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();

        var js = new NatsJSContext(conn);
        var kv = new NatsKVContext(js);

        var bucket = "KV_WATCH_DELETE_TEST";
        await kv.CreateStoreAsync(new NatsKVConfig(bucket)
        {
            Storage = NatsKVStorageType.Memory
        });

        await using var store = new NatsKeyValueStore(kv, bucket, NullLogger<NatsKeyValueStore>.Instance);

        var receivedEvents = new List<KvChangeEvent<string>>();
        var deleteReceived = new TaskCompletionSource<bool>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        // Start watching
        var watchTask = store.WatchAsync<string>("delete-key", async evt =>
        {
            receivedEvents.Add(evt);
            if (evt.Type == KvChangeType.Delete)
            {
                deleteReceived.TrySetResult(true);
            }
            await Task.CompletedTask;
        }, cts.Token);

        await Task.Delay(500);

        // Act: Create then delete a key
        await store.PutAsync("delete-key", "to-be-deleted");
        await Task.Delay(200);
        await store.DeleteAsync("delete-key");

        // Wait for delete event
        await deleteReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await cts.CancelAsync();
        try { await watchTask; } catch (OperationCanceledException) { }

        // Assert
        var deleteEvents = receivedEvents.Where(e => e.Type == KvChangeType.Delete).ToList();
        deleteEvents.Count.ShouldBeGreaterThanOrEqualTo(1);
        deleteEvents.Any(e => e.Key == "delete-key").ShouldBeTrue();
    }

    /// <summary>
    /// Verify that WatchAsync with wildcard pattern receives events for multiple keys.
    /// </summary>
    [Test]
    public async Task NatsKeyValueStore_WatchAsync_WildcardPattern_ReceivesMultipleKeys()
    {
        // Arrange
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts
        {
            Url = fixture.ConnectionString,
            SerializerRegistry = NatsJsonSerializerRegistry.Default
        };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();

        var js = new NatsJSContext(conn);
        var kv = new NatsKVContext(js);

        var bucket = "KV_WATCH_WILDCARD_TEST";
        await kv.CreateStoreAsync(new NatsKVConfig(bucket)
        {
            Storage = NatsKVStorageType.Memory
        });

        await using var store = new NatsKeyValueStore(kv, bucket, NullLogger<NatsKeyValueStore>.Instance);

        var receivedKeys = new HashSet<string>();
        var allReceived = new TaskCompletionSource<bool>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        // Watch all keys with wildcard
        var watchTask = store.WatchAsync<string>(">", async evt =>
        {
            if (evt.Type == KvChangeType.Put && evt.Key != null)
            {
                receivedKeys.Add(evt.Key);
                if (receivedKeys.Count >= 3)
                {
                    allReceived.TrySetResult(true);
                }
            }
            await Task.CompletedTask;
        }, cts.Token);

        await Task.Delay(500);

        // Act: Put multiple keys
        await store.PutAsync("config.app.name", "MyApp");
        await store.PutAsync("config.app.version", "1.0.0");
        await store.PutAsync("config.database.host", "localhost");

        await allReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await cts.CancelAsync();
        try { await watchTask; } catch (OperationCanceledException) { }

        // Assert
        receivedKeys.ShouldContain("config.app.name");
        receivedKeys.ShouldContain("config.app.version");
        receivedKeys.ShouldContain("config.database.host");
    }

    /// <summary>
    /// Verify that WatchAsync from one client receives updates from another client.
    /// This tests cross-client notification.
    /// </summary>
    [Test]
    public async Task NatsKeyValueStore_WatchAsync_CrossClient_ReceivesExternalUpdates()
    {
        // Arrange
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts
        {
            Url = fixture.ConnectionString,
            SerializerRegistry = NatsJsonSerializerRegistry.Default
        };

        // Client 1: Watcher
        await using var conn1 = new NatsConnection(opts);
        await conn1.ConnectAsync();
        var js1 = new NatsJSContext(conn1);
        var kv1 = new NatsKVContext(js1);

        // Client 2: Writer
        await using var conn2 = new NatsConnection(opts);
        await conn2.ConnectAsync();
        var js2 = new NatsJSContext(conn2);
        var kv2 = new NatsKVContext(js2);

        var bucket = "KV_CROSS_CLIENT_TEST";
        await kv1.CreateStoreAsync(new NatsKVConfig(bucket)
        {
            Storage = NatsKVStorageType.Memory
        });

        await using var watcherStore = new NatsKeyValueStore(kv1, bucket, NullLogger<NatsKeyValueStore>.Instance);
        await using var writerStore = new NatsKeyValueStore(kv2, bucket, NullLogger<NatsKeyValueStore>.Instance);

        var receivedValue = new TaskCompletionSource<string>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        // Client 1: Start watching
        var watchTask = watcherStore.WatchAsync<string>("shared-key", async evt =>
        {
            if (evt.Type == KvChangeType.Put && evt.Value != null)
            {
                receivedValue.TrySetResult(evt.Value);
            }
            await Task.CompletedTask;
        }, cts.Token);

        await Task.Delay(500);

        // Act: Client 2 writes
        await writerStore.PutAsync("shared-key", "external-update");

        // Wait for Client 1 to receive
        var value = await receivedValue.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await cts.CancelAsync();
        try { await watchTask; } catch (OperationCanceledException) { }

        // Assert
        value.ShouldBe("external-update");
    }

    #endregion

    #region NatsObjectStore WatchAsync Tests

    /// <summary>
    /// Verify that NatsObjectStore WatchAsync receives events when objects are added.
    /// </summary>
    [Test]
    public async Task NatsObjectStore_WatchAsync_OnPut_NotifiesHandler()
    {
        // Arrange
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts { Url = fixture.ConnectionString };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();

        var js = new NatsJSContext(conn);
        var objContext = new NatsObjContext(js);

        var bucket = "OBJ_WATCH_PUT_TEST";
        await objContext.CreateObjectStoreAsync(new NatsObjConfig(bucket)
        {
            Storage = NatsObjStorageType.Memory
        });

        await using var store = new NatsObjectStore(objContext, bucket, NullLogger<NatsObjectStore>.Instance);

        var receivedObjects = new List<ObjectInfo>();
        var objectReceived = new TaskCompletionSource<bool>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        // Start watching
        var watchTask = store.WatchAsync(async info =>
        {
            receivedObjects.Add(info);
            if (info.Key == "test-object.txt")
            {
                objectReceived.TrySetResult(true);
            }
            await Task.CompletedTask;
        }, cts.Token);

        await Task.Delay(500);

        // Act: Put an object
        var data = "Hello, World!"u8.ToArray();
        using var stream = new MemoryStream(data);
        await store.PutAsync("test-object.txt", stream);

        // Wait for event
        await objectReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await cts.CancelAsync();
        try { await watchTask; } catch (OperationCanceledException) { }

        // Assert
        receivedObjects.Any(o => o.Key == "test-object.txt").ShouldBeTrue();
        var obj = receivedObjects.First(o => o.Key == "test-object.txt");
        obj.Size.ShouldBe(data.Length);
    }

    /// <summary>
    /// Verify that NatsObjectStore WatchAsync receives events when objects are deleted.
    /// </summary>
    [Test]
    public async Task NatsObjectStore_WatchAsync_OnDelete_NotifiesHandler()
    {
        // Arrange
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts { Url = fixture.ConnectionString };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();

        var js = new NatsJSContext(conn);
        var objContext = new NatsObjContext(js);

        var bucket = "OBJ_WATCH_DELETE_TEST";
        await objContext.CreateObjectStoreAsync(new NatsObjConfig(bucket)
        {
            Storage = NatsObjStorageType.Memory
        });

        await using var store = new NatsObjectStore(objContext, bucket, NullLogger<NatsObjectStore>.Instance);

        // First, put an object
        var data = "To be deleted"u8.ToArray();
        using (var stream = new MemoryStream(data))
        {
            await store.PutAsync("delete-me.txt", stream);
        }

        var deleteReceived = new TaskCompletionSource<bool>();
        var receivedObjects = new List<ObjectInfo>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        // Start watching
        var watchTask = store.WatchAsync(async info =>
        {
            receivedObjects.Add(info);
            if (info.Key == "delete-me.txt" && info.Deleted)
            {
                deleteReceived.TrySetResult(true);
            }
            await Task.CompletedTask;
        }, cts.Token);

        await Task.Delay(500);

        // Act: Delete the object
        await store.DeleteAsync("delete-me.txt");

        // Wait for delete event
        await deleteReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await cts.CancelAsync();
        try { await watchTask; } catch (OperationCanceledException) { }

        // Assert
        var deletedObj = receivedObjects.FirstOrDefault(o => o.Key == "delete-me.txt" && o.Deleted);
        deletedObj.ShouldNotBeNull();
    }

    /// <summary>
    /// Verify that NatsObjectStore WatchAsync receives multiple object events.
    /// </summary>
    [Test]
    public async Task NatsObjectStore_WatchAsync_MultipleObjects_ReceivesAllEvents()
    {
        // Arrange
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts { Url = fixture.ConnectionString };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();

        var js = new NatsJSContext(conn);
        var objContext = new NatsObjContext(js);

        var bucket = "OBJ_WATCH_MULTI_TEST";
        await objContext.CreateObjectStoreAsync(new NatsObjConfig(bucket)
        {
            Storage = NatsObjStorageType.Memory
        });

        await using var store = new NatsObjectStore(objContext, bucket, NullLogger<NatsObjectStore>.Instance);

        var receivedNames = new HashSet<string>();
        var allReceived = new TaskCompletionSource<bool>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        // Start watching
        var watchTask = store.WatchAsync(async info =>
        {
            if (!info.Deleted)
            {
                receivedNames.Add(info.Key);
                if (receivedNames.Count >= 3)
                {
                    allReceived.TrySetResult(true);
                }
            }
            await Task.CompletedTask;
        }, cts.Token);

        await Task.Delay(500);

        // Act: Put multiple objects
        var objectsToCreate = new[] { "file1.txt", "file2.txt", "file3.txt" };
        foreach (var name in objectsToCreate)
        {
            using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes($"Content of {name}"));
            await store.PutAsync(name, stream);
        }

        await allReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await cts.CancelAsync();
        try { await watchTask; } catch (OperationCanceledException) { }

        // Assert
        foreach (var name in objectsToCreate)
        {
            receivedNames.ShouldContain(name);
        }
    }

    #endregion

    #region NatsObjectStore Additional Tests (from TEST2.MD)

    /// <summary>
    /// Verify GetInfoAsync returns correct metadata for an object.
    /// </summary>
    [Test]
    public async Task NatsObjectStore_GetInfoAsync_ReturnsMetadata()
    {
        // Arrange
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts { Url = fixture.ConnectionString };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();

        var js = new NatsJSContext(conn);
        var objContext = new NatsObjContext(js);

        var bucket = "OBJ_GETINFO_TEST";
        await objContext.CreateObjectStoreAsync(new NatsObjConfig(bucket)
        {
            Storage = NatsObjStorageType.Memory
        });

        await using var store = new NatsObjectStore(objContext, bucket, NullLogger<NatsObjectStore>.Instance);

        // Put an object
        var data = "Test data for GetInfo"u8.ToArray();
        using (var stream = new MemoryStream(data))
        {
            await store.PutAsync("info-test.txt", stream);
        }

        // Act
        var info = await store.GetInfoAsync("info-test.txt");

        // Assert
        info.ShouldNotBeNull();
        info!.Key.ShouldBe("info-test.txt");
        info.Size.ShouldBe(data.Length);
        info.Bucket.ShouldBe(bucket);
        info.Deleted.ShouldBeFalse();
    }

    /// <summary>
    /// Verify ListAsync returns all objects in the bucket.
    /// </summary>
    [Test]
    public async Task NatsObjectStore_ListAsync_ReturnsAllObjects()
    {
        // Arrange
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts { Url = fixture.ConnectionString };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();

        var js = new NatsJSContext(conn);
        var objContext = new NatsObjContext(js);

        var bucket = "OBJ_LIST_TEST";
        await objContext.CreateObjectStoreAsync(new NatsObjConfig(bucket)
        {
            Storage = NatsObjStorageType.Memory
        });

        await using var store = new NatsObjectStore(objContext, bucket, NullLogger<NatsObjectStore>.Instance);

        // Put multiple objects
        var objectNames = new[] { "list-a.txt", "list-b.txt", "list-c.txt" };
        foreach (var name in objectNames)
        {
            using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes($"Content of {name}"));
            await store.PutAsync(name, stream);
        }

        // Act
        var objects = await store.ListAsync();

        // Assert
        var objectList = objects.ToList();
        objectList.Count.ShouldBe(3);
        foreach (var name in objectNames)
        {
            objectList.Any(o => o.Key == name).ShouldBeTrue();
        }
    }

    /// <summary>
    /// Verify ListAsync on an empty bucket returns empty collection without hanging.
    /// This addresses the potential hanging issue mentioned in TEST2.MD.
    /// </summary>
    [Test]
    public async Task NatsObjectStore_ListAsync_EmptyBucket_ReturnsEmptyWithoutHanging()
    {
        // Arrange
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts { Url = fixture.ConnectionString };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();

        var js = new NatsJSContext(conn);
        var objContext = new NatsObjContext(js);

        var bucket = "OBJ_LIST_EMPTY_TEST";
        await objContext.CreateObjectStoreAsync(new NatsObjConfig(bucket)
        {
            Storage = NatsObjStorageType.Memory
        });

        await using var store = new NatsObjectStore(objContext, bucket, NullLogger<NatsObjectStore>.Instance);

        // Act - Use timeout to detect hanging
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var listTask = store.ListAsync(cancellationToken: cts.Token);

        // This should complete quickly, not hang
        var objects = await listTask;

        // Assert
        var objectList = objects.ToList();
        objectList.Count.ShouldBe(0);
    }

    #endregion
}

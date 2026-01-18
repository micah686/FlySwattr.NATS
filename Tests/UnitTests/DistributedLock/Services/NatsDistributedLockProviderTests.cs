using System.Reflection;
using FlySwattr.NATS.DistributedLock.Services;
using Medallion.Threading;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.KeyValueStore;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Shouldly;
using TUnit.Core;

namespace UnitTests.DistributedLock.Services;

[Property("nTag", "DistributedLock")]
public class NatsDistributedLockProviderTests
{
    private readonly INatsKVContext _kvContext;
    private readonly INatsKVStore _kvStore;
    private readonly ILogger<NatsDistributedLockProvider> _logger;
    private readonly NatsDistributedLockProvider _sut;
    private const string BucketName = "topology_locks";
    private const string LockKey = "test-lock";

    public NatsDistributedLockProviderTests()
    {
        _kvContext = Substitute.For<INatsKVContext>();
        _kvStore = Substitute.For<INatsKVStore>();
        _logger = Substitute.For<ILogger<NatsDistributedLockProvider>>();

        _kvContext.GetStoreAsync(BucketName, Arg.Any<CancellationToken>())
            .Returns(new ValueTask<INatsKVStore>(_kvStore));

        _kvContext.CreateStoreAsync(Arg.Any<NatsKVConfig>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<INatsKVStore>(_kvStore));

        _sut = new NatsDistributedLockProvider(_kvContext, _logger);
    }

    [Test]
    public async Task TryAcquireAsync_ShouldCreateLock_WhenKeyDoesNotExist()
    {
        // Arrange
        var lockObj = _sut.CreateLock(LockKey);
        
        // Mock successful creation (key didn't exist)
        _kvStore.CreateAsync(LockKey, Arg.Any<byte[]>(), cancellationToken: Arg.Any<CancellationToken>())
            .Returns(new ValueTask<ulong>(1));

        // Act
        var handle = await lockObj.TryAcquireAsync();

        // Assert
        handle.ShouldNotBeNull();
        await _kvStore.Received(1).CreateAsync(LockKey, Arg.Any<byte[]>(), cancellationToken: Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task TryAcquireAsync_ShouldFail_WhenLockHeldByOther()
    {
        // Arrange
        var lockObj = _sut.CreateLock(LockKey);
        var timeout = TimeSpan.FromMilliseconds(100); // Short timeout for test

        // Simulate lock held: CreateAsync throws (key exists)
        _kvStore.CreateAsync(LockKey, Arg.Any<byte[]>(), cancellationToken: Arg.Any<CancellationToken>())
            .Returns(x => ValueTask.FromException<ulong>(CreateException<NatsKVCreateException>()));

        // GetEntryAsync returns non-empty value (lock held)
        var entry = CreateEntry(BucketName, LockKey, new byte[] { 1 }, 1);
        _kvStore.GetEntryAsync<byte[]>(LockKey, cancellationToken: Arg.Any<CancellationToken>())
            .Returns(new ValueTask<NatsKVEntry<byte[]>>(entry));

        // Act
        var handle = await lockObj.TryAcquireAsync(timeout);

        // Assert
        handle.ShouldBeNull();
    }

    [Test]
    public async Task TryAcquireAsync_ShouldAcquire_WhenTombstoneExists()
    {
        // Arrange
        var lockObj = _sut.CreateLock(LockKey);

        // 1. CreateAsync throws (key exists)
        _kvStore.CreateAsync(LockKey, Arg.Any<byte[]>(), cancellationToken: Arg.Any<CancellationToken>())
            .Returns(x => ValueTask.FromException<ulong>(CreateException<NatsKVCreateException>()));

        // 2. GetEntryAsync returns empty value (tombstone)
        var entry = CreateEntry(BucketName, LockKey, Array.Empty<byte>(), 10);
        _kvStore.GetEntryAsync<byte[]>(LockKey, cancellationToken: Arg.Any<CancellationToken>())
            .Returns(new ValueTask<NatsKVEntry<byte[]>>(entry));

        // 3. UpdateAsync succeeds (acquires lock from tombstone)
        _kvStore.UpdateAsync(LockKey, Arg.Any<byte[]>(), 10, serializer: null, cancellationToken: Arg.Any<CancellationToken>())
            .Returns(new ValueTask<ulong>(11));

        // Act
        var handle = await lockObj.TryAcquireAsync();

        // Assert
        handle.ShouldNotBeNull();
        await _kvStore.Received(1).UpdateAsync(LockKey, Arg.Any<byte[]>(), 10, serializer: null, cancellationToken: Arg.Any<CancellationToken>());
    }

    private TException CreateException<TException>() where TException : Exception
    {
#pragma warning disable SYSLIB0050
        return (TException)System.Runtime.Serialization.FormatterServices.GetUninitializedObject(typeof(TException));
#pragma warning restore SYSLIB0050
    }

    [Test]
    public async Task DisposeAsync_ShouldReleaseLock()
    {
        // Arrange
        var lockObj = _sut.CreateLock(LockKey);
        ulong initialRevision = 123;

        _kvStore.CreateAsync(LockKey, Arg.Any<byte[]>(), cancellationToken: Arg.Any<CancellationToken>())
            .Returns(new ValueTask<ulong>(initialRevision));

        var handle = await lockObj.TryAcquireAsync();
        handle.ShouldNotBeNull();

        // Act
        await handle.DisposeAsync();

        // Assert
        // Should call UpdateAsync with empty bytes (tombstone) and matching revision
        await _kvStore.Received(1).UpdateAsync(
            LockKey, 
            Arg.Is<byte[]>(b => b.Length == 0), 
            initialRevision, 
            serializer: null,
            cancellationToken: Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Heartbeat_ShouldUpdateLockPeriodically()
    {
        // Arrange
        // Use a short TTL to trigger heartbeat quickly
        var shortTtl = TimeSpan.FromMilliseconds(200);
        var sut = new NatsDistributedLockProvider(_kvContext, _logger, shortTtl);
        var lockObj = sut.CreateLock(LockKey);
        ulong initialRevision = 1;

        _kvStore.CreateAsync(LockKey, Arg.Any<byte[]>(), cancellationToken: Arg.Any<CancellationToken>())
            .Returns(new ValueTask<ulong>(initialRevision));
        
        // Mock update success for heartbeat
        _kvStore.UpdateAsync(LockKey, Arg.Any<byte[]>(), Arg.Any<ulong>(), serializer: null, cancellationToken: Arg.Any<CancellationToken>())
            .Returns(new ValueTask<ulong>(initialRevision + 1));

        // Act
        await using var handle = await lockObj.TryAcquireAsync();
        
        // Wait for heartbeat interval (TTL / 2 = 100ms)
        await Task.Delay(600);

        // Assert
        // Should have called UpdateAsync at least once for heartbeat (with non-empty value)
        await _kvStore.Received().UpdateAsync(
            LockKey, 
            Arg.Is<byte[]>(b => b.Length > 0), 
            initialRevision, 
            serializer: null, 
            Arg.Any<CancellationToken>());
    }

    private NatsKVEntry<T> CreateEntry<T>(string bucket, string key, T value, ulong revision)
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
            
            return (NatsKVEntry<T>)boxed;
        }
        catch
        {
            return default;
        }
    }
}

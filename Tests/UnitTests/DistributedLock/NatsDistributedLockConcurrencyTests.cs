using FlySwattr.NATS.DistributedLock.Services;
using Medallion.Threading;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.KeyValueStore;
using NSubstitute;
using Shouldly;
using TUnit.Core;

namespace UnitTests.DistributedLock;

[Property("nTag", "DistributedLock")]
public class NatsDistributedLockConcurrencyTests
{
    private readonly INatsKVContext _kvContext;
    private readonly INatsKVStore _kvStore;
    private readonly ILogger<NatsDistributedLockProvider> _logger;
    private readonly NatsDistributedLockProvider _sut;
    private const string BucketName = "topology_locks";
    private const string LockKey = "concurrency-lock";

    public NatsDistributedLockConcurrencyTests()
    {
        _kvContext = Substitute.For<INatsKVContext>();
        _kvStore = Substitute.For<INatsKVStore>();
        _logger = Substitute.For<ILogger<NatsDistributedLockProvider>>();

        _kvContext.GetStoreAsync(BucketName, Arg.Any<CancellationToken>())
            .Returns(new ValueTask<INatsKVStore>(_kvStore));

        _kvContext.CreateStoreAsync(Arg.Any<NatsKVConfig>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<INatsKVStore>(_kvStore));

        // Use a short TTL to facilitate heartbeat testing
        _sut = new NatsDistributedLockProvider(_kvContext, _logger, TimeSpan.FromMilliseconds(500));
    }

    [Test]
    public async Task Heartbeat_ShouldDetectSplitBrain_AndCancelHandleLostToken()
    {
        // Arrange
        ulong initialRevision = 100;
        
        // 1. Initial acquisition succeeds
        _kvStore.CreateAsync(LockKey, Arg.Any<byte[]>(), cancellationToken: Arg.Any<CancellationToken>())
            .Returns(new ValueTask<ulong>(initialRevision));

        // 2. Heartbeat update FAILS with Revision Mismatch
        // This simulates another node acquiring the lock (changing the revision)
        _kvStore.UpdateAsync(LockKey, Arg.Any<byte[]>(), initialRevision, serializer: null, cancellationToken: Arg.Any<CancellationToken>())
            .Returns(x => ValueTask.FromException<ulong>(CreateException<NatsKVWrongLastRevisionException>()));

        // Act
        // Act
        var lockObj = _sut.CreateLock(LockKey);
        var handle = await lockObj.TryAcquireAsync();
        await Assert.That(handle).IsNotNull();

        // Wait for heartbeat interval (TTL / 2 = 250ms) plus some buffer
        await Task.Delay(1000);

        // Assert
        // The HandleLostToken should be cancelled because of the revision mismatch
        await Assert.That(handle!.HandleLostToken.IsCancellationRequested).IsTrue();
        
        // Ensure we logged the warning
        _logger.Received().Log(
            LogLevel.Warning, 
            Arg.Any<EventId>(), 
            Arg.Is<object>(o => o.ToString()!.Contains("lost due to revision mismatch")), 
            Arg.Any<Exception>(), 
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Test]
    public async Task DisposeAsync_ShouldNotReleaseLock_IfSplitBrainOccurred()
    {
        // Arrange
        ulong initialRevision = 100;

        // 1. Initial acquisition succeeds
        _kvStore.CreateAsync(LockKey, Arg.Any<byte[]>(), cancellationToken: Arg.Any<CancellationToken>())
            .Returns(new ValueTask<ulong>(initialRevision));

        // 2. Acquire the lock
        // Act
        var lockObj = _sut.CreateLock(LockKey);
        var handle = await lockObj.TryAcquireAsync();
        await Assert.That(handle).IsNotNull();

        // 3. Setup UpdateAsync (used for release) to throw Revision Mismatch
        // This simulates that while we held the lock, another node took it
        _kvStore.UpdateAsync(
                LockKey, 
                Arg.Is<byte[]>(b => b.Length == 0), // Tombstone
                initialRevision, 
                serializer: null, 
                cancellationToken: Arg.Any<CancellationToken>())
            .Returns(x => ValueTask.FromException<ulong>(CreateException<NatsKVWrongLastRevisionException>()));

        // Act
        await handle!.DisposeAsync();

        // Assert
        // Should NOT throw exception (swallowed)
        // Should log debug message about mismatch
        _logger.Received().Log(
            LogLevel.Debug, 
            Arg.Any<EventId>(), 
            Arg.Is<object>(o => o.ToString()!.Contains("already released or reacquired")), 
            Arg.Any<Exception>(), 
            Arg.Any<Func<object, Exception?, string>>());
    }

    private TException CreateException<TException>() where TException : Exception
    {
#pragma warning disable SYSLIB0050
        return (TException)System.Runtime.Serialization.FormatterServices.GetUninitializedObject(typeof(TException));
#pragma warning restore SYSLIB0050
    }
}

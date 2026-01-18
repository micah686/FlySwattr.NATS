using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core;
using FlySwattr.NATS.Topology.Managers;
using Medallion.Threading;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.KeyValueStore;
using NATS.Client.ObjectStore;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Shouldly;
using TUnit.Core;

namespace UnitTests.ControlPlane;

[Property("nTag", "Topology")]
public class NatsTopologyManagerTests
{
    private readonly INatsJSContext _jsContext;
    private readonly INatsKVContext _kvContext;
    private readonly INatsObjContext _objContext;
    private readonly ILogger<NatsTopologyManager> _logger;
    private readonly IDlqPolicyRegistry _dlqRegistry;
    private readonly IDistributedLockProvider _lockProvider;
    private readonly NatsTopologyManager _sut;

    public NatsTopologyManagerTests()
    {
        _jsContext = Substitute.For<INatsJSContext>();
        _kvContext = Substitute.For<INatsKVContext>();
        _objContext = Substitute.For<INatsObjContext>();
        _logger = Substitute.For<ILogger<NatsTopologyManager>>();
        _dlqRegistry = Substitute.For<IDlqPolicyRegistry>();
        _lockProvider = Substitute.For<IDistributedLockProvider>();

        // Setup lock provider to return a valid lock handle
        var lockHandle = Substitute.For<IDistributedSynchronizationHandle>();
        var mockLock = Substitute.For<IDistributedLock>();
        
        _lockProvider.CreateLock(Arg.Any<string>()).Returns(mockLock);
        
        // Mock TryAcquireAsync on the lock itself
        mockLock.TryAcquireAsync(Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
            .Returns(lockHandle);

        _sut = new NatsTopologyManager(_jsContext, _kvContext, _objContext, _logger, _dlqRegistry, _lockProvider);
    }

    [Test]
    public async Task EnsureStreamAsync_ShouldCreateStream_WhenNotExists()
    {
        // Arrange
        var spec = new StreamSpec { Name = StreamName.From("test-stream"), Subjects = ["test.>"] };
        
        // Mock GetStreamAsync to throw 404 (Not Found)
        _jsContext.GetStreamAsync(spec.Name.Value, cancellationToken: Arg.Any<CancellationToken>())
            .Throws(CreateNatsJSApiException(404));

        // Act
        await _sut.EnsureStreamAsync(spec);

        // Assert
        await _jsContext.Received(1).CreateStreamAsync(
            Arg.Is<StreamConfig>(c => c.Name == spec.Name.Value && c.Subjects != null && c.Subjects.Contains("test.>")), 
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task EnsureStreamAsync_ShouldUpdateStream_WhenExistsAndConfigChanged()
    {
        // Arrange
        var spec = new StreamSpec { Name = StreamName.From("test-stream"), MaxBytes = 1024 };
        
        // Existing stream with different config
        var existingConfig = new StreamConfig("test-stream", new[] { "old.>" }) { MaxBytes = 512, NumReplicas = 1 };
        var existingStream = Substitute.For<INatsJSStream>();
        existingStream.Info.Returns(new StreamInfo { Config = existingConfig });

        _jsContext.GetStreamAsync(spec.Name.Value, cancellationToken: Arg.Any<CancellationToken>())
            .Returns(existingStream);

        // Act
        await _sut.EnsureStreamAsync(spec);

        // Assert
        await _jsContext.Received(1).UpdateStreamAsync(
            Arg.Is<StreamConfig>(c => c.Name == spec.Name.Value && c.MaxBytes == 1024), 
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task EnsureStreamAsync_ShouldNotUpdate_WhenConfigIdentical()
    {
        // Arrange
        var spec = new StreamSpec { Name = StreamName.From("test-stream"), Subjects = ["test.>"], MaxBytes = 1024 };
        
        // Existing stream with same config
        var existingConfig = new StreamConfig("test-stream", new[] { "test.>" }) 
        { 
            MaxBytes = 1024, 
            NumReplicas = 1,
            MaxMsgSize = -1,
            MaxAge = TimeSpan.Zero,
            Storage = StreamConfigStorage.File,
            Retention = StreamConfigRetention.Limits,
            Discard = StreamConfigDiscard.Old
        };
        var existingStream = Substitute.For<INatsJSStream>();
        existingStream.Info.Returns(new StreamInfo { Config = existingConfig });

        _jsContext.GetStreamAsync(spec.Name.Value, cancellationToken: Arg.Any<CancellationToken>())
            .Returns(existingStream);

        // Act
        await _sut.EnsureStreamAsync(spec);

        // Assert
        await _jsContext.DidNotReceive().UpdateStreamAsync(Arg.Any<StreamConfig>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task EnsureConsumerAsync_ShouldCreateConsumer_WhenNotExists()
    {
        // Arrange
        var spec = new ConsumerSpec 
        { 
            StreamName = StreamName.From("test-stream"), 
            DurableName = ConsumerName.From("test-consumer") 
        };

        // Act
        await _sut.EnsureConsumerAsync(spec);

        // Assert
        await _jsContext.Received(1).CreateOrUpdateConsumerAsync(
            spec.StreamName.Value,
            Arg.Is<ConsumerConfig>(c => c.DurableName == spec.DurableName.Value),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task EnsureConsumerAsync_ShouldRecreateConsumer_OnImmutablePropertyError()
    {
        // Arrange
        var spec = new ConsumerSpec 
        { 
            StreamName = StreamName.From("test-stream"), 
            DurableName = ConsumerName.From("test-consumer") 
        };

        // First call fails with immutable error (10058), second succeeds
        _jsContext.CreateOrUpdateConsumerAsync(spec.StreamName.Value, Arg.Any<ConsumerConfig>(), Arg.Any<CancellationToken>())
            .Returns(
                x => throw CreateNatsJSApiException(400, 10058, "consumer name already in use"), // 1st call throws
                x => Substitute.For<INatsJSConsumer>() // 2nd call (after delete) returns consumer
            );

        // Act
        await _sut.EnsureConsumerAsync(spec);

        // Assert
        // Should have called delete once
        await _jsContext.Received(1).DeleteConsumerAsync(spec.StreamName.Value, spec.DurableName.Value, Arg.Any<CancellationToken>());
        // Should have called create twice (initial attempt + recreation)
        await _jsContext.Received(2).CreateOrUpdateConsumerAsync(spec.StreamName.Value, Arg.Any<ConsumerConfig>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task EnsureBucketAsync_ShouldCreateBucket()
    {
        // Arrange
        var name = BucketName.From("test-bucket");

        // Act
        await _sut.EnsureBucketAsync(name, StorageType.File);

        // Assert
        await _kvContext.Received(1).CreateStoreAsync(
            Arg.Is<NatsKVConfig>(c => c.Bucket == name.Value), 
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task EnsureObjectStoreAsync_ShouldCreateObjectStore()
    {
        // Arrange
        var name = BucketName.From("test-obj");

        // Act
        await _sut.EnsureObjectStoreAsync(name, StorageType.File);

        // Assert
        await _objContext.Received(1).CreateObjectStoreAsync(
            Arg.Is<NatsObjConfig>(c => c.Bucket == name.Value), 
            Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// 6.1 Concurrent Provisioning and Thundering Herds Test
    /// 
    /// Scenario: In a microservices environment, 5 instances start simultaneously
    /// and all attempt to provision the same stream topology.
    /// 
    /// The code uses DistributedLock (topology_lock_...) to synchronize this provisioning.
    /// This test verifies that startup logic is idempotent and thread-safe across distributed instances.
    /// 
    /// Assertions:
    /// - Only one CreateStreamAsync call actually reaches the NATS mock
    /// - The other 4 tasks wait and complete successfully without throwing LockNotAcquiredException
    /// </summary>
    [Test]
    public async Task ConcurrentProvisioning_ShouldSerializeAccess_WithDistributedLock()
    {
        // Arrange
        var jsContext = Substitute.For<INatsJSContext>();
        var kvContext = Substitute.For<INatsKVContext>();
        var objContext = Substitute.For<INatsObjContext>();
        var logger = Substitute.For<ILogger<NatsTopologyManager>>();
        var dlqRegistry = Substitute.For<IDlqPolicyRegistry>();
        var lockProvider = Substitute.For<IDistributedLockProvider>();
        
        var spec = new StreamSpec 
        { 
            Name = StreamName.From("concurrent-test-stream"), 
            Subjects = ["concurrent.>"] 
        };

        // Setup a gate to synchronize tasks at the lock acquisition point
        var lockAcquireGate = new SemaphoreSlim(0);
        var createCallCount = 0;
        var lockHeld = new SemaphoreSlim(1, 1); // Simulate the lock with a semaphore
        
        var mockLock = Substitute.For<IDistributedLock>();
        lockProvider.CreateLock(Arg.Any<string>()).Returns(mockLock);

        // Mock the lock to simulate serialized access
        mockLock.TryAcquireAsync(Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                // Signal that we're trying to acquire
                lockAcquireGate.Release();
                
                // Simulate lock acquisition with semaphore (only one task proceeds at a time)
                lockHeld.Wait();
                
                var handle = Substitute.For<IDistributedSynchronizationHandle>();
                handle.When(x => x.DisposeAsync()).Do(_ => lockHeld.Release());
                return new ValueTask<IDistributedSynchronizationHandle?>(handle);
            });

        // Track stream existence across calls - first returns null (create), subsequent return existing
        var streamCreated = false;
        jsContext.GetStreamAsync(spec.Name.Value, cancellationToken: Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                if (streamCreated)
                {
                    // Return existing stream config
                    var existingConfig = new StreamConfig(spec.Name.Value, spec.Subjects)
                    {
                        Storage = StreamConfigStorage.File,
                        Retention = StreamConfigRetention.Limits,
                        NumReplicas = 1,
                        MaxBytes = -1,
                        MaxMsgSize = -1,
                        MaxAge = TimeSpan.Zero,
                        Discard = StreamConfigDiscard.Old
                    };
                    var existingStream = Substitute.For<INatsJSStream>();
                    existingStream.Info.Returns(new StreamInfo { Config = existingConfig });
                    return new ValueTask<INatsJSStream>(existingStream);
                }
                // First caller gets 404 (not found)
                throw CreateNatsJSApiException(404);
            });

        jsContext.CreateStreamAsync(Arg.Any<StreamConfig>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                Interlocked.Increment(ref createCallCount);
                streamCreated = true;
                return new ValueTask<INatsJSStream>(Substitute.For<INatsJSStream>());
            });

        var sut = new NatsTopologyManager(jsContext, kvContext, objContext, logger, dlqRegistry, lockProvider);

        // Act: Spin up 5 concurrent tasks
        const int concurrentInstances = 5;
        var tasks = new Task[concurrentInstances];
        for (int i = 0; i < concurrentInstances; i++)
        {
            tasks[i] = sut.EnsureStreamAsync(spec);
        }

        // Wait for all tasks to attempt lock acquisition
        for (int i = 0; i < concurrentInstances; i++)
        {
            await lockAcquireGate.WaitAsync(TimeSpan.FromSeconds(5));
        }

        // All tasks should complete successfully
        await Task.WhenAll(tasks);

        // Assert
        // Only one CreateStreamAsync call should reach the NATS mock
        createCallCount.ShouldBe(1, "Only the first task to acquire the lock should create the stream");
        
        await jsContext.Received(1).CreateStreamAsync(
            Arg.Any<StreamConfig>(), 
            Arg.Any<CancellationToken>());
        
        // No exceptions should be thrown (all tasks completed successfully)
        foreach (var task in tasks)
        {
            task.IsCompletedSuccessfully.ShouldBeTrue("All tasks should complete successfully without exceptions");
        }
    }

    /// <summary>
    /// 6.2 Immutable Property Conflicts - Stream Immutability Safety Test
    /// 
    /// NATS JetStream streams have immutable properties (e.g., Storage type: File vs. Memory).
    /// Attempting to update these should throw an error.
    /// 
    /// Scenario:
    /// - Provision a Stream with Storage = File
    /// - Change the configuration to Storage = Memory and restart the service
    /// 
    /// Assertions:
    /// - The service should fail fast (throw InvalidOperationException)
    /// - It must NOT silently ignore the config change (leaving the system inconsistent)
    /// - It must NOT delete and recreate the stream (causing massive data loss)
    /// </summary>
    [Test]
    public async Task EnsureStreamAsync_ShouldThrowInvalidOperationException_WhenStorageTypeChanges()
    {
        // Arrange: Existing stream with Storage = File
        var spec = new StreamSpec 
        { 
            Name = StreamName.From("immutable-test-stream"), 
            Subjects = ["immutable.>"],
            StorageType = StorageType.Memory // Config wants Memory
        };

        var existingConfig = new StreamConfig(spec.Name.Value, spec.Subjects)
        {
            Storage = StreamConfigStorage.File, // Existing is File
            Retention = StreamConfigRetention.Limits,
            NumReplicas = 1,
            MaxBytes = -1,
            MaxMsgSize = -1,
            MaxAge = TimeSpan.Zero,
            Discard = StreamConfigDiscard.Old
        };
        
        var existingStream = Substitute.For<INatsJSStream>();
        existingStream.Info.Returns(new StreamInfo { Config = existingConfig });

        _jsContext.GetStreamAsync(spec.Name.Value, cancellationToken: Arg.Any<CancellationToken>())
            .Returns(existingStream);

        // Act & Assert
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await _sut.EnsureStreamAsync(spec));

        // Assert: Should fail with clear message about storage type
        exception.ShouldNotBeNull();
        exception!.Message.ShouldContain("Storage type cannot be changed");
        exception.Message.ShouldContain("File");
        exception.Message.ShouldContain("Memory");

        // Assert: Should NOT try to update (fail-fast before update attempt)
        await _jsContext.DidNotReceive().UpdateStreamAsync(
            Arg.Any<StreamConfig>(), 
            Arg.Any<CancellationToken>());

        // Assert: Should NOT delete and recreate (would cause massive data loss)
        await _jsContext.DidNotReceive().DeleteStreamAsync(
            Arg.Any<string>(), 
            Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// 6.2 Immutable Property Conflicts - Retention Policy Change Test
    /// 
    /// Additional test for retention policy changes which are also immutable.
    /// </summary>
    [Test]
    public async Task EnsureStreamAsync_ShouldThrowInvalidOperationException_WhenRetentionPolicyChanges()
    {
        // Arrange: Existing stream with Retention = Limits
        var spec = new StreamSpec 
        { 
            Name = StreamName.From("retention-test-stream"), 
            Subjects = ["retention.>"],
            StorageType = StorageType.File,
            RetentionPolicy = StreamRetention.Interest // Config wants Interest
        };

        var existingConfig = new StreamConfig(spec.Name.Value, spec.Subjects)
        {
            Storage = StreamConfigStorage.File,
            Retention = StreamConfigRetention.Limits, // Existing is Limits
            NumReplicas = 1,
            MaxBytes = -1,
            MaxMsgSize = -1,
            MaxAge = TimeSpan.Zero,
            Discard = StreamConfigDiscard.Old
        };
        
        var existingStream = Substitute.For<INatsJSStream>();
        existingStream.Info.Returns(new StreamInfo { Config = existingConfig });

        _jsContext.GetStreamAsync(spec.Name.Value, cancellationToken: Arg.Any<CancellationToken>())
            .Returns(existingStream);

        // Act & Assert
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await _sut.EnsureStreamAsync(spec));

        // Assert: Should fail fast with clear error message
        exception.ShouldNotBeNull();
        exception!.Message.ShouldContain("Retention policy cannot be changed");
        
        // Assert: Should NOT modify or delete
        await _jsContext.DidNotReceive().UpdateStreamAsync(Arg.Any<StreamConfig>(), Arg.Any<CancellationToken>());
        await _jsContext.DidNotReceive().DeleteStreamAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    private static NatsJSApiException CreateNatsJSApiException(int code, int errCode = 0, string description = "")
    {
        var error = new ApiError { Code = code, ErrCode = errCode, Description = description };
        return new NatsJSApiException(error);
    }
}

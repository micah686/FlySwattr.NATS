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

    private static NatsJSApiException CreateNatsJSApiException(int code, int errCode = 0, string description = "")
    {
        var error = new ApiError { Code = code, ErrCode = errCode, Description = description };
        return new NatsJSApiException(error);
    }
}

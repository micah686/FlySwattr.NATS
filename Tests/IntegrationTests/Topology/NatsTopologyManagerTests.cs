using FlySwattr.NATS.Core;
using FlySwattr.NATS.DistributedLock.Services;
using FlySwattr.NATS.Topology.Managers;
using FlySwattr.NATS.Abstractions;
using IntegrationTests.Infrastructure;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.KeyValueStore;
using NATS.Client.ObjectStore;
using Shouldly;
using TUnit.Core;

namespace IntegrationTests.Topology;

[Property("nTag", "Topology")]
public class NatsTopologyManagerTests
{
    [Test]
    public async Task EnsureStreamAsync_ShouldCreate_WhenNotExists()
    {
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts { Url = fixture.ConnectionString };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();

        var js = new NatsJSContext(conn);
        var kv = new NatsKVContext(js);
        var obj = new NatsObjContext(js);

        // Use a shorter TTL for tests to avoid long waits
        var lockProvider = new NatsDistributedLockProvider(kv, NullLogger<NatsDistributedLockProvider>.Instance, TimeSpan.FromSeconds(5));
        var dlqRegistry = new DlqPolicyRegistry(NullLogger<DlqPolicyRegistry>.Instance);
        var manager = new NatsTopologyManager(js, kv, obj, NullLogger<NatsTopologyManager>.Instance, dlqRegistry, lockProvider);

        var spec = new StreamSpec
        {
            Name = StreamName.From("TEST_STREAM"),
            Subjects = new[] { "test.>" },
            MaxBytes = 1024 * 1024,
            StorageType = StorageType.Memory,
            RetentionPolicy = StreamRetention.Limits
        };

        await manager.EnsureStreamAsync(spec);

        // Verify stream exists
        var stream = await js.GetStreamAsync("TEST_STREAM");
        stream.ShouldNotBeNull();
        stream.Info.Config.Name.ShouldBe("TEST_STREAM");
        stream.Info.Config.Subjects.ShouldNotBeNull();
        stream.Info.Config.Subjects.ShouldContain("test.>");
    }

    [Test]
    public async Task EnsureStreamAsync_ShouldUpdate_WhenConfigChanged()
    {
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();
    
        var opts = new NatsOpts { Url = fixture.ConnectionString };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();
    
        var js = new NatsJSContext(conn);
        var kv = new NatsKVContext(js);
        var obj = new NatsObjContext(js);
    
        var lockProvider = new NatsDistributedLockProvider(kv, NullLogger<NatsDistributedLockProvider>.Instance, TimeSpan.FromSeconds(30));
        var dlqRegistry = new DlqPolicyRegistry(NullLogger<DlqPolicyRegistry>.Instance);
        var manager = new NatsTopologyManager(js, kv, obj, NullLogger<NatsTopologyManager>.Instance, dlqRegistry, lockProvider);
    
        // Create initial stream
        var spec = new StreamSpec
        {
            Name = StreamName.From("UPDATE_STREAM"),
            Subjects = new[] { "update.>" },
            MaxBytes = 1024 * 1024,
            StorageType = StorageType.File,
            RetentionPolicy = StreamRetention.Limits
        };
    
        await manager.EnsureStreamAsync(spec);
    
        // Update mutable property (MaxBytes)
        spec.MaxBytes = 2 * 1024 * 1024;
        await manager.EnsureStreamAsync(spec);
    
        // Verify update
        var stream = await js.GetStreamAsync("UPDATE_STREAM");
        stream.Info.Config.MaxBytes.ShouldBe(2 * 1024 * 1024);
    }
    
    [Test]
    public async Task EnsureStreamAsync_ShouldValidateImmutableProperties()
    {
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();
    
        var opts = new NatsOpts { Url = fixture.ConnectionString };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();
    
        var js = new NatsJSContext(conn);
        var kv = new NatsKVContext(js);
        var obj = new NatsObjContext(js);
    
        var lockProvider = new NatsDistributedLockProvider(kv, NullLogger<NatsDistributedLockProvider>.Instance, TimeSpan.FromSeconds(30));
        var dlqRegistry = new DlqPolicyRegistry(NullLogger<DlqPolicyRegistry>.Instance);
        var manager = new NatsTopologyManager(js, kv, obj, NullLogger<NatsTopologyManager>.Instance, dlqRegistry, lockProvider);
    
        // Create initial stream
        var spec = new StreamSpec
        {
            Name = StreamName.From("IMMUTABLE_STREAM"),
            Subjects = new[] { "immutable.>" },
            StorageType = StorageType.File,
            RetentionPolicy = StreamRetention.Limits,
            Replicas = 1
        };
    
        await manager.EnsureStreamAsync(spec);
    
        // Try to change immutable property (Storage)
        spec.StorageType = StorageType.Memory;
    
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await manager.EnsureStreamAsync(spec);
        });
    }
    
    [Test]
    public async Task EnsureConsumerAsync_ShouldCreate_WhenNotExists()
    {
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();
    
        var opts = new NatsOpts { Url = fixture.ConnectionString };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();
    
        var js = new NatsJSContext(conn);
        var kv = new NatsKVContext(js);
        var obj = new NatsObjContext(js);
    
        var lockProvider = new NatsDistributedLockProvider(kv, NullLogger<NatsDistributedLockProvider>.Instance, TimeSpan.FromSeconds(30));
        var dlqRegistry = new DlqPolicyRegistry(NullLogger<DlqPolicyRegistry>.Instance);
        var manager = new NatsTopologyManager(js, kv, obj, NullLogger<NatsTopologyManager>.Instance, dlqRegistry, lockProvider);
    
        // Create stream first
        var streamSpec = new StreamSpec
        {
            Name = StreamName.From("CONSUMER_STREAM"),
            Subjects = new[] { "consumer.>" },
            StorageType = StorageType.Memory
        };
        await manager.EnsureStreamAsync(streamSpec);
    
        // Create consumer
        var consumerSpec = new ConsumerSpec
        {
            StreamName = StreamName.From("CONSUMER_STREAM"),
            DurableName = ConsumerName.From("test_consumer"),
            FilterSubject = "consumer.test",
            AckPolicy = AckPolicy.Explicit,
            DeliverPolicy = DeliverPolicy.All
        };
    
        await manager.EnsureConsumerAsync(consumerSpec);
    
        // Verify consumer exists
        var consumer = await js.GetConsumerAsync("CONSUMER_STREAM", "test_consumer");
        consumer.ShouldNotBeNull();
        consumer.Info.Config.Name.ShouldBe("test_consumer");
    }
    
    [Test]
    public async Task EnsureConsumerAsync_ShouldRecreate_WhenImmutablePropertyChanged()
    {
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();
    
        var opts = new NatsOpts { Url = fixture.ConnectionString };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();
    
        var js = new NatsJSContext(conn);
        var kv = new NatsKVContext(js);
        var obj = new NatsObjContext(js);
    
        var lockProvider = new NatsDistributedLockProvider(kv, NullLogger<NatsDistributedLockProvider>.Instance, TimeSpan.FromSeconds(30));
        var dlqRegistry = new DlqPolicyRegistry(NullLogger<DlqPolicyRegistry>.Instance);
        var manager = new NatsTopologyManager(js, kv, obj, NullLogger<NatsTopologyManager>.Instance, dlqRegistry, lockProvider);
    
        // Create stream first
        var streamSpec = new StreamSpec
        {
            Name = StreamName.From("RECREATE_STREAM"),
            Subjects = new[] { "recreate.>" },
            StorageType = StorageType.Memory
        };
        await manager.EnsureStreamAsync(streamSpec);
    
        // Create consumer with initial config
        var consumerSpec = new ConsumerSpec
        {
            StreamName = StreamName.From("RECREATE_STREAM"),
            DurableName = ConsumerName.From("recreate_consumer"),
            FilterSubject = "recreate.test",
            AckPolicy = AckPolicy.Explicit,
            DeliverPolicy = DeliverPolicy.All
        };
    
        await manager.EnsureConsumerAsync(consumerSpec);
    
        // Change immutable property (DeliverPolicy) and re-ensure
        consumerSpec.DeliverPolicy = DeliverPolicy.New;
        await manager.EnsureConsumerAsync(consumerSpec);
    
        // Verify consumer was recreated with new config
        var consumer = await js.GetConsumerAsync("RECREATE_STREAM", "recreate_consumer");
        consumer.Info.Config.DeliverPolicy.ShouldBe(ConsumerConfigDeliverPolicy.New);
    }
    
    [Test]
    public async Task EnsureBucketAsync_ShouldCreateKvBucket()
    {
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();
    
        var opts = new NatsOpts { Url = fixture.ConnectionString };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();
    
        var js = new NatsJSContext(conn);
        var kv = new NatsKVContext(js);
        var obj = new NatsObjContext(js);
    
        var lockProvider = new NatsDistributedLockProvider(kv, NullLogger<NatsDistributedLockProvider>.Instance, TimeSpan.FromSeconds(30));
        var dlqRegistry = new DlqPolicyRegistry(NullLogger<DlqPolicyRegistry>.Instance);
        var manager = new NatsTopologyManager(js, kv, obj, NullLogger<NatsTopologyManager>.Instance, dlqRegistry, lockProvider);
    
        var bucketName = BucketName.From("test_bucket");
        await manager.EnsureBucketAsync(bucketName, StorageType.Memory);
    
        // Verify bucket exists by attempting to get it
        var store = await kv.GetStoreAsync("test_bucket");
        store.ShouldNotBeNull();
        store.Bucket.ShouldBe("test_bucket");
    }
    
    [Test]
    public async Task EnsureObjectStoreAsync_ShouldCreateObjectStore()
    {
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();
    
        var opts = new NatsOpts { Url = fixture.ConnectionString };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();
    
        var js = new NatsJSContext(conn);
        var kv = new NatsKVContext(js);
        var obj = new NatsObjContext(js);
    
        var lockProvider = new NatsDistributedLockProvider(kv, NullLogger<NatsDistributedLockProvider>.Instance, TimeSpan.FromSeconds(30));
        var dlqRegistry = new DlqPolicyRegistry(NullLogger<DlqPolicyRegistry>.Instance);
        var manager = new NatsTopologyManager(js, kv, obj, NullLogger<NatsTopologyManager>.Instance, dlqRegistry, lockProvider);
    
        var bucketName = BucketName.From("test_object_store");
        await manager.EnsureObjectStoreAsync(bucketName, StorageType.File);
    
        // Verify object store exists by attempting to get it
        var store = await obj.GetObjectStoreAsync("test_object_store");
        store.ShouldNotBeNull();
        store.Bucket.ShouldBe("test_object_store");
    }
    
    [Test]
    public async Task EnsureDlqInfrastructureAsync_ShouldSetupDlqStream()
    {
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();
    
        var opts = new NatsOpts { Url = fixture.ConnectionString };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();
    
        var js = new NatsJSContext(conn);
        var kv = new NatsKVContext(js);
        var obj = new NatsObjContext(js);
    
        var lockProvider = new NatsDistributedLockProvider(kv, NullLogger<NatsDistributedLockProvider>.Instance, TimeSpan.FromSeconds(30));
        var dlqRegistry = new DlqPolicyRegistry(NullLogger<DlqPolicyRegistry>.Instance);
        var manager = new NatsTopologyManager(js, kv, obj, NullLogger<NatsTopologyManager>.Instance, dlqRegistry, lockProvider);
    
        // Create source stream first
        var streamSpec = new StreamSpec
        {
            Name = StreamName.From("SOURCE_STREAM"),
            Subjects = new[] { "source.>" },
            StorageType = StorageType.Memory
        };
        await manager.EnsureStreamAsync(streamSpec);
    
        // Create consumer with DLQ policy
        var dlqPolicy = new DeadLetterPolicy
        {
            SourceStream = "SOURCE_STREAM",
            SourceConsumer = "source_consumer",
            TargetStream = StreamName.From("DLQ_STREAM"),
            TargetSubject = "dlq.failed"
        };
    
        var consumerSpec = new ConsumerSpec
        {
            StreamName = StreamName.From("SOURCE_STREAM"),
            DurableName = ConsumerName.From("source_consumer"),
            FilterSubject = "source.test",
            AckPolicy = AckPolicy.Explicit,
            DeliverPolicy = DeliverPolicy.All,
            DeadLetterPolicy = dlqPolicy
        };
    
        await manager.EnsureConsumerAsync(consumerSpec);
    
        // Verify DLQ stream was created
        var dlqStream = await js.GetStreamAsync("DLQ_STREAM");
        dlqStream.ShouldNotBeNull();
        dlqStream.Info.Config.Name.ShouldBe("DLQ_STREAM");
        dlqStream.Info.Config.Subjects.ShouldNotBeNull();
        dlqStream.Info.Config.Subjects.ShouldContain("dlq.failed");
    
        // Verify DLQ consumer was created
        var dlqConsumer = await js.GetConsumerAsync("DLQ_STREAM", "dlq_SOURCE_STREAM_source_consumer");
        dlqConsumer.ShouldNotBeNull();
    }
    
    [Test]
    public async Task Idempotency_MultipleCallsShouldNotFail()
    {
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();
    
        var opts = new NatsOpts { Url = fixture.ConnectionString };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();
    
        var js = new NatsJSContext(conn);
        var kv = new NatsKVContext(js);
        var obj = new NatsObjContext(js);
    
        var lockProvider = new NatsDistributedLockProvider(kv, NullLogger<NatsDistributedLockProvider>.Instance, TimeSpan.FromSeconds(30));
        var dlqRegistry = new DlqPolicyRegistry(NullLogger<DlqPolicyRegistry>.Instance);
        var manager = new NatsTopologyManager(js, kv, obj, NullLogger<NatsTopologyManager>.Instance, dlqRegistry, lockProvider);
    
        var streamSpec = new StreamSpec
        {
            Name = StreamName.From("IDEMPOTENT_STREAM"),
            Subjects = new[] { "idempotent.>" },
            StorageType = StorageType.Memory
        };
    
        // Call multiple times
        await manager.EnsureStreamAsync(streamSpec);
        await manager.EnsureStreamAsync(streamSpec);
        await manager.EnsureStreamAsync(streamSpec);
    
        // Verify stream exists (and was not created multiple times)
        var stream = await js.GetStreamAsync("IDEMPOTENT_STREAM");
        stream.ShouldNotBeNull();
    
        // Test consumer idempotency
        var consumerSpec = new ConsumerSpec
        {
            StreamName = StreamName.From("IDEMPOTENT_STREAM"),
            DurableName = ConsumerName.From("idempotent_consumer"),
            FilterSubject = "idempotent.test",
            AckPolicy = AckPolicy.Explicit
        };
    
        await manager.EnsureConsumerAsync(consumerSpec);
        await manager.EnsureConsumerAsync(consumerSpec);
        await manager.EnsureConsumerAsync(consumerSpec);
    
        var consumer = await js.GetConsumerAsync("IDEMPOTENT_STREAM", "idempotent_consumer");
        consumer.ShouldNotBeNull();
    
        // Test bucket idempotency
        var bucketName = BucketName.From("idempotent_bucket");
        await manager.EnsureBucketAsync(bucketName, StorageType.Memory);
        await manager.EnsureBucketAsync(bucketName, StorageType.Memory);
        await manager.EnsureBucketAsync(bucketName, StorageType.Memory);
    
        var bucket = await kv.GetStoreAsync("idempotent_bucket");
        bucket.ShouldNotBeNull();
    
        // Test object store idempotency
        var objBucketName = BucketName.From("idempotent_obj_store");
        await manager.EnsureObjectStoreAsync(objBucketName, StorageType.Memory);
        await manager.EnsureObjectStoreAsync(objBucketName, StorageType.Memory);
        await manager.EnsureObjectStoreAsync(objBucketName, StorageType.Memory);
    
        var objStore = await obj.GetObjectStoreAsync("idempotent_obj_store");
        objStore.ShouldNotBeNull();
    }
}

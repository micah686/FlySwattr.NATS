using FlySwattr.NATS.DistributedLock.Services;
using IntegrationTests.Infrastructure;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.KeyValueStore;
using Shouldly;
using TUnit.Core;

namespace IntegrationTests.DistributedLock;

[Property("nTag", "DistributedLock")]
public class NatsDistributedLockProviderTests
{
    [Test]
    public async Task AcquireAsync_WithTimeout_ShouldNotOverflow()
    {
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts { Url = fixture.ConnectionString };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();

        var js = new NatsJSContext(conn);
        var kv = new NatsKVContext(js);

        var lockProvider = new NatsDistributedLockProvider(kv, NullLogger<NatsDistributedLockProvider>.Instance, TimeSpan.FromSeconds(30));

        // This should not throw a TimeSpan overflow exception
        var lockHandle = await lockProvider.CreateLock("test_lock").AcquireAsync(TimeSpan.FromSeconds(30));

        lockHandle.ShouldNotBeNull();

        await lockHandle.DisposeAsync();
    }

    [Test]
    public async Task TryAcquireAsync_WithTimeout_ShouldNotOverflow()
    {
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts { Url = fixture.ConnectionString };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();

        var js = new NatsJSContext(conn);
        var kv = new NatsKVContext(js);

        var lockProvider = new NatsDistributedLockProvider(kv, NullLogger<NatsDistributedLockProvider>.Instance, TimeSpan.FromSeconds(30));

        // This should not throw a TimeSpan overflow exception
        var lockHandle = await lockProvider.CreateLock("test_lock_2").TryAcquireAsync(TimeSpan.FromSeconds(30));

        lockHandle.ShouldNotBeNull();

        await lockHandle!.DisposeAsync();
    }

    [Test]
    public async Task AcquireAsync_ShouldSucceed_WhenNoContention()
    {
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts { Url = fixture.ConnectionString };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();

        var js = new NatsJSContext(conn);
        var kv = new NatsKVContext(js);

        var lockProvider = new NatsDistributedLockProvider(kv, NullLogger<NatsDistributedLockProvider>.Instance, TimeSpan.FromSeconds(30));

        // Acquire lock first time should succeed quickly
        var lockHandle1 = await lockProvider.CreateLock("test_lock_contention").AcquireAsync(TimeSpan.FromSeconds(5));
        lockHandle1.ShouldNotBeNull();

        // Release the lock
        await lockHandle1.DisposeAsync();

        // Should be able to acquire it again
        var lockHandle2 = await lockProvider.CreateLock("test_lock_contention").AcquireAsync(TimeSpan.FromSeconds(5));
        lockHandle2.ShouldNotBeNull();

        await lockHandle2.DisposeAsync();
    }

    [Test]
    public async Task TryAcquireAsync_ShouldReturnNull_WhenTimeout()
    {
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts { Url = fixture.ConnectionString };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();

        var js = new NatsJSContext(conn);
        var kv = new NatsKVContext(js);

        var lockProvider = new NatsDistributedLockProvider(kv, NullLogger<NatsDistributedLockProvider>.Instance, TimeSpan.FromSeconds(30));

        // Acquire lock first
        var lockHandle1 = await lockProvider.CreateLock("test_timeout").AcquireAsync(TimeSpan.FromSeconds(5));
        lockHandle1.ShouldNotBeNull();

        // Try to acquire same lock with short timeout - should return null
        var lockHandle2 = await lockProvider.CreateLock("test_timeout").TryAcquireAsync(TimeSpan.FromMilliseconds(100));
        lockHandle2.ShouldBeNull();

        await lockHandle1.DisposeAsync();
    }

    [Test]
    public async Task CanCreateKVBucket_Directly()
    {
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts { Url = fixture.ConnectionString };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();

        var js = new NatsJSContext(conn);
        var kv = new NatsKVContext(js);

        // Try to create a bucket directly to verify KV is working
        var config = new NatsKVConfig("test_direct_bucket") { MaxAge = TimeSpan.FromMinutes(5) };
        var store = await kv.CreateStoreAsync(config);

        store.ShouldNotBeNull();
        store.Bucket.ShouldBe("test_direct_bucket");
    }

    [Test]
    public async Task LockProvider_ShouldAcquireOnFirstAttempt()
    {
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        var opts = new NatsOpts { Url = fixture.ConnectionString };
        await using var conn = new NatsConnection(opts);
        await conn.ConnectAsync();

        var js = new NatsJSContext(conn);
        var kv = new NatsKVContext(js);

        // Create lock provider with a short TTL
        var lockProvider = new NatsDistributedLockProvider(kv, NullLogger<NatsDistributedLockProvider>.Instance, TimeSpan.FromSeconds(5));

        // First acquisition should succeed immediately (no retries needed)
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var lockHandle = await lockProvider.CreateLock("topology_lock_stream_TEST_STREAM").AcquireAsync(TimeSpan.FromSeconds(10));
        sw.Stop();

        lockHandle.ShouldNotBeNull();
        sw.ElapsedMilliseconds.ShouldBeLessThan(2000); // Should be very quick (< 2 seconds)

        await lockHandle.DisposeAsync();
    }
}

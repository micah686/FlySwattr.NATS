using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Stores;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.ObjectStore;
using NATS.Client.ObjectStore.Models;
using NSubstitute;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Core.Stores;

[Property("nTag", "Core")]
public class NatsObjectStoreTests : IAsyncDisposable
{
    private readonly INatsObjContext _objContext;
    private readonly INatsObjStore _objStore;
    private readonly ILogger<NatsObjectStore> _logger;
    private readonly NatsObjectStore _sut;
    private const string BucketName = "test-bucket";

    public NatsObjectStoreTests()
    {
        _objContext = Substitute.For<INatsObjContext>();
        _objStore = Substitute.For<INatsObjStore>();
        _logger = Substitute.For<ILogger<NatsObjectStore>>();

        _objContext.GetObjectStoreAsync(BucketName, Arg.Any<CancellationToken>())
            .Returns(new ValueTask<INatsObjStore>(_objStore));

        _sut = new NatsObjectStore(_objContext, BucketName, _logger);
    }

    public async ValueTask DisposeAsync()
    {
        await _sut.DisposeAsync();
    }

    [Test]
    public async Task PutAsync_ShouldRetry_OnTransientFailure()
    {
        var key = "retry-key";
        using var stream = new MemoryStream(new byte[] { 1, 2, 3 });
        int callCount = 0;

        _objStore.PutAsync(key, stream, leaveOpen: true, cancellationToken: Arg.Any<CancellationToken>())
            .Returns(x =>
            {
                callCount++;
                if (callCount == 1) throw new TimeoutException("Simulated timeout");
                if (stream.Position != 0) throw new Exception("Stream position was not reset!");
                return new ValueTask<ObjectMetadata>(new ObjectMetadata { Bucket = BucketName, Name = key });
            });

        await _sut.PutAsync(key, stream);

        await _objStore.Received(2).PutAsync(key, stream, leaveOpen: true, cancellationToken: Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetAsync_ShouldRetry_OnTransientFailure()
    {
        var key = "retry-get-key";
        using var targetStream = new MemoryStream();
        int callCount = 0;

        _objStore.GetAsync(key, targetStream, cancellationToken: Arg.Any<CancellationToken>())
            .Returns(x =>
            {
                callCount++;
                if (callCount == 1) throw new IOException("Simulated IO error");
                return new ValueTask<ObjectMetadata>(new ObjectMetadata { Bucket = BucketName, Name = key });
            });

        await _sut.GetAsync(key, targetStream);

        await _objStore.Received(2).GetAsync(key, targetStream, cancellationToken: Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task DeleteAsync_ShouldSucceed_WhenKeyNotFound()
    {
        var key = "missing-key";
        var exception = new NatsObjNotFoundException("Not found");

        _objStore.DeleteAsync(key, cancellationToken: Arg.Any<CancellationToken>())
            .Returns(x => throw exception);

        await _sut.DeleteAsync(key);

        // NatsObjectStore retries on NatsException, and NatsObjNotFoundException inherits from it.
        // It retries 3 times + 1 initial call = 4 calls.
        await _objStore.Received(4).DeleteAsync(key, cancellationToken: Arg.Any<CancellationToken>());
    }
}

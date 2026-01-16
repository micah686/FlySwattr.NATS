using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core;
using FlySwattr.NATS.DistributedLock.Services;
using FlySwattr.NATS.Topology.Configuration;
using FlySwattr.NATS.Topology.Managers;
using FlySwattr.NATS.Topology.Services;
using IntegrationTests.Infrastructure;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.KeyValueStore;
using NATS.Client.ObjectStore;
using NSubstitute;
using Shouldly;
using TUnit.Core;

namespace IntegrationTests.DistributedLock;

/// <summary>
/// T03: Distributed Locking Concurrency Tests
/// Verifies that 5 concurrent instances of TopologyProvisioningService
/// can provision the same topology without crashes using distributed locking.
/// </summary>
public class DistributedLockConcurrencyTests
{
    private NatsContainerFixture? _fixture;
    private NatsConnection? _connection;

    [Before(Test)]
    public async Task Setup()
    {
        _fixture = new NatsContainerFixture();
        await _fixture.InitializeAsync();

        _connection = new NatsConnection(new NatsOpts
        {
            Url = _fixture.ConnectionString
        });
    }

    [After(Test)]
    public async Task Cleanup()
    {
        if (_connection is not null)
        {
            await _connection.DisposeAsync();
        }

        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    /// <summary>
    /// T03: Start 5 concurrent instances of TopologyProvisioningService against a real NATS container
    /// and assert zero startup crashes. The distributed lock should coordinate parallel provisioning attempts.
    /// </summary>
    [Test]
    public async Task ConcurrentProvisioning_ShouldSucceed_WithDistributedLocking()
    {
        // Arrange
        var jsContext = new NatsJSContext(_connection!);
        var kvContext = new NatsKVContext(jsContext);
        var objContext = new NatsObjContext(jsContext);

        // Create distributed lock provider
        var lockProvider = new NatsDistributedLockProvider(
            kvContext,
            NullLogger<NatsDistributedLockProvider>.Instance);

        // Create DLQ policy registry (mock it for tests)
        var dlqRegistry = Substitute.For<IDlqPolicyRegistry>();

        // Create topology manager with distributed locking
        var topologyManager = new NatsTopologyManager(
            jsContext,
            kvContext,
            objContext,
            NullLogger<NatsTopologyManager>.Instance,
            dlqRegistry,
            lockProvider);

        // Create a single topology source with test streams and consumers
        var topologySource = Substitute.For<ITopologySource>();
        topologySource.GetStreams().Returns(new[]
        {
            new StreamSpec { Name = StreamName.From("CONCURRENT_TEST_STREAM") }
        });
        topologySource.GetConsumers().Returns(new[]
        {
            new ConsumerSpec 
            { 
                StreamName = StreamName.From("CONCURRENT_TEST_STREAM"),
                DurableName = ConsumerName.From("concurrent_test_consumer")
            }
        });

        var topologyReadySignal = new TopologyReadySignal(NullLogger<TopologyReadySignal>.Instance);

        var startupOptions = Options.Create(new TopologyStartupOptions
        {
            MaxRetryAttempts = 3,
            InitialRetryDelay = TimeSpan.FromMilliseconds(100),
            MaxRetryDelay = TimeSpan.FromSeconds(1),
            TotalStartupTimeout = TimeSpan.FromSeconds(30),
            ConnectionTimeout = TimeSpan.FromSeconds(5)
        });

        // Act - Start 5 concurrent provisioning services
        var exceptions = new List<Exception>();
        var tasks = new Task[5];

        for (int i = 0; i < 5; i++)
        {
            // Each instance needs its own connection to properly simulate concurrent services
            var instanceConnection = new NatsConnection(new NatsOpts { Url = _fixture!.ConnectionString });
            var instanceJsContext = new NatsJSContext(instanceConnection);
            var instanceKvContext = new NatsKVContext(instanceJsContext);
            var instanceObjContext = new NatsObjContext(instanceJsContext);
            
            var instanceLockProvider = new NatsDistributedLockProvider(
                instanceKvContext,
                NullLogger<NatsDistributedLockProvider>.Instance);

            var instanceDlqRegistry = Substitute.For<IDlqPolicyRegistry>();

            var instanceTopologyManager = new NatsTopologyManager(
                instanceJsContext,
                instanceKvContext,
                instanceObjContext,
                NullLogger<NatsTopologyManager>.Instance,
                instanceDlqRegistry,
                instanceLockProvider);

            var service = new TopologyProvisioningService(
                new[] { topologySource },
                instanceTopologyManager,
                instanceConnection,
                NullLogger<TopologyProvisioningService>.Instance,
                startupOptions,
                topologyReadySignal);

            tasks[i] = Task.Run(async () =>
            {
                try
                {
                    await service.StartAsync(CancellationToken.None);
                }
                catch (Exception ex)
                {
                    lock (exceptions)
                    {
                        exceptions.Add(ex);
                    }
                }
                finally
                {
                    await instanceConnection.DisposeAsync();
                }
            });
        }

        // Wait for all tasks to complete
        await Task.WhenAll(tasks);

        // Assert - Zero crashes
        exceptions.ShouldBeEmpty($"Expected zero exceptions, but got: {string.Join(", ", exceptions.Select(e => e.Message))}");

        // Verify stream and consumer exist (provisioning succeeded)
        var stream = await jsContext.GetStreamAsync("CONCURRENT_TEST_STREAM");
        stream.ShouldNotBeNull();

        var consumer = await jsContext.GetConsumerAsync("CONCURRENT_TEST_STREAM", "concurrent_test_consumer");
        consumer.ShouldNotBeNull();
    }

    /// <summary>
    /// Verifies that concurrent provisioning attempts for different streams work independently.
    /// </summary>
    [Test]
    public async Task ConcurrentProvisioning_DifferentStreams_ShouldSucceedInParallel()
    {
        // Arrange
        var jsContext = new NatsJSContext(_connection!);
        var kvContext = new NatsKVContext(jsContext);

        var completionTimes = new Dictionary<string, TimeSpan>();
        var startTime = DateTime.UtcNow;

        var tasks = new List<Task>();

        for (int i = 0; i < 5; i++)
        {
            var streamName = $"PARALLEL_STREAM_{i}";
            var instanceConnection = new NatsConnection(new NatsOpts { Url = _fixture!.ConnectionString });
            var instanceJsContext = new NatsJSContext(instanceConnection);
            var instanceKvContext = new NatsKVContext(instanceJsContext);
            var instanceObjContext = new NatsObjContext(instanceJsContext);

            var instanceLockProvider = new NatsDistributedLockProvider(
                instanceKvContext,
                NullLogger<NatsDistributedLockProvider>.Instance);

            var instanceDlqRegistry = Substitute.For<IDlqPolicyRegistry>();

            var instanceTopologyManager = new NatsTopologyManager(
                instanceJsContext,
                instanceKvContext,
                instanceObjContext,
                NullLogger<NatsTopologyManager>.Instance,
                instanceDlqRegistry,
                instanceLockProvider);

            var topologySource = Substitute.For<ITopologySource>();
            topologySource.GetStreams().Returns(new[]
            {
                new StreamSpec { Name = StreamName.From(streamName) }
            });
            topologySource.GetConsumers().Returns(Array.Empty<ConsumerSpec>());

            var service = new TopologyProvisioningService(
                new[] { topologySource },
                instanceTopologyManager,
                instanceConnection,
                NullLogger<TopologyProvisioningService>.Instance,
                Options.Create(new TopologyStartupOptions
                {
                    MaxRetryAttempts = 3,
                    InitialRetryDelay = TimeSpan.FromMilliseconds(100),
                    MaxRetryDelay = TimeSpan.FromSeconds(1),
                    TotalStartupTimeout = TimeSpan.FromSeconds(30),
                    ConnectionTimeout = TimeSpan.FromSeconds(5)
                }),
                null);

            var localStreamName = streamName; // Capture for closure
            tasks.Add(Task.Run(async () =>
            {
                await service.StartAsync(CancellationToken.None);
                lock (completionTimes)
                {
                    completionTimes[localStreamName] = DateTime.UtcNow - startTime;
                }
                await instanceConnection.DisposeAsync();
            }));
        }

        // Act
        await Task.WhenAll(tasks);

        // Assert - All 5 streams should be created
        for (int i = 0; i < 5; i++)
        {
            var stream = await jsContext.GetStreamAsync($"PARALLEL_STREAM_{i}");
            stream.ShouldNotBeNull();
        }
    }
}

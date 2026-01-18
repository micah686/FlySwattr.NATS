using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Configuration;
using FlySwattr.NATS.Extensions;
using FlySwattr.NATS.Hosting.Health;
using Medallion.Threading;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using TUnit.Core;

namespace UnitTests.DI;

public class ServiceCollectionExtensionsTests
{
    [Test]
    public async Task AddEnterpriseNATSMessaging_GoldenPath_ShouldRegisterAllServices()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        
        // Act
        services.AddEnterpriseNATSMessaging(opts =>
        {
            opts.Core.Url = "nats://localhost:4222";
        });
        
        var sp = services.BuildServiceProvider();

        // Assert - Core
        // Note: IMessageBus is the high-level interface, but distinct interfaces are also registered
        await Assert.That(sp.GetService<IMessageBus>()).IsNotNull();
        await Assert.That(sp.GetService<IJetStreamPublisher>()).IsNotNull();
        await Assert.That(sp.GetService<IJetStreamConsumer>()).IsNotNull();
        await Assert.That(sp.GetService<Func<string, IKeyValueStore>>()).IsNotNull(); // KV store factory
        await Assert.That(sp.GetService<Func<string, IObjectStore>>()).IsNotNull(); // Obj store factory
        
        // Assert - Hosting
        // Hosted services are registered as IHostedService
        var hostedServices = sp.GetServices<IHostedService>();
        await Assert.That(hostedServices).IsNotNull();
        // NatsConsumerBackgroundService is only registered when users add specific consumers
        // DlqAdvisoryListenerService must be added explicitly or we should check if Enterprise adds it (it doesn't appear to)
        
        await Assert.That(hostedServices.Any(s => s.GetType().Name == "NatsStartupCheck")).IsTrue();
        
        await Assert.That(sp.GetService<IConsumerHealthMetrics>()).IsNotNull();
        
        // Assert - Topology
        await Assert.That(sp.GetService<ITopologyManager>()).IsNotNull();
        
        // Assert - Distributed Lock
        await Assert.That(sp.GetService<IDistributedLockProvider>()).IsNotNull();
    }

    [Test]
    public async Task AddEnterpriseNATSMessaging_WithResilienceDisabled_ShouldNotRegisterResilienceDecorators()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        
        // Act
        services.AddEnterpriseNATSMessaging(opts =>
        {
            opts.Core.Url = "nats://localhost:4222";
            opts.EnableResilience = false;
        });
        
        var sp = services.BuildServiceProvider();

        // Assert
        var publisher = sp.GetService<IJetStreamPublisher>();
        await Assert.That(publisher).IsNotNull();
        
        // Should NOT be the Resilient wrapper
        await Assert.That(publisher!.GetType().Name).DoesNotContain("Resilient");
    }

    [Test]
    public async Task AddEnterpriseNATSMessaging_WithOffloadingDisabled_ShouldNotRegisterOffloadingDecorators()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        
        // Act
        services.AddEnterpriseNATSMessaging(opts =>
        {
            opts.Core.Url = "nats://localhost:4222";
            opts.EnablePayloadOffloading = false;
            opts.EnableResilience = false; // Disable resilience too to see the inner type clearly
        });
        
        var sp = services.BuildServiceProvider();

        // Assert
        var publisher = sp.GetService<IJetStreamPublisher>();
        await Assert.That(publisher).IsNotNull();
        
        // Should NOT be the Offloading wrapper
        await Assert.That(publisher!.GetType().Name).DoesNotContain("Offloading");
    }

    [Test]
    public async Task AddEnterpriseNATSMessaging_VerifyDecoratorChain_ResilienceWrapsOffloadingWrapsCore()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        
        // Act
        services.AddEnterpriseNATSMessaging(opts =>
        {
            opts.Core.Url = "nats://localhost:4222";
            opts.EnableResilience = true;
            opts.EnablePayloadOffloading = true;
        });
        
        var sp = services.BuildServiceProvider();

        // Assert
        var publisher = sp.GetService<IJetStreamPublisher>();
        await Assert.That(publisher).IsNotNull();
        
        // Top level should be Resilient
        await Assert.That(publisher!.GetType().Name).Contains("Resilient");
        
        // Use reflection to inspect the 'inner' or '_inner' field to verify the chain
        // Resilient -> Offloading -> Core (NatsJetStreamBus)
        
        var resilientInner = GetInnerService(publisher);
        await Assert.That(resilientInner).IsNotNull();
        await Assert.That(resilientInner!.GetType().Name).Contains("Offloading");
        
        var offloadingInner = GetInnerService(resilientInner!);
        await Assert.That(offloadingInner).IsNotNull();
        await Assert.That(offloadingInner!.GetType().Name).Contains("NatsJetStreamBus");
    }
    
    private object? GetInnerService(object service)
    {
        var type = service.GetType();
        // Try common field names for decorators
        var field = type.GetField("_inner", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
                 ?? type.GetField("Inner", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
                 
        return field?.GetValue(service);
    }
}

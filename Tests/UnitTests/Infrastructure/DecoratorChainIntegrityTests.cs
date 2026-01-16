using System.Reflection;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core;
using FlySwattr.NATS.Core.Decorators;
using FlySwattr.NATS.Core.Extensions;
using FlySwattr.NATS.Resilience.Decorators;
using FlySwattr.NATS.Resilience.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Infrastructure;

/// <summary>
/// T01: Decorator Chain Integrity Tests
/// Verifies that IJetStreamConsumer resolves through the correct decorator chain:
/// ResilientJetStreamConsumer -> OffloadingJetStreamConsumer -> NatsJetStreamBus
/// </summary>
public class DecoratorChainIntegrityTests
{
    /// <summary>
    /// Verifies the decorator chain for IJetStreamConsumer is:
    /// Resilient -> Offloading -> Core (NatsJetStreamBus)
    /// </summary>
    [Test]
    public void IJetStreamConsumer_ShouldResolve_ToCorrectDecoratorChain()
    {
        // Arrange
        var services = new ServiceCollection();
        
        // Add required logging
        services.AddLogging(builder => builder.AddConsole());
        
        // Add Core (registers NatsJetStreamBus as IJetStreamConsumer)
        services.AddFlySwattrNatsCore(config =>
        {
            config.Url = "nats://localhost:4222";
        });
        
        // Add Offloading (decorates with OffloadingJetStreamConsumer)
        services.AddPayloadOffloading();
        
        // Add Resilience (decorates with ResilientJetStreamConsumer)
        services.AddFlySwattrNatsResilience();

        var provider = services.BuildServiceProvider();

        // Act
        var consumer = provider.GetRequiredService<IJetStreamConsumer>();

        // Assert
        // Outer layer should be ResilientJetStreamConsumer
        consumer.ShouldBeOfType<ResilientJetStreamConsumer>();

        // Get the inner consumer from ResilientJetStreamConsumer
        var resilientInner = GetInnerField<IJetStreamConsumer>(consumer, "_inner");
        resilientInner.ShouldNotBeNull();
        resilientInner.ShouldBeOfType<OffloadingJetStreamConsumer>();

        // Get the inner consumer from OffloadingJetStreamConsumer
        var offloadingInner = GetInnerField<IJetStreamConsumer>(resilientInner, "_inner");
        offloadingInner.ShouldNotBeNull();
        offloadingInner.ShouldBeOfType<NatsJetStreamBus>();
    }

    /// <summary>
    /// Verifies the decorator chain for IJetStreamPublisher is:
    /// Resilient -> Offloading -> Core (NatsJetStreamBus)
    /// </summary>
    [Test]
    public void IJetStreamPublisher_ShouldResolve_ToCorrectDecoratorChain()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole());
        
        services.AddFlySwattrNatsCore(config =>
        {
            config.Url = "nats://localhost:4222";
        });
        services.AddPayloadOffloading();
        services.AddFlySwattrNatsResilience();

        var provider = services.BuildServiceProvider();

        // Act
        var publisher = provider.GetRequiredService<IJetStreamPublisher>();

        // Assert
        publisher.ShouldBeOfType<ResilientJetStreamPublisher>();

        var resilientInner = GetInnerField<IJetStreamPublisher>(publisher, "_inner");
        resilientInner.ShouldNotBeNull();
        resilientInner.ShouldBeOfType<OffloadingJetStreamPublisher>();

        var offloadingInner = GetInnerField<IJetStreamPublisher>(resilientInner, "_inner");
        offloadingInner.ShouldNotBeNull();
        offloadingInner.ShouldBeOfType<NatsJetStreamBus>();
    }

    /// <summary>
    /// Verifies that without Resilience, the chain is:
    /// Offloading -> Core
    /// </summary>
    [Test]
    public void IJetStreamConsumer_WithoutResilience_ShouldResolve_ToOffloadingThenCore()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole());
        
        services.AddFlySwattrNatsCore(config =>
        {
            config.Url = "nats://localhost:4222";
        });
        services.AddPayloadOffloading();
        // NOTE: No Resilience added

        var provider = services.BuildServiceProvider();

        // Act
        var consumer = provider.GetRequiredService<IJetStreamConsumer>();

        // Assert
        consumer.ShouldBeOfType<OffloadingJetStreamConsumer>();

        var offloadingInner = GetInnerField<IJetStreamConsumer>(consumer, "_inner");
        offloadingInner.ShouldNotBeNull();
        offloadingInner.ShouldBeOfType<NatsJetStreamBus>();
    }

    /// <summary>
    /// Verifies that Core-only registration returns NatsJetStreamBus directly.
    /// </summary>
    [Test]
    public void IJetStreamConsumer_CoreOnly_ShouldResolve_ToNatsJetStreamBus()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole());
        
        services.AddFlySwattrNatsCore(config =>
        {
            config.Url = "nats://localhost:4222";
        });
        // NOTE: No Offloading or Resilience

        var provider = services.BuildServiceProvider();

        // Act
        var consumer = provider.GetRequiredService<IJetStreamConsumer>();

        // Assert
        consumer.ShouldBeOfType<NatsJetStreamBus>();
    }

    private static T? GetInnerField<T>(object obj, string fieldName) where T : class
    {
        var field = obj.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
        return field?.GetValue(obj) as T;
    }
}

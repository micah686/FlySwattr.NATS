using FlySwattr.NATS.Core.Configuration;
using FlySwattr.NATS.Core.Services;
using Microsoft.Extensions.Options;
using TUnit.Core;

namespace UnitTests.Core.Services;

[Property("nTag", "Core")]
public class MessageTypeAliasRegistryTests
{
    [Test]
    public async Task Resolve_ShouldReturnNull_WhenAliasNotRegistered()
    {
        // The Type.GetType reflection fallback has been removed to prevent arbitrary
        // type resolution (security risk in DLQ replay paths). Only explicitly
        // registered aliases resolve successfully.
        var registry = new MessageTypeAliasRegistry(Options.Create(new MessageTypeAliasOptions()));
        var unregisteredAlias = typeof(TestMessage).AssemblyQualifiedName!;

        var resolved = registry.Resolve(unregisteredAlias);

        await Assert.That(resolved).IsNull();
    }

    [Test]
    public async Task Resolve_ShouldReturnType_WhenExplicitlyRegistered()
    {
        var registry = new MessageTypeAliasRegistry(Options.Create(new MessageTypeAliasOptions()));
        registry.Register<TestMessage>("TestMessage");

        var resolved = registry.Resolve("TestMessage");

        await Assert.That(resolved).IsEqualTo(typeof(TestMessage));
    }

    [Test]
    public async Task GetAlias_ShouldThrow_WhenExplicitAliasesAreRequired()
    {
        var registry = new MessageTypeAliasRegistry(Options.Create(new MessageTypeAliasOptions
        {
            RequireExplicitAliases = true
        }));

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            Task.FromResult(registry.GetAlias(typeof(TestMessage))));
    }

    private sealed class TestMessage;
}

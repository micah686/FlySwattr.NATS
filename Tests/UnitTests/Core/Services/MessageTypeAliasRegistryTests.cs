using FlySwattr.NATS.Core.Configuration;
using FlySwattr.NATS.Core.Services;
using Microsoft.Extensions.Options;
using TUnit.Core;

namespace UnitTests.Core.Services;

[Property("nTag", "Core")]
public class MessageTypeAliasRegistryTests
{
    [Test]
    public async Task Resolve_ShouldReturnAssemblyQualifiedType_WhenLegacyWireValueIsUsed()
    {
        var registry = new MessageTypeAliasRegistry(Options.Create(new MessageTypeAliasOptions()));
        var legacyWireValue = typeof(TestMessage).AssemblyQualifiedName!;

        var resolved = registry.Resolve(legacyWireValue);

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

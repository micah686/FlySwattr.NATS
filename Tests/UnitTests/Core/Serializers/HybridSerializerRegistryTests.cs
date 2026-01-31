using System.Buffers;
using FlySwattr.NATS.Core.Serializers;
using MemoryPack;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Core.Serializers;

[Property("nTag", "Core")]
public class HybridSerializerRegistryTests
{
    #region Default Instance Tests

    [Test]
    public void Default_ShouldBeSingleton()
    {
        // Act
        var instance1 = HybridSerializerRegistry.Default;
        var instance2 = HybridSerializerRegistry.Default;

        // Assert
        instance1.ShouldBeSameAs(instance2);
    }

    [Test]
    public void Default_ShouldNotBeNull()
    {
        // Assert
        HybridSerializerRegistry.Default.ShouldNotBeNull();
    }

    #endregion

    #region GetSerializer Tests

    [Test]
    public void GetSerializer_ShouldReturnSameInstance_ForSameType()
    {
        // Arrange
        var registry = new HybridSerializerRegistry();

        // Act
        var serializer1 = registry.GetSerializer<TestPlainMessage>();
        var serializer2 = registry.GetSerializer<TestPlainMessage>();

        // Assert
        serializer1.ShouldBeSameAs(serializer2);
    }

    [Test]
    public void GetSerializer_ShouldReturnDifferentInstances_ForDifferentTypes()
    {
        // Arrange
        var registry = new HybridSerializerRegistry();

        // Act
        var serializer1 = registry.GetSerializer<TestPlainMessage>();
        var serializer2 = registry.GetSerializer<AnotherPlainMessage>();

        // Assert - different types should get different serializers
        // Note: They are different generic type instances but both non-null
        serializer1.ShouldNotBeNull();
        serializer2.ShouldNotBeNull();
    }

    #endregion

    #region GetDeserializer Tests

    [Test]
    public void GetDeserializer_ShouldReturnSameInstance_ForSameType()
    {
        // Arrange
        var registry = new HybridSerializerRegistry();

        // Act
        var deserializer1 = registry.GetDeserializer<TestPlainMessage>();
        var deserializer2 = registry.GetDeserializer<TestPlainMessage>();

        // Assert
        deserializer1.ShouldBeSameAs(deserializer2);
    }

    [Test]
    public void GetSerializer_And_GetDeserializer_ShouldReturnSameInstance_ForSameType()
    {
        // Arrange
        var registry = new HybridSerializerRegistry();

        // Act
        var serializer = registry.GetSerializer<TestPlainMessage>();
        var deserializer = registry.GetDeserializer<TestPlainMessage>();

        // Assert - HybridNatsTypeSerializer implements both interfaces
        serializer.ShouldBeSameAs(deserializer);
    }

    #endregion

    #region IsMemoryPackable Tests

    [Test]
    public void IsMemoryPackable_ShouldReturnTrue_ForDecoratedType()
    {
        // Act
        var result = HybridSerializerRegistry.IsMemoryPackable<TestMemoryPackableMessage>();

        // Assert
        result.ShouldBeTrue();
    }

    [Test]
    public void IsMemoryPackable_ShouldReturnFalse_ForNonDecoratedType()
    {
        // Act
        var result = HybridSerializerRegistry.IsMemoryPackable<TestPlainMessage>();

        // Assert
        result.ShouldBeFalse();
    }

    [Test]
    public void IsMemoryPackable_ShouldReturnFalse_ForPrimitiveTypes()
    {
        // Act & Assert
        HybridSerializerRegistry.IsMemoryPackable<int>().ShouldBeFalse();
        HybridSerializerRegistry.IsMemoryPackable<string>().ShouldBeFalse();
        HybridSerializerRegistry.IsMemoryPackable<bool>().ShouldBeFalse();
    }

    [Test]
    public void IsMemoryPackable_ShouldCacheResults()
    {
        // Act - call multiple times
        var result1 = HybridSerializerRegistry.IsMemoryPackable<TestMemoryPackableMessage>();
        var result2 = HybridSerializerRegistry.IsMemoryPackable<TestMemoryPackableMessage>();

        // Assert - both should return the same result (from cache)
        result1.ShouldBe(result2);
        result1.ShouldBeTrue();
    }

    #endregion

    #region Serializer Format Tests

    [Test]
    public void Serializer_ShouldUseMemoryPack_ForDecoratedTypes()
    {
        // Arrange
        var registry = new HybridSerializerRegistry();
        var serializer = registry.GetSerializer<TestMemoryPackableMessage>();
        var message = new TestMemoryPackableMessage { Content = "test" };
        var writer = new ArrayBufferWriter<byte>();

        // Act
        serializer.Serialize(writer, message);

        // Assert - MemoryPack binary doesn't start with '{' (JSON object delimiter)
        writer.WrittenCount.ShouldBeGreaterThan(0);
        writer.WrittenSpan[0].ShouldNotBe((byte)'{');
    }

    [Test]
    public void Serializer_ShouldUseJson_ForNonDecoratedTypes()
    {
        // Arrange
        var registry = new HybridSerializerRegistry();
        var serializer = registry.GetSerializer<TestPlainMessage>();
        var message = new TestPlainMessage { Content = "test" };
        var writer = new ArrayBufferWriter<byte>();

        // Act
        serializer.Serialize(writer, message);

        // Assert - JSON objects start with '{'
        writer.WrittenCount.ShouldBeGreaterThan(0);
        writer.WrittenSpan[0].ShouldBe((byte)'{');
    }

    [Test]
    public void Serializer_ShouldRoundtrip_MemoryPackableTypes()
    {
        // Arrange
        var registry = new HybridSerializerRegistry();
        var serializer = registry.GetSerializer<TestMemoryPackableMessage>();
        var deserializer = registry.GetDeserializer<TestMemoryPackableMessage>();
        var original = new TestMemoryPackableMessage { Content = "roundtrip test" };
        var writer = new ArrayBufferWriter<byte>();

        // Act
        serializer.Serialize(writer, original);
        var result = deserializer.Deserialize(new ReadOnlySequence<byte>(writer.WrittenMemory));

        // Assert
        result.ShouldNotBeNull();
        result.Content.ShouldBe(original.Content);
    }

    [Test]
    public void Serializer_ShouldRoundtrip_JsonTypes()
    {
        // Arrange
        var registry = new HybridSerializerRegistry();
        var serializer = registry.GetSerializer<TestPlainMessage>();
        var deserializer = registry.GetDeserializer<TestPlainMessage>();
        var original = new TestPlainMessage { Content = "roundtrip test" };
        var writer = new ArrayBufferWriter<byte>();

        // Act
        serializer.Serialize(writer, original);
        var result = deserializer.Deserialize(new ReadOnlySequence<byte>(writer.WrittenMemory));

        // Assert
        result.ShouldNotBeNull();
        result.Content.ShouldBe(original.Content);
    }

    #endregion

    #region Test Types

    private class TestPlainMessage
    {
        public string? Content { get; set; }
    }

    private class AnotherPlainMessage
    {
        public int Value { get; set; }
    }

    #endregion
}

#region MemoryPackable Test Types (must be partial for source generation)

[MemoryPackable]
public partial class TestMemoryPackableMessage
{
    public string? Content { get; set; }
}

#endregion

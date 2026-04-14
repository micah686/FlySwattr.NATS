using System.Buffers;
using FlySwattr.NATS.Abstractions.Attributes;
using FlySwattr.NATS.Core.Serializers;
using MemoryPack;
using Shouldly;
using TUnit.Core;

namespace IntegrationTests.Chaos;

/// <summary>
/// Tests for schema evolution behavior: version-aware deserialization,
/// backward/forward compatibility, and configurable fingerprint enforcement.
/// </summary>
[Property("nTag", "Integration")]
[Property("nTag", "SchemaEvolution")]
public partial class SchemaEvolutionTests
{
    [Test]
    public void Deserialize_ShouldSucceed_WhenSameVersionAndFingerprint()
    {
        // Arrange
        var writer = new ArrayBufferWriter<byte>();
        MemoryPackSchemaEnvelopeSerializer.Serialize(writer, new EvolvableMessage { Name = "test", Value = 42 });
        var data = writer.WrittenMemory;

        // Act
        var result = MemoryPackSchemaEnvelopeSerializer.Default.Deserialize<EvolvableMessage>(data.Span);

        // Assert
        result.ShouldNotBeNull();
        result.Name.ShouldBe("test");
        result.Value.ShouldBe(42);
    }

    [Test]
    public void Deserialize_ShouldThrow_WhenEnvelopeMissing()
    {
        // Arrange: raw MemoryPack bytes without envelope
        var writer = new ArrayBufferWriter<byte>();
        MemoryPackSerializer.Serialize(writer, new EvolvableMessage { Name = "test" });
        var data = writer.WrittenMemory;

        // Act & Assert
        var ex = Should.Throw<MemoryPackSerializationException>(
            () => MemoryPackSchemaEnvelopeSerializer.Default.Deserialize<EvolvableMessage>(data.Span));
        ex.Message.ShouldContain("envelope missing");
    }

    [Test]
    public void Deserialize_ShouldThrow_WhenSchemaIdMismatch()
    {
        // Arrange: Serialize one type, deserialize as a completely different type
        var writer = new ArrayBufferWriter<byte>();
        MemoryPackSchemaEnvelopeSerializer.Serialize(writer, new EvolvableMessage { Name = "test" });
        var data = writer.WrittenMemory;

        // Act & Assert: Different type has different SchemaId (full type name)
        var ex = Should.Throw<MemoryPackSerializationException>(
            () => MemoryPackSchemaEnvelopeSerializer.Default.Deserialize<DifferentSchemaMessage>(data.Span));
        ex.Message.ShouldContain("Schema mismatch");
    }

    [Test]
    public void MinSupportedVersion_ShouldDefaultToOne()
    {
        var descriptor = MemoryPackSchemaMetadata.GetDescriptor<EvolvableMessage>();
        descriptor.MinSupportedVersion.ShouldBe(1);
    }

    [Test]
    public void MinSupportedVersion_ShouldReflectAttribute()
    {
        var descriptor = MemoryPackSchemaMetadata.GetDescriptor<MessageWithMinVersion>();
        descriptor.MinSupportedVersion.ShouldBe(2);
        descriptor.SchemaVersion.ShouldBe(3);
    }

    [Test]
    public void ContentType_ShouldIncludeVersionParameter()
    {
        var serializer = new HybridNatsSerializer();
        var contentType = serializer.GetContentType<EvolvableMessage>();
        contentType.ShouldBe("application/x-memorypack; v=1");
    }

    [Test]
    public void ContentType_ShouldBeJson_ForNonMemoryPackTypes()
    {
        var serializer = new HybridNatsSerializer();
        var contentType = serializer.GetContentType<PlainJsonMessage>();
        contentType.ShouldBe("application/json");
    }

    [Test]
    public void TryParseContentTypeVersion_ShouldExtractVersion()
    {
        HybridNatsSerializer.TryParseContentTypeVersion("application/x-memorypack; v=1", out var version).ShouldBeTrue();
        version.ShouldBe(1);
    }

    [Test]
    public void TryParseContentTypeVersion_ShouldReturnFalse_ForJsonContentType()
    {
        HybridNatsSerializer.TryParseContentTypeVersion("application/json", out _).ShouldBeFalse();
    }

    [Test]
    public void TryParseContentTypeVersion_ShouldReturnFalse_ForNullInput()
    {
        HybridNatsSerializer.TryParseContentTypeVersion(null, out _).ShouldBeFalse();
    }

    [Test]
    public void TryParseContentTypeVersion_ShouldReturnFalse_ForEmptyInput()
    {
        HybridNatsSerializer.TryParseContentTypeVersion("", out _).ShouldBeFalse();
    }

    [Test]
    public void TryParseContentTypeVersion_ShouldHandleVersionWithTrailingSemicolon()
    {
        HybridNatsSerializer.TryParseContentTypeVersion("application/x-memorypack; v=2; charset=utf-8", out var version).ShouldBeTrue();
        version.ShouldBe(2);
    }

    [Test]
    public void EnforceSchemaFingerprint_ShouldBeConfigurablePerInstance()
    {
        // Enforcement is now per-instance, not global static state.
        // Each HybridNatsSerializer holds its own MemoryPackSchemaEnvelopeSerializer instance.
        var strictSerializer = new HybridNatsSerializer(enforceSchemaFingerprint: true);
        var lenientSerializer = new HybridNatsSerializer(enforceSchemaFingerprint: false);

        // Serialize with strict default
        var writer = new ArrayBufferWriter<byte>();
        MemoryPackSchemaEnvelopeSerializer.Serialize(writer, new EvolvableMessage { Name = "test", Value = 1 });
        var data = writer.WrittenMemory;

        // Both instances can deserialize the same payload (same fingerprint here)
        var r1 = strictSerializer.Deserialize<EvolvableMessage>(data);
        var r2 = lenientSerializer.Deserialize<EvolvableMessage>(data);
        r1.ShouldNotBeNull();
        r2.ShouldNotBeNull();
    }

    [Test]
    public void Descriptor_ShouldUseSchemaVersionFromAttribute()
    {
        var descriptor = MemoryPackSchemaMetadata.GetDescriptor<EvolvableMessage>();
        descriptor.SchemaVersion.ShouldBe(1);
    }

    [Test]
    public void Descriptor_ShouldComputeStableFingerprint()
    {
        var d1 = MemoryPackSchemaMetadata.GetDescriptor<EvolvableMessage>();
        var d2 = MemoryPackSchemaMetadata.GetDescriptor<EvolvableMessage>();
        d1.SchemaFingerprint.ShouldBe(d2.SchemaFingerprint);
        d1.SchemaFingerprint.ShouldNotBeNullOrEmpty();
    }

    [Test]
    public void Descriptor_ShouldProduceDifferentFingerprints_ForDifferentLayouts()
    {
        var d1 = MemoryPackSchemaMetadata.GetDescriptor<EvolvableMessage>();
        var d2 = MemoryPackSchemaMetadata.GetDescriptor<DifferentSchemaMessage>();
        d1.SchemaFingerprint.ShouldNotBe(d2.SchemaFingerprint);
    }

    // Test message types

    [MemoryPackable]
    [MessageSchema(1)]
    internal partial class EvolvableMessage
    {
        public string Name { get; set; } = "";
        public int Value { get; set; }
    }

    [MemoryPackable]
    [MessageSchema(1)]
    internal partial class DifferentSchemaMessage
    {
        public string DifferentField { get; set; } = "";
    }

    [MemoryPackable]
    [MessageSchema(3, MinSupportedVersion = 2)]
    internal partial class MessageWithMinVersion
    {
        public string Name { get; set; } = "";
        public int Value { get; set; }
        public string? NewField { get; set; }
    }

    internal class PlainJsonMessage
    {
        public string Name { get; set; } = "";
    }
}

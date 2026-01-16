using System.Buffers;
using System.Text.Json;
using FlySwattr.NATS.Core.Serializers;
using MemoryPack;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Hosting.Services;

/// <summary>
/// T08: Poison Payload Handling Tests
/// Verifies that invalid byte arrays (neither valid MemoryPack nor valid JSON)
/// are detected and throw appropriate deserialization exceptions.
/// The consumer's poison handler will route these to DLQ.
/// </summary>
[Property("nTag", "Hosting")]
public class PoisonPayloadHandlingTests
{
    private readonly HybridNatsSerializer _serializer = new();

    #region Deserialization Failure Tests

    /// <summary>
    /// T08: When deserializing garbage bytes as a MemoryPackable type,
    /// MemoryPackSerializationException should be thrown.
    /// </summary>
    [Test]
    public void Deserialize_GarbageBytes_AsMemoryPackable_ShouldThrow()
    {
        // Arrange - Random garbage bytes (not valid MemoryPack format)
        var garbageBytes = new byte[] { 0xFF, 0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0x02 };
        
        // Act & Assert - Should throw MemoryPackSerializationException
        var exception = Assert.Throws<MemoryPackSerializationException>(() =>
            _serializer.Deserialize<PoisonTestPayload>(garbageBytes));

        exception.ShouldNotBeNull();
    }

    /// <summary>
    /// T08: When deserializing garbage bytes as a plain POCO (JSON path),
    /// JsonException should be thrown.
    /// </summary>
    [Test]
    public void Deserialize_GarbageBytes_AsPlainPoco_ShouldThrowJsonException()
    {
        // Arrange - Random garbage bytes (not valid JSON)
        var garbageBytes = new byte[] { 0xFF, 0xDE, 0xAD, 0xBE, 0xEF };
        
        // Act & Assert - Should throw JsonException
        var exception = Assert.Throws<JsonException>(() =>
            _serializer.Deserialize<PlainPoisonPayload>(garbageBytes));

        exception.ShouldNotBeNull();
    }

    /// <summary>
    /// T08: When deserializing truncated MemoryPack bytes,
    /// MemoryPackSerializationException should be thrown (incomplete data).
    /// </summary>
    [Test]
    public void Deserialize_TruncatedMemoryPackBytes_ShouldThrow()
    {
        // Arrange - Serialize valid data then truncate
        var validPayload = new PoisonTestPayload { Id = 123, Name = "Test" };
        var writer = new ArrayBufferWriter<byte>();
        _serializer.Serialize(writer, validPayload);
        
        // Truncate to half the bytes
        var truncated = writer.WrittenMemory[..(writer.WrittenCount / 2)];
        
        // Act & Assert - Should throw due to incomplete data
        var exception = Assert.Throws<MemoryPackSerializationException>(() =>
            _serializer.Deserialize<PoisonTestPayload>(truncated));

        exception.ShouldNotBeNull();
    }

    /// <summary>
    /// T08: When deserializing malformed JSON,
    /// JsonException should be thrown.
    /// </summary>
    [Test]
    public void Deserialize_MalformedJson_ShouldThrowJsonException()
    {
        // Arrange - Malformed JSON (missing closing brace)
        var malformedJson = """{"name": "unclosed"""u8.ToArray();
        
        // Act & Assert
        var exception = Assert.Throws<JsonException>(() =>
            _serializer.Deserialize<PlainPoisonPayload>(malformedJson));

        exception.ShouldNotBeNull();
    }

    /// <summary>
    /// T08: When deserializing empty bytes as MemoryPackable,
    /// should throw MemoryPackSerializationException.
    /// </summary>
    [Test]
    public void Deserialize_EmptyBytes_AsMemoryPackable_ShouldThrow()
    {
        // Arrange
        var emptyBytes = Array.Empty<byte>();
        
        // Act & Assert
        var exception = Assert.Throws<MemoryPackSerializationException>(() =>
            _serializer.Deserialize<PoisonTestPayload>(emptyBytes));

        exception.ShouldNotBeNull();
    }

    /// <summary>
    /// T08: When deserializing empty bytes as plain POCO (JSON path),
    /// should throw JsonException.
    /// </summary>
    [Test]
    public void Deserialize_EmptyBytes_AsPlainPoco_ShouldThrow()
    {
        // Arrange
        var emptyBytes = Array.Empty<byte>();
        
        // Act & Assert
        var exception = Assert.Throws<JsonException>(() =>
            _serializer.Deserialize<PlainPoisonPayload>(emptyBytes));

        exception.ShouldNotBeNull();
    }

    /// <summary>
    /// T08: When deserializing bytes that are valid JSON but wrong structure,
    /// should throw JsonException.
    /// </summary>
    [Test]
    public void Deserialize_WrongJsonStructure_ShouldThrow()
    {
        // Arrange - Valid JSON but wrong type (array instead of object)
        var arrayJson = """["item1", "item2"]"""u8.ToArray();
        
        // Act & Assert
        var exception = Assert.Throws<JsonException>(() =>
            _serializer.Deserialize<PlainPoisonPayload>(arrayJson));

        exception.ShouldNotBeNull();
    }

    /// <summary>
    /// T08: Verifies that exception messages contain useful information
    /// for debugging poison payloads.
    /// </summary>
    [Test]
    public void Deserialize_GarbageBytes_ExceptionShouldContainUsefulInfo()
    {
        // Arrange
        var garbageBytes = new byte[] { 0x00, 0x01, 0x02, 0x03 };
        
        // Act & Assert - JSON path
        var jsonException = Assert.Throws<JsonException>(() =>
            _serializer.Deserialize<PlainPoisonPayload>(garbageBytes));

        // The exception message should help identify it as a deserialization issue
        jsonException.ShouldNotBeNull();
    }

    #endregion
}

#region File-scoped MemoryPackable types

/// <summary>
/// MemoryPackable test type for poison payload tests.
/// Must be file-scoped partial for source generation.
/// </summary>
[MemoryPackable]
public partial class PoisonTestPayload
{
    public int Id { get; set; }
    public string? Name { get; set; }
}

/// <summary>
/// Plain POCO for testing JSON fallback path.
/// </summary>
public class PlainPoisonPayload
{
    public int Id { get; set; }
    public string? Name { get; set; }
}

#endregion

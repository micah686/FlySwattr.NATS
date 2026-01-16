using System.Buffers;
using System.Text.Json;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Serializers;
using MemoryPack;
using NATS.Client.Core;
using NSubstitute;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Core.Serializers;

[Property("nTag", "Core")]
public class HybridNatsSerializerTests
{
    private readonly HybridNatsSerializer _serializer;

    public HybridNatsSerializerTests()
    {
        _serializer = new HybridNatsSerializer();
    }

    #region Basic Serialization Tests

    [Test]
    public void Serialize_ShouldSerializeToBytes()
    {
        // Arrange
        var message = new TestMessage("Data");
        var writer = new ArrayBufferWriter<byte>();

        // Act
        _serializer.Serialize(writer, message);

        // Assert
        writer.WrittenCount.ShouldBeGreaterThan(0);
    }

    [Test]
    public void Deserialize_ShouldDeserializeFromBytes()
    {
        // Arrange
        var message = new TestMessage("Data");
        var writer = new ArrayBufferWriter<byte>();
        _serializer.Serialize(writer, message);
        var data = writer.WrittenMemory;

        // Act
        var result = _serializer.Deserialize<TestMessage>(data);

        // Assert
        result.ShouldBe(message);
    }

    [Test]
    public void GetContentType_ShouldReturnMemoryPack_WhenAttributeIsPresent()
    {
        var contentType = _serializer.GetContentType<MemoryPackableMessage>();
        contentType.ShouldBe("application/x-memorypack");
    }

    [Test]
    public void GetContentType_ShouldReturnJson_WhenAttributeIsAbsent()
    {
        var contentType = _serializer.GetContentType<TestMessage>();
        contentType.ShouldBe("application/json");
    }

    [Test]
    public void Serialize_ShouldHandlePrimitives()
    {
        var writer = new ArrayBufferWriter<byte>();
        _serializer.Serialize(writer, 42);
        
        var result = _serializer.Deserialize<int>(writer.WrittenMemory);
        result.ShouldBe(42);
    }

    [Test]
    public void Serialize_ShouldHandleCollections()
    {
        var writer = new ArrayBufferWriter<byte>();
        var list = new List<string> { "a", "b" };

        _serializer.Serialize(writer, list);

        var result = _serializer.Deserialize<List<string>>(writer.WrittenMemory);
        result.ShouldBe(list);
    }

    [Test]
    public void Serialize_ShouldHandleNull()
    {
        var writer = new ArrayBufferWriter<byte>();
        _serializer.Serialize(writer, (string?)null);

        // JSON deserialization of "null" literal returns null in C#, but HybridNatsSerializer throws on null result.
        Assert.Throws<InvalidOperationException>(() => _serializer.Deserialize<string?>(writer.WrittenMemory));
    }

    [Test]
    public void Serialize_LargePayload_ShouldNotExceedLimit()
    {
        // Arrange
        int limit = 10;
        var serializerWithLimit = new HybridNatsSerializer(maxPayloadSize: limit);
        var writer = new ArrayBufferWriter<byte>();

        // Let's test with a MemoryPackable type (SizeLimitingBufferWriter only applies to MemoryPack path)
        var mpMessage = new MemoryPackableMessage { Content = new string('a', 20) };
        
        // Assert
        Assert.Throws<InvalidOperationException>(() => serializerWithLimit.Serialize(writer, mpMessage));
    }

    /// <summary>
    /// Validates that the SizeLimitingBufferWriter protection triggers when attempting to serialize
    /// a payload exceeding the default 10MB limit. This is a defensive mechanism to prevent
    /// OutOfMemoryExceptions or denial-of-service attacks via massive payloads.
    /// 
    /// The protection triggers BEFORE memory is committed to the underlying transport writer,
    /// effectively protecting the application's heap.
    /// </summary>
    [Test]
    public void Serialize_PayloadExceeding10MB_ShouldThrowInvalidOperationException()
    {
        // Arrange
        // Use default serializer with 10MB limit (10 * 1024 * 1024 = 10,485,760 bytes)
        var serializer = new HybridNatsSerializer();
        var writer = new ArrayBufferWriter<byte>();

        // Create an object graph that exceeds 10MB
        // Each character in a string is serialized, plus MemoryPack overhead
        // We'll create a ~11MB payload to ensure we exceed the limit
        const int elevenMegabytes = 11 * 1024 * 1024;
        var oversizedMessage = new MemoryPackableMessage { Content = new string('X', elevenMegabytes) };

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => 
            serializer.Serialize(writer, oversizedMessage));

        // Verify the exception message indicates a size limit violation
        exception!.Message.ShouldContain("exceeded maximum payload size");
    }

    #endregion

    #region Format Selection Verification Tests

    /// <summary>
    /// Verifies that [MemoryPackable] types produce MemoryPack binary format, NOT JSON.
    /// MemoryPack binary format does not start with '{' (0x7B) like JSON objects do.
    /// </summary>
    [Test]
    public void Serialize_MemoryPackableType_ShouldNotProduceJsonFormat()
    {
        // Arrange
        var message = new MemoryPackableMessage { Content = "test" };
        var writer = new ArrayBufferWriter<byte>();
        
        // Act
        _serializer.Serialize(writer, message);
        
        // Assert - JSON always starts with '{' (0x7B) for objects
        // MemoryPack has a different binary format
        var firstByte = writer.WrittenSpan[0];
        firstByte.ShouldNotBe((byte)'{', "MemoryPackable types should NOT produce JSON format starting with '{'");
    }

    /// <summary>
    /// Verifies that plain POCOs without [MemoryPackable] produce JSON format.
    /// JSON format for records/objects starts with '{'.
    /// </summary>
    [Test]
    public void Serialize_PlainPoco_ShouldProduceJsonFormat()
    {
        // Arrange
        var message = new TestMessage("test");
        var writer = new ArrayBufferWriter<byte>();
        
        // Act
        _serializer.Serialize(writer, message);
        
        // Assert - JSON for a record/object starts with '{'
        var firstByte = writer.WrittenSpan[0];
        firstByte.ShouldBe((byte)'{', "Plain POCOs should produce JSON format starting with '{'");
    }

    /// <summary>
    /// Verifies that MemoryPack roundtrip works correctly for [MemoryPackable] types.
    /// </summary>
    [Test]
    public void Serialize_MemoryPackableType_ShouldRoundtripCorrectly()
    {
        // Arrange
        var original = new MemoryPackableMessage { Content = "roundtrip test" };
        var writer = new ArrayBufferWriter<byte>();
        
        // Act
        _serializer.Serialize(writer, original);
        var result = _serializer.Deserialize<MemoryPackableMessage>(writer.WrittenMemory);
        
        // Assert
        result.ShouldNotBeNull();
        result.Content.ShouldBe(original.Content);
    }

    #endregion

    #region Attribute Inheritance Edge Case Tests

    /// <summary>
    /// BEHAVIOR DOCUMENTATION: [MemoryPackable] classes inheriting from non-[MemoryPackable] 
    /// base classes CAN serialize successfully. MemoryPack's source generator includes
    /// public properties from base classes in the generated formatter, even when the base
    /// class itself doesn't have the [MemoryPackable] attribute.
    /// 
    /// This test documents that while this works, it's an implicit behavior that developers
    /// should be aware of - the base class doesn't need [MemoryPackable] for its properties
    /// to be serialized when accessed through a derived [MemoryPackable] class.
    /// </summary>
    [Test]
    public void Serialize_MemoryPackableWithNonPackableBase_IncludesBaseProperties()
    {
        // Arrange - DerivedMemoryPackable inherits from NonPackableBase
        var message = new DerivedMemoryPackable { BaseProperty = "base", DerivedProperty = "derived" };
        var writer = new ArrayBufferWriter<byte>();
        
        // Act - MemoryPack serializes successfully
        _serializer.Serialize(writer, message);
        
        // Assert - Serialization succeeds
        writer.WrittenCount.ShouldBeGreaterThan(0);
        
        // Verify roundtrip - Both properties are preserved
        var result = _serializer.Deserialize<DerivedMemoryPackable>(writer.WrittenMemory);
        result.ShouldNotBeNull();
        result.DerivedProperty.ShouldBe("derived");
        result.BaseProperty.ShouldBe("base"); // Base property IS included in serialization
    }

    /// <summary>
    /// Verifies that a properly structured [MemoryPackable] hierarchy (where base is also MemoryPackable)
    /// serializes successfully.
    /// </summary>
    [Test]
    public void Serialize_MemoryPackableWithPackableBase_ShouldSucceed()
    {
        // Arrange
        var message = new FullyPackableDerived { BaseValue = "base", DerivedValue = "derived" };
        var writer = new ArrayBufferWriter<byte>();
        
        // Act - Should not throw
        _serializer.Serialize(writer, message);
        
        // Assert
        writer.WrittenCount.ShouldBeGreaterThan(0);
        
        // Verify roundtrip
        var result = _serializer.Deserialize<FullyPackableDerived>(writer.WrittenMemory);
        result.ShouldNotBeNull();
        result.BaseValue.ShouldBe("base");
        result.DerivedValue.ShouldBe("derived");
    }

    #endregion

    #region Polymorphic Dispatch Tests

    /// <summary>
    /// Critical test: When declared type is 'object' but runtime type is [MemoryPackable],
    /// the serializer uses typeof(T) (object) which is NOT MemoryPackable.
    /// This means it falls back to JSON serialization.
    /// </summary>
    [Test]
    public void Serialize_ObjectDeclaredType_WithMemoryPackableInstance_UsesJsonPath()
    {
        // Arrange - Declared as object, but runtime type is MemoryPackable
        object message = new MemoryPackableMessage { Content = "test" };
        var writer = new ArrayBufferWriter<byte>();
        
        // Act
        _serializer.Serialize<object>(writer, message);
        
        // Assert - Since typeof(T) == typeof(object), NOT MemoryPackable
        // Should fall back to JSON (starts with '{')
        var firstByte = writer.WrittenSpan[0];
        firstByte.ShouldBe((byte)'{', "object declared type should use JSON path even with MemoryPackable instance");
        
        // Also verify content type reflects this
        _serializer.GetContentType<object>().ShouldBe("application/json");
    }

    /// <summary>
    /// Tests that interface-declared types fall back to JSON even when the
    /// concrete instance is [MemoryPackable].
    /// </summary>
    [Test]
    public void Serialize_InterfaceDeclaredType_WithMemoryPackableInstance_UsesJsonPath()
    {
        // Arrange
        ITestInterface message = new MemoryPackableWithInterface { Content = "test" };
        var writer = new ArrayBufferWriter<byte>();
        
        // Act
        _serializer.Serialize<ITestInterface>(writer, message);
        
        // Assert - Interface is not [MemoryPackable], uses JSON
        var firstByte = writer.WrittenSpan[0];
        firstByte.ShouldBe((byte)'{', "Interface declared type should use JSON path");
        
        _serializer.GetContentType<ITestInterface>().ShouldBe("application/json");
    }

    /// <summary>
    /// Tests that List&lt;object&gt; containing MemoryPackable items uses JSON because
    /// typeof(List&lt;object&gt;) is not [MemoryPackable].
    /// </summary>
    [Test]
    public void Serialize_ListOfObject_ContainingMemoryPackableItems_UsesJsonPath()
    {
        // Arrange
        var list = new List<object> 
        { 
            new MemoryPackableMessage { Content = "item1" },
            new TestMessage("item2")
        };
        var writer = new ArrayBufferWriter<byte>();
        
        // Act
        _serializer.Serialize(writer, list);
        
        // Assert - List<object> is not MemoryPackable, uses JSON (starts with '[')
        var firstByte = writer.WrittenSpan[0];
        firstByte.ShouldBe((byte)'[', "List<object> should produce JSON array format");
        
        _serializer.GetContentType<List<object>>().ShouldBe("application/json");
    }

    /// <summary>
    /// Verifies that a strongly-typed List of [MemoryPackable] items uses MemoryPack.
    /// </summary>
    [Test]
    public void Serialize_ListOfMemoryPackable_UsesMemoryPackPath()
    {
        // Arrange
        var list = new MemoryPackableList
        {
            Items = new List<MemoryPackableMessage>
            {
                new() { Content = "item1" },
                new() { Content = "item2" }
            }
        };
        var writer = new ArrayBufferWriter<byte>();
        
        // Act
        _serializer.Serialize(writer, list);
        
        // Assert - Should NOT start with '[' (JSON array)
        var firstByte = writer.WrittenSpan[0];
        firstByte.ShouldNotBe((byte)'[', "MemoryPackable list wrapper should NOT produce JSON format");
        
        _serializer.GetContentType<MemoryPackableList>().ShouldBe("application/x-memorypack");
    }

    /// <summary>
    /// Documents the behavior: When you serialize as MemoryPackable but deserialize as object,
    /// deserialization fails because the binary format doesn't match JSON expectations.
    /// </summary>
    [Test]
    public void Deserialize_MemoryPackPayload_AsObjectType_ShouldFail()
    {
        // Arrange - Serialize as MemoryPackable
        var original = new MemoryPackableMessage { Content = "test" };
        var writer = new ArrayBufferWriter<byte>();
        _serializer.Serialize(writer, original);
        
        // Act & Assert - Try to deserialize as object (expects JSON)
        // This should fail because the payload is MemoryPack binary, not JSON
        Assert.Throws<JsonException>(() => 
            _serializer.Deserialize<object>(writer.WrittenMemory));
    }

    #endregion

    #region Test Types

    public record TestMessage(string Content);

    public interface ITestInterface 
    { 
        string? Content { get; } 
    }

    #endregion

    #region T05/T06: JSON Fallback and Partial Data Tests

    /// <summary>
    /// T05: Fallback Logic Test
    /// Serialize a non-[MemoryPackable] object and verify it produces valid JSON.
    /// </summary>
    [Test]
    public void Serialize_NonMemoryPackableObject_ShouldProduceValidJson()
    {
        // Arrange - PlainPocoForFallback is NOT decorated with [MemoryPackable]
        var message = new PlainPocoForFallback
        {
            Name = "Test User",
            Value = 42,
            Timestamp = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc)
        };
        var writer = new ArrayBufferWriter<byte>();
        
        // Act
        _serializer.Serialize(writer, message);
        
        // Assert - Output should be valid JSON
        var jsonBytes = writer.WrittenSpan;
        jsonBytes[0].ShouldBe((byte)'{', "Non-MemoryPackable should produce JSON starting with '{'");
        
        // Parse as JSON to verify validity
        using var jsonDoc = JsonDocument.Parse(writer.WrittenMemory);
        var root = jsonDoc.RootElement;
        
        // Verify JSON structure contains expected properties (camelCase per default options)
        root.TryGetProperty("name", out var nameProp).ShouldBeTrue("JSON should contain 'name' property");
        nameProp.GetString().ShouldBe("Test User");
        
        root.TryGetProperty("value", out var valueProp).ShouldBeTrue("JSON should contain 'value' property");
        valueProp.GetInt32().ShouldBe(42);
        
        root.TryGetProperty("timestamp", out var timestampProp).ShouldBeTrue("JSON should contain 'timestamp' property");
        timestampProp.ValueKind.ShouldBe(JsonValueKind.String);
    }

    /// <summary>
    /// T05 (Extended): Verify JSON fallback produces roundtrip-compatible output.
    /// </summary>
    [Test]
    public void Serialize_NonMemoryPackableObject_ShouldRoundtripCorrectly()
    {
        // Arrange
        var original = new PlainPocoForFallback
        {
            Name = "Roundtrip Test",
            Value = 123,
            Timestamp = new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc)
        };
        var writer = new ArrayBufferWriter<byte>();
        
        // Act
        _serializer.Serialize(writer, original);
        var result = _serializer.Deserialize<PlainPocoForFallback>(writer.WrittenMemory);
        
        // Assert
        result.ShouldNotBeNull();
        result.Name.ShouldBe(original.Name);
        result.Value.ShouldBe(original.Value);
        result.Timestamp.ShouldBe(original.Timestamp);
    }

    /// <summary>
    /// T06: Partial Data Check
    /// Verify that deserializing JSON with extra fields does not throw - extra data is ignored.
    /// </summary>
    [Test]
    public void Deserialize_JsonWithExtraFields_ShouldNotThrow()
    {
        // Arrange - Create JSON with extra fields not present in target type
        var jsonWithExtraFields = """
        {
            "name": "Test User",
            "value": 42,
            "timestamp": "2024-01-15T10:30:00Z",
            "extraField1": "this field doesn't exist in POCO",
            "extraField2": 999,
            "nestedExtra": { "a": 1, "b": 2 }
        }
        """;
        var jsonBytes = System.Text.Encoding.UTF8.GetBytes(jsonWithExtraFields);
        
        // Act - Should not throw despite extra fields
        var result = _serializer.Deserialize<PlainPocoForFallback>(jsonBytes);
        
        // Assert - Known fields should be populated correctly
        result.ShouldNotBeNull();
        result.Name.ShouldBe("Test User");
        result.Value.ShouldBe(42);
    }

    /// <summary>
    /// T06: Partial Data Check
    /// Verify that deserializing JSON with missing optional fields works correctly.
    /// </summary>
    [Test]
    public void Deserialize_JsonWithMissingOptionalFields_ShouldUseDefaults()
    {
        // Arrange - JSON missing some nullable fields
        var jsonWithMissingFields = """
        {
            "name": "Minimal User"
        }
        """;
        var jsonBytes = System.Text.Encoding.UTF8.GetBytes(jsonWithMissingFields);
        
        // Act
        var result = _serializer.Deserialize<PlainPocoForFallback>(jsonBytes);
        
        // Assert
        result.ShouldNotBeNull();
        result.Name.ShouldBe("Minimal User");
        result.Value.ShouldBe(0); // default int value
        result.Timestamp.ShouldBe(default); // default DateTime
    }

    /// <summary>
    /// T06: Partial Data Check (Variant)
    /// Ensure that DTO properties that exist in memory aren't affected by [JsonIgnore] during the
    /// serialization/deserialization cycle - they're simply absent from the JSON output.
    /// </summary>
    [Test]
    public void Serialize_DtoWithJsonIgnore_ShouldExcludeIgnoredProperties()
    {
        // Arrange
        var dto = new DtoWithIgnoredProperties
        {
            VisibleProperty = "I'm visible",
            SensitiveData = "This should not appear in JSON"
        };
        var writer = new ArrayBufferWriter<byte>();
        
        // Act
        _serializer.Serialize(writer, dto);
        
        // Assert - Parse JSON and verify sensitiveData is NOT present
        using var jsonDoc = JsonDocument.Parse(writer.WrittenMemory);
        var root = jsonDoc.RootElement;
        
        root.TryGetProperty("visibleProperty", out var visible).ShouldBeTrue();
        visible.GetString().ShouldBe("I'm visible");
        
        // The ignored property should NOT be in JSON output
        root.TryGetProperty("sensitiveData", out _).ShouldBeFalse("JsonIgnore property should not appear in JSON");
    }

    #endregion
}

#region MemoryPackable Test Types (must be partial and in file scope for source generation)

[MemoryPack.MemoryPackable]
public partial record MemoryPackableMessage
{
    public string? Content { get; set; }
}

/// <summary>
/// Non-packable base class - used to test inheritance edge cases.
/// </summary>
public class NonPackableBase 
{ 
    public string? BaseProperty { get; set; } 
}

/// <summary>
/// Derived class with [MemoryPackable] but inheriting from non-packable base.
/// This should cause MemoryPack serialization to fail.
/// </summary>
[MemoryPack.MemoryPackable]
public partial class DerivedMemoryPackable : NonPackableBase 
{ 
    public string? DerivedProperty { get; set; } 
}

/// <summary>
/// Properly structured MemoryPackable base class.
/// </summary>
[MemoryPack.MemoryPackable]
[MemoryPack.MemoryPackUnion(0, typeof(FullyPackableDerived))]
public abstract partial class FullyPackableBase
{
    public string? BaseValue { get; set; }
}

/// <summary>
/// Derived class from a properly MemoryPackable base - should serialize successfully.
/// </summary>
[MemoryPack.MemoryPackable]
public partial class FullyPackableDerived : FullyPackableBase
{
    public string? DerivedValue { get; set; }
}

/// <summary>
/// MemoryPackable class that implements an interface.
/// </summary>
[MemoryPack.MemoryPackable]
public partial record MemoryPackableWithInterface : HybridNatsSerializerTests.ITestInterface
{
    public string? Content { get; set; }
}

/// <summary>
/// MemoryPackable wrapper for a list of MemoryPackable items.
/// </summary>
[MemoryPack.MemoryPackable]
public partial class MemoryPackableList
{
    public List<MemoryPackableMessage>? Items { get; set; }
}

/// <summary>
/// Plain POCO for testing JSON fallback (T05).
/// NOT decorated with [MemoryPackable].
/// </summary>
public class PlainPocoForFallback
{
    public string? Name { get; set; }
    public int Value { get; set; }
    public DateTime Timestamp { get; set; }
}

/// <summary>
/// DTO with [JsonIgnore] for testing partial data handling (T06).
/// </summary>
public class DtoWithIgnoredProperties
{
    public string? VisibleProperty { get; set; }
    
    [System.Text.Json.Serialization.JsonIgnore]
    public string? SensitiveData { get; set; }
}

#endregion

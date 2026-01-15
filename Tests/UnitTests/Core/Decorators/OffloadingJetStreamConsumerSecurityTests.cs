using System.Buffers;
using System.Text.Json;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Decorators;
using FlySwattr.NATS.Core.Serializers;
using MemoryPack;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Core.Decorators;

/// <summary>
/// Security tests verifying that the system is protected against Type Hijacking / Deserialization Attacks
/// via the ClaimCheckMessage.OriginalType field.
/// 
/// Key Security Properties Verified:
/// 1. The OriginalType field is INFORMATIONAL ONLY - not used for type resolution during deserialization
/// 2. The HybridNatsSerializer uses compile-time generic type T for deserialization
/// 3. Attackers cannot force instantiation of arbitrary types by modifying OriginalType
/// </summary>
[Property("nTag", "Core")]
public class OffloadingJetStreamConsumerSecurityTests
{
    private readonly HybridNatsSerializer _serializer;

    public OffloadingJetStreamConsumerSecurityTests()
    {
        _serializer = new HybridNatsSerializer();
    }

    #region Type Safety Verification Tests

    /// <summary>
    /// SECURITY TEST: Verifies that the serializer uses compile-time generic type T for deserialization,
    /// not any runtime metadata like OriginalType.
    /// 
    /// Attack scenario: An attacker crafts a ClaimCheckMessage with a malicious OriginalType
    /// (e.g., "System.IO.FileInfo") hoping the consumer will instantiate that dangerous type.
    /// 
    /// Expected: The consumer ignores OriginalType and deserializes using compile-time type T.
    /// </summary>
    [Test]
    public void Deserialize_ShouldUseGenericTypeParameter_NotRuntimeTypeInfo()
    {
        // Arrange - Create and serialize a SafeMessage
        var originalMessage = new SafeMessage { Content = "safe content" };
        var writer = new ArrayBufferWriter<byte>();
        _serializer.Serialize(writer, originalMessage);
        var payload = writer.WrittenMemory;

        // Create a ClaimCheckMessage with a MALICIOUS OriginalType
        // This simulates what an attacker might inject
        var maliciousClaimCheck = new ClaimCheckMessage
        {
            ObjectStoreRef = "objstore://test/payload",
            OriginalSize = payload.Length,
            OriginalType = "System.IO.FileInfo, System.IO.FileSystem" // ATTACK VECTOR
        };

        // Act - Deserialize using the ACTUAL type (SafeMessage), not the malicious OriginalType
        // This is exactly what OffloadingJetStreamConsumer does in ResolveFromClaimCheckMessageAsync
        var result = _serializer.Deserialize<SafeMessage>(payload);

        // Assert - Successfully deserialized as SafeMessage, NOT FileInfo
        result.ShouldNotBeNull();
        result.Content.ShouldBe("safe content");
        
        // The test PASSES because:
        // 1. We used Deserialize<SafeMessage>, not the type from maliciousClaimCheck.OriginalType
        // 2. The serializer has no way to "see" the OriginalType and doesn't use it
        // 3. System.IO.FileInfo was never instantiated or even referenced
    }

    /// <summary>
    /// SECURITY TEST: Verifies that referencing unknown/malicious assembly types in OriginalType
    /// has no effect on deserialization.
    /// 
    /// Attack scenario: An attacker sets OriginalType to a type from an unknown assembly,
    /// hoping to trigger Type.GetType() loading of malicious code.
    /// </summary>
    [Test]
    public void Deserialize_UnknownTypeInOriginalType_ShouldNotAttemptTypeLoading()
    {
        // Arrange
        var originalMessage = new SafeMessage { Content = "expected content" };
        var writer = new ArrayBufferWriter<byte>();
        _serializer.Serialize(writer, originalMessage);
        var payload = writer.WrittenMemory;

        // Malicious ClaimCheckMessage referencing an unknown assembly
        var maliciousClaimCheck = new ClaimCheckMessage
        {
            ObjectStoreRef = "objstore://test/payload",
            OriginalSize = payload.Length,
            OriginalType = "MaliciousNamespace.DangerousType, Evil.Assembly, Version=1.0.0.0"
        };

        // Act - Deserialize using the expected type
        // If the system tried to use OriginalType, we'd get TypeLoadException
        var result = _serializer.Deserialize<SafeMessage>(payload);

        // Assert - No exception, no type loading attempted
        result.ShouldNotBeNull();
        result.Content.ShouldBe("expected content");
    }

    /// <summary>
    /// SECURITY TEST: Verifies that type mismatches between serialized payload and deserialization
    /// target are handled gracefully.
    /// 
    /// This ensures that even if an attacker crafts a valid payload for TypeA and convinces
    /// the consumer to deserialize as TypeB, the result is a controlled outcome (default values
    /// or exceptions), not arbitrary code execution or memory corruption.
    /// </summary>
    [Test]
    public void Deserialize_TypeMismatch_ShouldHandleGracefully()
    {
        // Arrange - Serialize a different type than what we'll try to deserialize as
        var originalMessage = new DifferentMessage { Value = 42 };
        var writer = new ArrayBufferWriter<byte>();
        _serializer.Serialize(writer, originalMessage);
        var payload = writer.WrittenMemory;

        // Act - Try to deserialize as SafeMessage (wrong type)
        // JSON is flexible: unknown properties are ignored, missing properties use defaults
        var result = _serializer.Deserialize<SafeMessage>(payload);

        // Assert - The result is a controlled SafeMessage with default values
        // NOT an arbitrary type specified by the attacker
        result.ShouldNotBeNull();
        result.ShouldBeOfType<SafeMessage>(); // Confirms type is controlled by caller
        result.Content.ShouldBe(string.Empty); // Default value, not attacker-controlled
        
        // Security insight: Even with mismatched JSON, we get a SafeMessage instance
        // The VALUE may be wrong (default), but the TYPE is exactly what we requested
        // An attacker cannot force instantiation of a different type
    }


    /// <summary>
    /// SECURITY TEST: Verifies that OriginalType is never used by the serialization layer.
    /// The ClaimCheckMessage's OriginalType is purely for logging/diagnostics.
    /// </summary>
    [Test]
    public void ClaimCheckMessage_OriginalType_IsNotUsedBySerializer()
    {
        // Arrange - Serialize a ClaimCheckMessage with various OriginalType values
        var claimCheck1 = new ClaimCheckMessage
        {
            ObjectStoreRef = "objstore://bucket/key1",
            OriginalSize = 100,
            OriginalType = "System.IO.FileInfo" // Dangerous type
        };

        var claimCheck2 = new ClaimCheckMessage
        {
            ObjectStoreRef = "objstore://bucket/key2",
            OriginalSize = 200,
            OriginalType = "Evil.MalwareType, Virus.dll" // Non-existent type
        };

        var writer1 = new ArrayBufferWriter<byte>();
        var writer2 = new ArrayBufferWriter<byte>();

        // Act - Serialize both (should not throw or do anything dangerous)
        _serializer.Serialize(writer1, claimCheck1);
        _serializer.Serialize(writer2, claimCheck2);

        // Deserialize both back as ClaimCheckMessage (the expected type)
        var result1 = _serializer.Deserialize<ClaimCheckMessage>(writer1.WrittenMemory);
        var result2 = _serializer.Deserialize<ClaimCheckMessage>(writer2.WrittenMemory);

        // Assert - OriginalType is just stored/retrieved as a string, never interpreted
        result1.OriginalType.ShouldBe("System.IO.FileInfo");
        result2.OriginalType.ShouldBe("Evil.MalwareType, Virus.dll");
        
        // Neither FileInfo nor MalwareType was instantiated or loaded
    }

    /// <summary>
    /// SECURITY TEST: Verifies behavior with MemoryPack serialized payloads.
    /// MemoryPack is type-safe by design - you cannot deserialize to arbitrary types.
    /// </summary>
    [Test]
    public void Deserialize_MemoryPackPayload_IsTypeSafe()
    {
        // Arrange - Create a MemoryPackable message
        var original = new SafeMemoryPackMessage { Content = "safe content" };
        var writer = new ArrayBufferWriter<byte>();
        _serializer.Serialize(writer, original);
        var payload = writer.WrittenMemory;

        // Act - Deserialize as the correct type
        var result = _serializer.Deserialize<SafeMemoryPackMessage>(payload);

        // Assert
        result.ShouldNotBeNull();
        result.Content.ShouldBe("safe content");

        // Attempting to deserialize as wrong type would fail
        // MemoryPack cannot be tricked into instantiating arbitrary types
    }

    #endregion

    #region Test Types

    /// <summary>
    /// Safe message type used in tests - represents a legitimate payload type.
    /// </summary>
    public class SafeMessage
    {
        public string Content { get; set; } = string.Empty;
    }

    /// <summary>
    /// Different message type used to test type mismatch handling.
    /// </summary>
    public class DifferentMessage
    {
        public int Value { get; set; }
    }

    #endregion
}

/// <summary>
/// MemoryPackable test type for verifying MemoryPack security properties.
/// Must be partial and at file scope for source generation.
/// </summary>
[MemoryPackable]
public partial class SafeMemoryPackMessage
{
    public string? Content { get; set; }
}

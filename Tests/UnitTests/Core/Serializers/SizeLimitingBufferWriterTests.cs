using System.Buffers;
using FlySwattr.NATS.Core.Serializers;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Core.Serializers;

[Property("nTag", "Core")]
public class SizeLimitingBufferWriterTests
{
    private const int DefaultMaxSize = 1000;

    private static SizeLimitingBufferWriter CreateSut(ArrayBufferWriter<byte> innerWriter, int maxSize = DefaultMaxSize)
    {
        return new SizeLimitingBufferWriter(innerWriter, maxSize);
    }

    #region Advance Tests

    [Test]
    public void Advance_ShouldDelegateToInner_WhenWithinLimit()
    {
        // Arrange
        var innerWriter = new ArrayBufferWriter<byte>();
        var sut = CreateSut(innerWriter, maxSize: 100);

        // Act - write some data and advance
        var span = sut.GetSpan(50);
        span[..50].Fill(0xFF);
        sut.Advance(50);

        // Assert - inner writer should have received the data
        innerWriter.WrittenCount.ShouldBe(50);
    }

    [Test]
    public void Advance_ShouldNotThrow_WhenExactlyAtLimit()
    {
        // Arrange
        var innerWriter = new ArrayBufferWriter<byte>();
        var sut = CreateSut(innerWriter, maxSize: 100);

        // Act & Assert - should not throw
        var span = sut.GetSpan(100);
        span[..100].Fill(0xFF);
        Should.NotThrow(() => sut.Advance(100));
        innerWriter.WrittenCount.ShouldBe(100);
    }

    [Test]
    public void Advance_ShouldThrowInvalidOperationException_WhenExceedingLimit()
    {
        // Arrange
        var innerWriter = new ArrayBufferWriter<byte>();
        var sut = CreateSut(innerWriter, maxSize: 100);

        // Act & Assert
        var exception = Should.Throw<InvalidOperationException>(() => sut.Advance(101));
        exception.Message.ShouldContain("100");
    }

    [Test]
    public void Advance_ShouldTrackCumulativeSize()
    {
        // Arrange
        var innerWriter = new ArrayBufferWriter<byte>();
        var sut = CreateSut(innerWriter, maxSize: 100);

        // Act - advance multiple times
        var span = sut.GetSpan(30);
        span[..30].Fill(0xFF);
        sut.Advance(30);

        span = sut.GetSpan(30);
        span[..30].Fill(0xFF);
        sut.Advance(30);

        span = sut.GetSpan(30);
        span[..30].Fill(0xFF);
        sut.Advance(30);

        // Assert - should not throw yet (90 total)
        innerWriter.WrittenCount.ShouldBe(90);

        // Now try to advance past the limit
        var exception = Should.Throw<InvalidOperationException>(() => sut.Advance(20));
        exception.Message.ShouldContain("100");
    }

    [Test]
    public void Advance_ShouldIncludeMaxSizeInExceptionMessage()
    {
        // Arrange
        const int customMaxSize = 512;
        var innerWriter = new ArrayBufferWriter<byte>();
        var sut = CreateSut(innerWriter, maxSize: customMaxSize);

        // Act
        var exception = Should.Throw<InvalidOperationException>(() => sut.Advance(customMaxSize + 1));

        // Assert
        exception.Message.ShouldContain("512");
        exception.Message.ShouldContain("exceeded maximum payload size");
    }

    [Test]
    public void Advance_WithZeroCount_ShouldNotAffectTracking()
    {
        // Arrange
        var innerWriter = new ArrayBufferWriter<byte>();
        var sut = CreateSut(innerWriter, maxSize: 100);

        // Act
        var span = sut.GetSpan(50);
        span[..50].Fill(0xFF);
        sut.Advance(50);

        sut.Advance(0); // Should not affect tracking

        span = sut.GetSpan(50);
        span[..50].Fill(0xFF);
        sut.Advance(50); // Should still be within limit (total = 100)

        // Assert
        innerWriter.WrittenCount.ShouldBe(100);
    }

    [Test]
    public void Advance_ShouldThrow_WhenCumulativeSizeExceedsLimit()
    {
        // Arrange
        var innerWriter = new ArrayBufferWriter<byte>();
        var sut = CreateSut(innerWriter, maxSize: 100);

        var span = sut.GetSpan(60);
        span[..60].Fill(0xFF);
        sut.Advance(60);

        // Act & Assert
        var exception = Should.Throw<InvalidOperationException>(() => sut.Advance(50));
        exception.Message.ShouldContain("100");
    }

    #endregion

    #region GetMemory Tests

    [Test]
    public void GetMemory_ShouldDelegateToInner()
    {
        // Arrange
        var innerWriter = new ArrayBufferWriter<byte>();
        var sut = CreateSut(innerWriter);

        // Act
        var result = sut.GetMemory(128);

        // Assert - should return a memory with at least requested size
        result.Length.ShouldBeGreaterThanOrEqualTo(128);
    }

    [Test]
    public void GetMemory_ShouldDelegateWithDefaultSizeHint()
    {
        // Arrange
        var innerWriter = new ArrayBufferWriter<byte>();
        var sut = CreateSut(innerWriter);

        // Act
        var result = sut.GetMemory();

        // Assert - should return a memory (default size hint behavior)
        result.Length.ShouldBeGreaterThan(0);
    }

    #endregion

    #region GetSpan Tests

    [Test]
    public void GetSpan_ShouldDelegateToInner()
    {
        // Arrange
        var innerWriter = new ArrayBufferWriter<byte>();
        var sut = CreateSut(innerWriter);

        // Act
        var result = sut.GetSpan(128);

        // Assert - should return a span with at least requested size
        result.Length.ShouldBeGreaterThanOrEqualTo(128);
    }

    [Test]
    public void GetSpan_ShouldDelegateWithDefaultSizeHint()
    {
        // Arrange
        var innerWriter = new ArrayBufferWriter<byte>();
        var sut = CreateSut(innerWriter);

        // Act
        var result = sut.GetSpan();

        // Assert - should return a span (default size hint behavior)
        result.Length.ShouldBeGreaterThan(0);
    }

    #endregion

    #region Integration-style Tests

    [Test]
    public void MultipleOperations_ShouldTrackSizeCorrectly()
    {
        // Arrange
        var innerWriter = new ArrayBufferWriter<byte>();
        var sut = CreateSut(innerWriter, maxSize: 1000);

        // Act - simulate writing data in chunks
        var span = sut.GetSpan(100);
        span[..100].Fill(0xFF);
        sut.Advance(100);

        var memory = sut.GetMemory(200);
        memory.Span[..200].Fill(0xFF);
        sut.Advance(200);

        span = sut.GetSpan(300);
        span[..300].Fill(0xFF);
        sut.Advance(300);

        // Total so far: 600, remaining capacity: 400

        // Assert - next advance should work within remaining capacity
        span = sut.GetSpan(400);
        span[..400].Fill(0xFF);
        Should.NotThrow(() => sut.Advance(400)); // Total = 1000, exactly at limit

        // Any further advance should throw
        Should.Throw<InvalidOperationException>(() => sut.Advance(1));
    }

    [Test]
    public void Constructor_ShouldAcceptMaxSizeOfZero()
    {
        // Arrange
        var innerWriter = new ArrayBufferWriter<byte>();
        var sut = CreateSut(innerWriter, maxSize: 0);

        // Assert - any advance should throw
        Should.Throw<InvalidOperationException>(() => sut.Advance(1));
    }

    #endregion
}

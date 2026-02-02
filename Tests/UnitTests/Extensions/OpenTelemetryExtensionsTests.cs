using FlySwattr.NATS.Core.Telemetry;
using FlySwattr.NATS.Extensions;
using NSubstitute;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using TUnit.Core;

namespace FlySwattr.NATS.UnitTests.Extensions;

/// <summary>
/// Unit tests for OpenTelemetry extension methods.
/// </summary>
public class OpenTelemetryExtensionsTests
{
    #region TracerProviderBuilder Tests

    [Test]
    public async Task AddFlySwattrNatsInstrumentation_TracerProvider_ShouldNotThrowWithValidBuilder()
    {
        // Arrange
        var builder = Substitute.For<TracerProviderBuilder>();
        builder.AddSource(Arg.Any<string[]>()).Returns(builder);

        // Act & Assert - should not throw
        var result = builder.AddFlySwattrNatsInstrumentation();
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task AddFlySwattrNatsInstrumentation_TracerProvider_ShouldAddCorrectActivitySource()
    {
        // Arrange
        var builder = Substitute.For<TracerProviderBuilder>();
        builder.AddSource(Arg.Any<string[]>()).Returns(builder);

        // Act
        builder.AddFlySwattrNatsInstrumentation();

        // Assert
        builder.Received(1).AddSource(NatsTelemetry.ActivitySourceName);
        await Task.CompletedTask;
    }

    [Test]
    public async Task AddFlySwattrNatsInstrumentation_TracerProvider_ShouldThrowOnNullBuilder()
    {
        // Arrange
        TracerProviderBuilder? builder = null;

        // Act & Assert
        await Assert.That(() => builder!.AddFlySwattrNatsInstrumentation()).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task AddFlySwattrNatsInstrumentation_TracerProvider_ShouldReturnBuilderForChaining()
    {
        // Arrange
        var builder = Substitute.For<TracerProviderBuilder>();
        builder.AddSource(Arg.Any<string[]>()).Returns(builder);

        // Act
        var result = builder.AddFlySwattrNatsInstrumentation();

        // Assert
        await Assert.That(result).IsSameReferenceAs(builder);
    }

    #endregion

    #region MeterProviderBuilder Tests

    [Test]
    public async Task AddFlySwattrNatsInstrumentation_MeterProvider_ShouldNotThrowWithValidBuilder()
    {
        // Arrange
        var builder = Substitute.For<MeterProviderBuilder>();
        builder.AddMeter(Arg.Any<string[]>()).Returns(builder);

        // Act & Assert - should not throw
        var result = builder.AddFlySwattrNatsInstrumentation();
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task AddFlySwattrNatsInstrumentation_MeterProvider_ShouldAddCorrectMeter()
    {
        // Arrange
        var builder = Substitute.For<MeterProviderBuilder>();
        builder.AddMeter(Arg.Any<string[]>()).Returns(builder);

        // Act
        builder.AddFlySwattrNatsInstrumentation();

        // Assert
        builder.Received(1).AddMeter(NatsTelemetry.MeterName);
        await Task.CompletedTask;
    }

    [Test]
    public async Task AddFlySwattrNatsInstrumentation_MeterProvider_ShouldThrowOnNullBuilder()
    {
        // Arrange
        MeterProviderBuilder? builder = null;

        // Act & Assert
        await Assert.That(() => builder!.AddFlySwattrNatsInstrumentation()).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task AddFlySwattrNatsInstrumentation_MeterProvider_ShouldReturnBuilderForChaining()
    {
        // Arrange
        var builder = Substitute.For<MeterProviderBuilder>();
        builder.AddMeter(Arg.Any<string[]>()).Returns(builder);

        // Act
        var result = builder.AddFlySwattrNatsInstrumentation();

        // Assert
        await Assert.That(result).IsSameReferenceAs(builder);
    }

    #endregion

    #region Integration Tests

    [Test]
    public async Task ActivitySourceName_ShouldMatchTelemetryConstant()
    {
        // The extension method should use the constant from NatsTelemetry
        var sourceName = NatsTelemetry.ActivitySourceName;
        await Assert.That(sourceName).IsEqualTo("FlySwattr.NATS");
    }

    [Test]
    public async Task MeterName_ShouldMatchTelemetryConstant()
    {
        // The extension method should use the constant from NatsTelemetry
        var meterName = NatsTelemetry.MeterName;
        await Assert.That(meterName).IsEqualTo("FlySwattr.NATS");
    }

    #endregion
}

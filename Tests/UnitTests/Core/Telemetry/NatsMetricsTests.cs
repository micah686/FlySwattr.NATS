using System.Diagnostics;
using System.Diagnostics.Metrics;
using FlySwattr.NATS.Core.Telemetry;
using TUnit.Core;

namespace FlySwattr.NATS.UnitTests.Core.Telemetry;

/// <summary>
/// Measurement record for testing.
/// </summary>
internal record MeasurementRecord(string InstrumentName, long LongValue, double DoubleValue, KeyValuePair<string, object?>[] Tags);

/// <summary>
/// Unit tests for NatsTelemetry metrics instrumentation.
/// </summary>
public class NatsMetricsTests : IAsyncDisposable
{
    private readonly MeterListener _meterListener;
    private readonly List<MeasurementRecord> _measurements = [];
    private readonly object _lock = new();

    public NatsMetricsTests()
    {
        _meterListener = new MeterListener();
        _meterListener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == NatsTelemetry.MeterName)
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };
        _meterListener.SetMeasurementEventCallback<long>(OnMeasurementRecorded);
        _meterListener.SetMeasurementEventCallback<double>(OnMeasurementRecorded);
        _meterListener.Start();
    }

    private void OnMeasurementRecorded(Instrument instrument, long measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
    {
        lock (_lock)
        {
            _measurements.Add(new MeasurementRecord(instrument.Name, measurement, 0, tags.ToArray()));
        }
    }

    private void OnMeasurementRecorded(Instrument instrument, double measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
    {
        lock (_lock)
        {
            _measurements.Add(new MeasurementRecord(instrument.Name, 0, measurement, tags.ToArray()));
        }
    }

    public ValueTask DisposeAsync()
    {
        _meterListener.Dispose();
        return ValueTask.CompletedTask;
    }

    #region Meter Configuration Tests

    [Test]
    public async Task MeterName_ShouldBeFlySwattrNats()
    {
        var meterName = NatsTelemetry.MeterName;
        await Assert.That(meterName).IsEqualTo("FlySwattr.NATS");
    }

    [Test]
    public async Task Meter_ShouldBeCreatedWithCorrectName()
    {
        var meter = NatsTelemetry.Meter;
        await Assert.That(meter).IsNotNull();
        await Assert.That(meter.Name).IsEqualTo("FlySwattr.NATS");
    }

    [Test]
    public async Task Meter_ShouldHaveVersion()
    {
        var version = NatsTelemetry.Meter.Version;
        await Assert.That(version).IsEqualTo("1.0.0");
    }

    #endregion

    #region Counter Tests

    [Test]
    public async Task MessagesPublished_ShouldBeCounter()
    {
        var counter = NatsTelemetry.MessagesPublished;
        await Assert.That(counter).IsNotNull();
        await Assert.That(counter.Name).IsEqualTo("flyswattr.nats.messages.published");
    }

    [Test]
    public async Task MessagesReceived_ShouldBeCounter()
    {
        var counter = NatsTelemetry.MessagesReceived;
        await Assert.That(counter).IsNotNull();
        await Assert.That(counter.Name).IsEqualTo("flyswattr.nats.messages.received");
    }

    [Test]
    public async Task MessagesFailed_ShouldBeCounter()
    {
        var counter = NatsTelemetry.MessagesFailed;
        await Assert.That(counter).IsNotNull();
        await Assert.That(counter.Name).IsEqualTo("flyswattr.nats.messages.failed");
    }

    [Test]
    public async Task MessagesPublished_ShouldRecordMeasurements()
    {
        // Arrange
        var tags = new TagList { { "test.subject", "test-subject" } };

        // Act
        NatsTelemetry.MessagesPublished.Add(1, tags);
        _meterListener.RecordObservableInstruments();

        // Assert
        MeasurementRecord? measurement;
        lock (_lock)
        {
            measurement = _measurements.FirstOrDefault(m => m.InstrumentName == "flyswattr.nats.messages.published");
        }
        await Assert.That(measurement?.LongValue).IsEqualTo(1);
    }

    #endregion

    #region Histogram Tests

    [Test]
    public async Task MessageProcessingDuration_ShouldBeHistogram()
    {
        var histogram = NatsTelemetry.MessageProcessingDuration;
        await Assert.That(histogram).IsNotNull();
        await Assert.That(histogram.Name).IsEqualTo("flyswattr.nats.message.processing.duration");
        await Assert.That(histogram.Unit).IsEqualTo("ms");
    }

    [Test]
    public async Task PublishDuration_ShouldBeHistogram()
    {
        var histogram = NatsTelemetry.PublishDuration;
        await Assert.That(histogram).IsNotNull();
        await Assert.That(histogram.Name).IsEqualTo("flyswattr.nats.publish.duration");
        await Assert.That(histogram.Unit).IsEqualTo("ms");
    }

    [Test]
    public async Task KvOperationDuration_ShouldBeHistogram()
    {
        var histogram = NatsTelemetry.KvOperationDuration;
        await Assert.That(histogram).IsNotNull();
        await Assert.That(histogram.Name).IsEqualTo("flyswattr.nats.kv.operation.duration");
        await Assert.That(histogram.Unit).IsEqualTo("ms");
    }

    [Test]
    public async Task ObjectStoreOperationDuration_ShouldBeHistogram()
    {
        var histogram = NatsTelemetry.ObjectStoreOperationDuration;
        await Assert.That(histogram).IsNotNull();
        await Assert.That(histogram.Name).IsEqualTo("flyswattr.nats.objectstore.operation.duration");
        await Assert.That(histogram.Unit).IsEqualTo("ms");
    }

    [Test]
    public async Task MessageProcessingDuration_ShouldRecordMeasurements()
    {
        // Arrange
        var tags = new TagList { { "test.subject", "test-subject" } };

        // Act
        NatsTelemetry.MessageProcessingDuration.Record(42.5, tags);
        _meterListener.RecordObservableInstruments();

        // Assert
        MeasurementRecord? measurement;
        lock (_lock)
        {
            measurement = _measurements.FirstOrDefault(m => m.InstrumentName == "flyswattr.nats.message.processing.duration");
        }
        await Assert.That(measurement?.DoubleValue).IsEqualTo(42.5);
    }

    #endregion

    #region Semantic Convention Tests

    [Test]
    public async Task MessagesPublished_ShouldRecordWithDestinationTag()
    {
        // Arrange - use unique subject to avoid confusion with other tests
        var uniqueSubject = $"orders.created.{Guid.NewGuid():N}";
        var tags = new TagList { { NatsTelemetry.MessagingDestinationName, uniqueSubject } };

        // Act
        NatsTelemetry.MessagesPublished.Add(1, tags);
        _meterListener.RecordObservableInstruments();

        // Assert - find measurement with the specific tag value
        KeyValuePair<string, object?>? destinationTag;
        lock (_lock)
        {
            var measurement = _measurements.FirstOrDefault(m => 
                m.InstrumentName == "flyswattr.nats.messages.published" && 
                m.Tags.Any(t => t.Key == NatsTelemetry.MessagingDestinationName && t.Value?.ToString() == uniqueSubject));
            destinationTag = measurement?.Tags.FirstOrDefault(t => t.Key == NatsTelemetry.MessagingDestinationName);
        }
        await Assert.That(destinationTag?.Value?.ToString()).IsEqualTo(uniqueSubject);
    }

    [Test]
    public async Task MessagesFailed_ShouldRecordWithErrorTypeTag()
    {
        // Arrange - use unique subject to avoid confusion with other tests
        var uniqueSubject = $"orders.failed.{Guid.NewGuid():N}";
        var tags = new TagList 
        { 
            { NatsTelemetry.MessagingDestinationName, uniqueSubject },
            { "error.type", "InvalidOperationException" }
        };

        // Act
        NatsTelemetry.MessagesFailed.Add(1, tags);
        _meterListener.RecordObservableInstruments();

        // Assert - find measurement with the specific tag values
        KeyValuePair<string, object?>? errorTag;
        lock (_lock)
        {
            var measurement = _measurements.FirstOrDefault(m => 
                m.InstrumentName == "flyswattr.nats.messages.failed" && 
                m.Tags.Any(t => t.Key == NatsTelemetry.MessagingDestinationName && t.Value?.ToString() == uniqueSubject));
            errorTag = measurement?.Tags.FirstOrDefault(t => t.Key == "error.type");
        }
        await Assert.That(errorTag?.Value?.ToString()).IsEqualTo("InvalidOperationException");
    }

    #endregion
}

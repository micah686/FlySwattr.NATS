using System.Buffers;

namespace FlySwattr.NATS.Core.Serializers;

internal class SizeLimitingBufferWriter : IBufferWriter<byte>
{
    private readonly IBufferWriter<byte> _inner;
    private readonly int _maxSize;
    private int _committedSize;
    private int _reservedSize;

    public SizeLimitingBufferWriter(IBufferWriter<byte> inner, int maxSize)
    {
        _inner = inner;
        _maxSize = maxSize;
    }

    public void Advance(int count)
    {
        if (count < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count), "Count cannot be negative.");
        }
        
        if (_committedSize > _maxSize - count) // Check for overflow and limit exceeded
        {
            throw new InvalidOperationException($"Serialization exceeded maximum payload size of {_maxSize} bytes.");
        }
        _committedSize += count;
        _reservedSize = _committedSize;

        _inner.Advance(count);
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        var remaining = GetRemainingCapacity(sizeHint, nameof(sizeHint));
        var memory = _inner.GetMemory(remaining.RequestedHint);
        var visibleLength = remaining.RequestedHint == 0
            ? Math.Min(memory.Length, remaining.RemainingCapacity)
            : Math.Min(memory.Length, remaining.RequestedHint);
        TrackReservation(visibleLength);
        return memory[..visibleLength];
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        var remaining = GetRemainingCapacity(sizeHint, nameof(sizeHint));
        var span = _inner.GetSpan(remaining.RequestedHint);
        var visibleLength = remaining.RequestedHint == 0
            ? Math.Min(span.Length, remaining.RemainingCapacity)
            : Math.Min(span.Length, remaining.RequestedHint);
        TrackReservation(visibleLength);
        return span[..visibleLength];
    }

    private (int RequestedHint, int RemainingCapacity) GetRemainingCapacity(int sizeHint, string paramName)
    {
        if (sizeHint < 0)
        {
            throw new ArgumentOutOfRangeException(paramName, "Size hint cannot be negative.");
        }

        var remaining = _maxSize - Math.Max(_committedSize, _reservedSize);
        if (remaining <= 0)
        {
            throw new InvalidOperationException($"Serialization exceeded maximum payload size of {_maxSize} bytes.");
        }

        if (sizeHint == 0)
        {
            return (0, remaining);
        }

        if (sizeHint > remaining)
        {
            throw new InvalidOperationException($"Serialization exceeded maximum payload size of {_maxSize} bytes.");
        }

        return (sizeHint, remaining);
    }

    private void TrackReservation(int visibleLength)
    {
        if (visibleLength <= 0)
        {
            return;
        }

        if (_committedSize > _maxSize - visibleLength)
        {
            throw new InvalidOperationException($"Serialization exceeded maximum payload size of {_maxSize} bytes.");
        }

        var reservedSize = _committedSize + visibleLength;
        if (reservedSize > _reservedSize)
        {
            _reservedSize = reservedSize;
        }
    }
}

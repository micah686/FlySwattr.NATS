using System.Buffers;

namespace FlySwattr.NATS.Core.Serializers;

internal class SizeLimitingBufferWriter : IBufferWriter<byte>
{
    private readonly IBufferWriter<byte> _inner;
    private readonly int _maxSize;
    private int _currentSize;

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
        
        if (_currentSize > _maxSize - count) // Check for overflow and limit exceeded
        {
            throw new InvalidOperationException($"Serialization exceeded maximum payload size of {_maxSize} bytes.");
        }
        _currentSize += count;
        _inner.Advance(count);
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        return _inner.GetMemory(GetAllowedSizeHint(sizeHint));
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        return _inner.GetSpan(GetAllowedSizeHint(sizeHint));
    }

    private int GetAllowedSizeHint(int sizeHint)
    {
        if (sizeHint < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(sizeHint), "Size hint cannot be negative.");
        }

        var remaining = _maxSize - _currentSize;
        if (remaining <= 0)
        {
            throw new InvalidOperationException($"Serialization exceeded maximum payload size of {_maxSize} bytes.");
        }

        if (sizeHint == 0)
        {
            return remaining;
        }

        if (sizeHint > remaining)
        {
            throw new InvalidOperationException($"Serialization exceeded maximum payload size of {_maxSize} bytes.");
        }

        return sizeHint;
    }
}

// ReSharper disable CheckNamespace
// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Abstractions;

/// <summary>
/// Indicates the type of change in a KV watch event.
/// </summary>
public enum KvChangeType
{
    /// <summary>A value was put (created or updated).</summary>
    Put,
    /// <summary>A value was deleted.</summary>
    Delete
}

/// <summary>
/// Represents a change event from a KV watch operation.
/// Explicitly surfaces delete operations so callers can distinguish between
/// a missing value (never existed) and a deleted value.
/// </summary>
/// <typeparam name="T">The type of value being watched.</typeparam>
/// <param name="Type">The type of change (Put or Delete).</param>
/// <param name="Value">The value, or default if this is a delete event.</param>
/// <param name="Key">The key that was changed.</param>
/// <param name="Revision">The revision number of this change.</param>
public record KvChangeEvent<T>(KvChangeType Type, T? Value, string Key, ulong Revision)
{
    /// <summary>
    /// Returns true if this event represents a deletion.
    /// </summary>
    public bool IsDelete => Type == KvChangeType.Delete;
    
    /// <summary>
    /// Returns true if this event represents a put (create or update).
    /// </summary>
    public bool IsPut => Type == KvChangeType.Put;
}

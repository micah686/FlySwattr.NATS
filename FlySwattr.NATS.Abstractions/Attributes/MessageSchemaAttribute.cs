namespace FlySwattr.NATS.Abstractions.Attributes;

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false)]
public sealed class MessageSchemaAttribute : Attribute
{
    public MessageSchemaAttribute(int version)
    {
        if (version <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(version), "Schema version must be greater than zero.");
        }

        Version = version;
    }

    public int Version { get; }

    /// <summary>
    /// The minimum schema version this type can accept when reading older messages.
    /// Defines the backward-compatibility window: messages with version &lt; MinSupportedVersion
    /// will be rejected even if the reader's version is higher.
    /// Default: 1 (accepts all prior versions)
    /// </summary>
    public int MinSupportedVersion { get; set; } = 1;
}

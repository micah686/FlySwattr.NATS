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
}

namespace FlySwattr.NATS.Core.Configuration;

/// <summary>
/// Optional alias configuration for message type metadata.
/// </summary>
public class MessageTypeAliasOptions
{
    /// <summary>
    /// Maps aliases to CLR types for replay and inspection workflows.
    /// </summary>
    public Dictionary<string, Type> AliasMappings { get; } = new(StringComparer.Ordinal);
}

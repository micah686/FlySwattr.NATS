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

    /// <summary>
    /// When <see langword="true"/>, every type that passes through
    /// <see cref="IMessageTypeAliasRegistry.GetAlias"/> must have been registered
    /// explicitly via <see cref="IMessageTypeAliasRegistry.Register{T}"/> or
    /// <see cref="AliasMappings"/>. An <see cref="InvalidOperationException"/> is thrown
    /// on first use of an unregistered type.
    /// <para>
    /// Recommended for production deployments: forces all wire-visible types to carry a
    /// stable, documented alias and prevents two types with the same short name from
    /// silently resolving to the same alias.
    /// </para>
    /// </summary>
    public bool RequireExplicitAliases { get; set; }
}

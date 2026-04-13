using System.Collections.Concurrent;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Configuration;
using Microsoft.Extensions.Options;

namespace FlySwattr.NATS.Core.Services;

internal sealed class MessageTypeAliasRegistry : IMessageTypeAliasRegistry
{
    private readonly ConcurrentDictionary<string, Type> _aliasToType = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<Type, string> _typeToAlias = new();
    private readonly bool _requireExplicitAliases;

    public MessageTypeAliasRegistry(IOptions<MessageTypeAliasOptions>? options = null)
    {
        if (options?.Value is not { } value)
        {
            return;
        }

        _requireExplicitAliases = value.RequireExplicitAliases;

        foreach (var mapping in value.AliasMappings)
        {
            _aliasToType[mapping.Key] = mapping.Value;
            _typeToAlias[mapping.Value] = mapping.Key;
        }
    }

    public string GetAlias(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);

        if (_typeToAlias.TryGetValue(messageType, out var existing))
        {
            return existing;
        }

        if (_requireExplicitAliases)
        {
            throw new InvalidOperationException(
                $"No alias is registered for type '{messageType.FullName}'. " +
                "Register it via MessageTypeAliasOptions.AliasMappings or " +
                "IMessageTypeAliasRegistry.Register<T>() before publishing or consuming " +
                "messages of this type. Alternatively, set RequireExplicitAliases = false " +
                "to allow implicit type.Name aliases (not recommended for production).");
        }

        return _typeToAlias.GetOrAdd(messageType, type =>
        {
            var alias = type.Name;
            // Mirror into the reverse lookup so Resolve() works for types encountered at
            // runtime without requiring an explicit Register<T>() call. TryAdd is used so
            // a pre-registered explicit alias always wins over the implicit default.
            _aliasToType.TryAdd(alias, type);
            return alias;
        });
    }

    public Type? Resolve(string alias)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(alias);

        if (_aliasToType.TryGetValue(alias, out var type))
        {
            return type;
        }

        type = Type.GetType(alias, throwOnError: false);
        if (type != null)
        {
            _aliasToType.TryAdd(alias, type);
            _typeToAlias.TryAdd(type, alias);
        }

        return type;
    }

    public void Register<T>(string alias)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(alias);

        _aliasToType[alias] = typeof(T);
        _typeToAlias[typeof(T)] = alias;
    }
}

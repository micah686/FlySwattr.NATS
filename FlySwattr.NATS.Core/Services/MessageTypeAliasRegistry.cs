using System.Collections.Concurrent;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Core.Configuration;
using Microsoft.Extensions.Options;

namespace FlySwattr.NATS.Core.Services;

internal sealed class MessageTypeAliasRegistry : IMessageTypeAliasRegistry
{
    private readonly ConcurrentDictionary<string, Type> _aliasToType = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<Type, string> _typeToAlias = new();

    public MessageTypeAliasRegistry(IOptions<MessageTypeAliasOptions>? options = null)
    {
        if (options?.Value is not { } value)
        {
            return;
        }

        foreach (var mapping in value.AliasMappings)
        {
            _aliasToType[mapping.Key] = mapping.Value;
            _typeToAlias[mapping.Value] = mapping.Key;
        }
    }

    public string GetAlias(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);

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

        _aliasToType.TryGetValue(alias, out var type);
        return type;
    }

    public void Register<T>(string alias)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(alias);

        _aliasToType[alias] = typeof(T);
        _typeToAlias[typeof(T)] = alias;
    }
}

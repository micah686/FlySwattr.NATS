using System.Collections.Concurrent;
using System.Reflection;
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

        return _typeToAlias.GetOrAdd(messageType, static type => type.Name);
    }

    public Type? Resolve(string alias)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(alias);

        if (_aliasToType.TryGetValue(alias, out var explicitType))
        {
            return explicitType;
        }

        return AppDomain.CurrentDomain.GetAssemblies()
            .SelectMany(static assembly =>
            {
                try
                {
                    return assembly.GetTypes();
                }
                catch (ReflectionTypeLoadException ex)
                {
                    return ex.Types.OfType<Type>();
                }
            })
            .FirstOrDefault(type => string.Equals(type.Name, alias, StringComparison.Ordinal) ||
                                    string.Equals(type.FullName, alias, StringComparison.Ordinal));
    }

    public void Register<T>(string alias)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(alias);

        _aliasToType[alias] = typeof(T);
        _typeToAlias[typeof(T)] = alias;
    }
}

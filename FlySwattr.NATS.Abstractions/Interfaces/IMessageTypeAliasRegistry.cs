// ReSharper disable CheckNamespace
namespace FlySwattr.NATS.Abstractions;

/// <summary>
/// Resolves stable, application-defined aliases for message types.
/// </summary>
public interface IMessageTypeAliasRegistry
{
    string GetAlias(Type messageType);
    Type? Resolve(string alias);
    void Register<T>(string alias);
}

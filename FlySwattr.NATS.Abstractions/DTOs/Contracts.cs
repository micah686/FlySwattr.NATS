// ReSharper disable CheckNamespace
// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Abstractions;

public record MessageHeaders(Dictionary<string, string> Headers)
{
    public static MessageHeaders Empty => new(new Dictionary<string, string>());
}


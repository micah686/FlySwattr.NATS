// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Abstractions.Exceptions;

public class NullMessagePayloadException : Exception
{
    public NullMessagePayloadException(string subject)
        : base($"Null payload received on subject: {subject}") { }
}

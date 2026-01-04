// ReSharper disable CheckNamespace
// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Abstractions;

public record ObjectMetaInfo(
    string Name,
    string? Description = null,
    IDictionary<string, string>? Headers = null
);

public record ObjectInfo(
    string Bucket,
    string Key,
    long Size,
    DateTimeOffset LastModified,
    string Digest,
    bool Deleted = false,
    string? Description = null,
    IDictionary<string, string>? Headers = null
);
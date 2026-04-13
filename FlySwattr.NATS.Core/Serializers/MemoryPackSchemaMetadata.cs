using System.Collections.Concurrent;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using FlySwattr.NATS.Abstractions.Attributes;
using MemoryPack;
using NATS.Client.Core;

namespace FlySwattr.NATS.Core.Serializers;

internal sealed record MemoryPackSchemaDescriptor(
    string SchemaId,
    int SchemaVersion,
    int MinSupportedVersion,
    string SchemaFingerprint);

internal static class MemoryPackSchemaMetadata
{
    private static readonly ConcurrentDictionary<Type, bool> IsMemoryPackableCache = new();
    private static readonly ConcurrentDictionary<Type, MemoryPackSchemaDescriptor> DescriptorCache = new();

    public const string SchemaIdHeader = "X-Schema-Id";
    public const string SchemaVersionHeader = "X-Schema-Version";
    public const string SchemaFingerprintHeader = "X-Schema-Fingerprint";

    public static bool IsMemoryPackable<T>()
    {
        return IsMemoryPackable(typeof(T));
    }

    public static bool IsMemoryPackable(Type type)
    {
        return IsMemoryPackableCache.GetOrAdd(type, static candidate =>
            candidate.IsDefined(typeof(MemoryPackableAttribute), inherit: false));
    }

    public static MemoryPackSchemaDescriptor GetDescriptor<T>()
    {
        return GetDescriptor(typeof(T));
    }

    public static MemoryPackSchemaDescriptor GetDescriptor(Type type)
    {
        return DescriptorCache.GetOrAdd(type, static candidate =>
        {
            var schemaId = candidate.FullName ?? candidate.Name;
            var attr = candidate.GetCustomAttribute<MessageSchemaAttribute>();
            var schemaVersion = attr?.Version ?? 1;
            var minSupportedVersion = attr?.MinSupportedVersion ?? 1;
            var schemaFingerprint = ComputeFingerprint(candidate);
            return new MemoryPackSchemaDescriptor(schemaId, schemaVersion, minSupportedVersion, schemaFingerprint);
        });
    }

    public static void AddHeadersIfNeeded<T>(NatsHeaders headers)
    {
        if (!IsMemoryPackable<T>())
        {
            return;
        }

        var descriptor = GetDescriptor<T>();
        headers[SchemaIdHeader] = descriptor.SchemaId;
        headers[SchemaVersionHeader] = descriptor.SchemaVersion.ToString();
        headers[SchemaFingerprintHeader] = descriptor.SchemaFingerprint;
    }

    private static string ComputeFingerprint(Type type)
    {
        var builder = new StringBuilder();
        builder.Append(type.FullName ?? type.Name);
        builder.Append('|');

        foreach (var declaringType in GetTypeHierarchy(type))
        {
            foreach (var member in GetSerializableMembers(declaringType))
            {
                builder.Append(member.Name);
                builder.Append(':');
                builder.Append(GetMemberType(member).FullName ?? GetMemberType(member).Name);
                builder.Append('|');
            }
        }

        var utf8 = Encoding.UTF8.GetBytes(builder.ToString());
        var hash = SHA256.HashData(utf8);
        return Convert.ToHexString(hash);
    }

    private static IEnumerable<Type> GetTypeHierarchy(Type type)
    {
        var chain = new Stack<Type>();
        var current = type;
        while (current != null && current != typeof(object))
        {
            chain.Push(current);
            current = current.BaseType;
        }

        while (chain.Count > 0)
        {
            yield return chain.Pop();
        }
    }

    private static IEnumerable<MemberInfo> GetSerializableMembers(Type type)
    {
        const BindingFlags Flags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly;

        var fields = type
            .GetFields(Flags)
            .Where(static field => !field.IsStatic && !field.IsDefined(typeof(NonSerializedAttribute), inherit: false))
            .Cast<MemberInfo>();

        var properties = type
            .GetProperties(Flags)
            .Where(static property => property.GetIndexParameters().Length == 0)
            .Where(static property => property.GetMethod != null || property.SetMethod != null)
            .Cast<MemberInfo>();

        return fields
            .Concat(properties)
            .OrderBy(static member => member.MetadataToken);
    }

    private static Type GetMemberType(MemberInfo member)
    {
        return member switch
        {
            FieldInfo field => field.FieldType,
            PropertyInfo property => property.PropertyType,
            _ => typeof(object)
        };
    }
}

using MemoryPack;

namespace FlySwattr.NATS.Core.Serializers;

[MemoryPackable]
internal partial class MemoryPackSchemaEnvelope
{
    public required string SchemaId { get; set; }
    public int SchemaVersion { get; set; }
    public required string SchemaFingerprint { get; set; }
    public required byte[] Payload { get; set; }
}

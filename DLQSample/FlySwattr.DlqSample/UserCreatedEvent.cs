using MemoryPack;

namespace FlySwattr.DlqSample;

[MemoryPackable]
public partial record UserCreatedEvent(string UserId, string Username, string Email);
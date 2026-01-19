using MemoryPack;

namespace Shared;

/// <summary>
/// Represents an order created event for pub/sub and JetStream messaging.
/// </summary>
[MemoryPackable]
public partial record OrderCreatedEvent(
    string OrderId,
    string CustomerId,
    decimal Amount,
    DateTime CreatedAt);

/// <summary>
/// Represents a query for order information (request/reply pattern).
/// </summary>
[MemoryPackable]
public partial record OrderQuery(string OrderId);

/// <summary>
/// Represents the response to an order query (request/reply pattern).
/// </summary>
[MemoryPackable]
public partial record OrderQueryResponse(
    string OrderId,
    string CustomerId,
    decimal Amount,
    DateTime CreatedAt,
    string Status);

/// <summary>
/// Represents a task for queue group processing.
/// </summary>
[MemoryPackable]
public partial record WorkTask(
    string TaskId,
    string Description,
    int Priority,
    DateTime ScheduledAt);

/// <summary>
/// Represents a poison message that will intentionally fail processing.
/// Used to demonstrate DLQ functionality.
/// </summary>
[MemoryPackable]
public partial record PoisonMessage(
    string Id,
    string Reason,
    DateTime CreatedAt);

/// <summary>
/// Represents configuration stored in KV Store.
/// </summary>
[MemoryPackable]
public partial record AppConfiguration(
    string Key,
    string Value,
    DateTime UpdatedAt);

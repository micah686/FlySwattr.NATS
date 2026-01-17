using MemoryPack;

namespace Shared;

/// <summary>
/// Published when a new order is placed.
/// Demonstrates Pub/Sub pattern.
/// </summary>
/// <param name="OrderId">Unique identifier for the order</param>
/// <param name="Sku">Stock keeping unit for the ordered item</param>
[MemoryPackable]
public partial record OrderPlacedEvent(Guid OrderId, string Sku);

/// <summary>
/// Request to check if an item is in stock.
/// Demonstrates Request/Reply pattern.
/// </summary>
/// <param name="Sku">Stock keeping unit to check</param>
[MemoryPackable]
public partial record CheckStockRequest(string Sku);

/// <summary>
/// Response to a stock check request.
/// </summary>
/// <param name="Sku">Stock keeping unit</param>
/// <param name="InStock">Whether the item is in stock</param>
/// <param name="Quantity">Available quantity</param>
[MemoryPackable]
public partial record StockStatusResponse(string Sku, bool InStock, int Quantity);

/// <summary>
/// Command to ship an order.
/// Demonstrates Queue Groups pattern for load balancing.
/// </summary>
/// <param name="OrderId">The order to ship</param>
[MemoryPackable]
public partial record ShipOrderCommand(Guid OrderId);

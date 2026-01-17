using System.Collections.Concurrent;
using FlySwattr.NATS.Abstractions;
using Microsoft.Extensions.Logging;

namespace FlySwattr.NATS.Core;

public interface IDlqPolicyRegistry
{
    void Register(string stream, string consumer, DeadLetterPolicy policy);
    void Unregister(string stream, string consumer);
    DeadLetterPolicy? Get(string stream, string consumer);
}

internal class DlqPolicyRegistry : IDlqPolicyRegistry
{
    private readonly ConcurrentDictionary<string, DeadLetterPolicy> _policies = new();
    private readonly ILogger<DlqPolicyRegistry> _logger;

    public DlqPolicyRegistry(ILogger<DlqPolicyRegistry> logger)
    {
        _logger = logger;
    }

    public void Register(string stream, string consumer, DeadLetterPolicy policy)
    {
        var key = $"{stream}/{consumer}";
        _policies.AddOrUpdate(key, policy, (k, old) =>
        {
            _logger.LogWarning("DLQ policy for {Key} already registered, updating...", key);
            return policy;
        });
    }

    public void Unregister(string stream, string consumer)
    {
        var key = $"{stream}/{consumer}";
        _policies.TryRemove(key, out _);
    }

    public DeadLetterPolicy? Get(string stream, string consumer)
    {
        var key = $"{stream}/{consumer}";
        return _policies.TryGetValue(key, out var policy) ? policy : null;
    }
}

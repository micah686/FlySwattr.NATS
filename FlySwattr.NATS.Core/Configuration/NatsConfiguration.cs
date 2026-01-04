using NATS.Client.Core;

namespace FlySwattr.NATS.Core.Configuration;

public class NatsConfiguration
{
    public string Url { get; set; } = "nats://localhost:4222";
    public NatsAuthOpts? NatsAuth { get; set; }
    public NatsTlsOpts? TlsOpts { get; set; }
    public TimeSpan? ReconnectWait { get; set; }
    public int? MaxReconnect { get; set; }

    // Concurrency Tuning
    public int MaxConcurrency { get; set; } = 100;
}

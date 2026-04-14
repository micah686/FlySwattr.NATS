using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("UnitTests")]
[assembly: InternalsVisibleTo("IntegrationTests")]
[assembly: InternalsVisibleTo("Benchmarks")]
[assembly: InternalsVisibleTo("DynamicProxyGenAssembly2")] // For NSubstitute
[assembly: InternalsVisibleTo("FlySwattr.NATS.Caching")]
[assembly: InternalsVisibleTo("FlySwattr.NATS.Hosting")]

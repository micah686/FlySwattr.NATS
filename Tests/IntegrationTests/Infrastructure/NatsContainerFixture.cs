using Testcontainers.Nats;

namespace IntegrationTests.Infrastructure;

public class NatsContainerFixture : IAsyncDisposable
{
    private readonly NatsContainer _container;

    public NatsContainerFixture()
    {
        _container = new NatsBuilder()
            .WithImage("nats:2.10") // Explicit version
            .WithCommand("--jetstream")
            .Build();
    }

    public NatsContainer Container => _container;

    public string ConnectionString => _container.GetConnectionString();

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _container.DisposeAsync();
    }
}

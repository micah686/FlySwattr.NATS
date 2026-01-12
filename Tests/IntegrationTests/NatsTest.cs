using IntegrationTests.Infrastructure;
using NATS.Client.Core;
using TUnit.Core;

namespace IntegrationTests;

[Property("nTag", "Abstractions")]
public class NatsTest
{
    [Test]
    public async Task NatsContainer_ShouldStart_AndAllowConnection()
    {
        // Arrange
        await using var fixture = new NatsContainerFixture();
        await fixture.InitializeAsync();

        // Act
        var opts = new NatsOpts { Url = fixture.ConnectionString };
        await using var connection = new NatsConnection(opts);
        await connection.ConnectAsync();

        // Assert
        if (connection.ConnectionState != NatsConnectionState.Open)
        {
            throw new Exception($"Expected NATS connection to be Open, but was {connection.ConnectionState}");
        }
    }
}

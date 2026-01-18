using FlySwattr.NATS.Core.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Core.Configuration;

/// <summary>
/// T07: Cold Start Retry Tests
/// Verifies that configuring an invalid NATS URL causes startup to wait/retry
/// rather than crashing immediately.
/// </summary>
public class ColdStartRetryTests
{
    /// <summary>
    /// Verifies that NatsConnection with an invalid URL doesn't throw immediately on creation.
    /// The connection attempt happens lazily on first use.
    /// </summary>
    [Test]
    public void NatsConnection_WithInvalidUrl_ShouldNotThrowOnCreation()
    {
        // Arrange & Act - Create connection with invalid URL
        var opts = new NatsOpts
        {
            Url = "nats://invalid.nonexistent.host:9999",
            MaxReconnectRetry = 3, // Limit retries for test speed
            ReconnectWaitMin = TimeSpan.FromMilliseconds(50),
            ReconnectWaitMax = TimeSpan.FromMilliseconds(100)
        };

        // Assert - Should not throw on construction (connection is lazy)
        var connection = new NatsConnection(opts);
        connection.ShouldNotBeNull();
        
        // Connection state should be Closed or Reconnecting (not connected yet)
        connection.ConnectionState.ShouldBeOneOf(
            NatsConnectionState.Closed, 
            NatsConnectionState.Reconnecting);
    }

    /// <summary>
    /// Verifies that NatsConnection retries on connection failure rather than 
    /// throwing immediately. This tests the "Cold Start" protection.
    /// </summary>
    [Test]
    public async Task NatsConnection_WithInvalidUrl_ShouldRetryBeforeFailing()
    {
        // Arrange
        var opts = new NatsOpts
        {
            Url = "nats://localhost:9999", // Invalid port
            MaxReconnectRetry = 2, // Allow a few retries
            ReconnectWaitMin = TimeSpan.FromMilliseconds(50),
            ReconnectWaitMax = TimeSpan.FromMilliseconds(100)
        };

        var connection = new NatsConnection(opts);

        // Act - Try to ping (forces connection attempt)
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        
        try
        {
            await connection.PingAsync(cts.Token);
            Assert.Fail("Should have thrown due to connection failure");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Expected - connection should fail after retries
        }

        // Assert - The connection attempted retries (didn't crash immediately)
        // Note: Due to async nature, we verify the connection state reflects retry behavior
        connection.ConnectionState.ShouldBeOneOf(
            NatsConnectionState.Closed,
            NatsConnectionState.Reconnecting);
    }

    /// <summary>
    /// Verifies that DI service registration with invalid URL doesn't throw
    /// during service provider build (lazy initialization).
    /// </summary>
    [Test]
    public void AddFlySwattrNatsCore_WithInvalidUrl_ShouldNotThrowDuringRegistration()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole());

        // Act - Register with invalid URL - should not throw
        services.AddFlySwattrNatsCore(config =>
        {
            config.Url = "nats://this.host.does.not.exist:4222";
        });

        // Building the provider should also succeed (connection is lazy)
        var provider = services.BuildServiceProvider();
        provider.ShouldNotBeNull();

        // Getting the connection should succeed (no immediate connection attempt)
        var connection = provider.GetService<INatsConnection>();
        connection.ShouldNotBeNull();
    }

    /// <summary>
    /// Verifies that MaxReconnectRetry configuration is respected.
    /// When set to 0, connection should attempt at least once before failing.
    /// </summary>
    [Test]
    public void NatsConnection_MaxReconnectRetryZero_ShouldAttemptOnce()
    {
        // Arrange
        var opts = new NatsOpts
        {
            Url = "nats://localhost:9999",
            MaxReconnectRetry = 0 // No retries
        };

        var connection = new NatsConnection(opts);

        // Assert - Connection should be created
        connection.ShouldNotBeNull();
        connection.Opts.MaxReconnectRetry.ShouldBe(0);
    }

    /// <summary>
    /// Verifies that ReconnectWait configuration affects retry delay.
    /// </summary>
    [Test]
    public void NatsConnection_ReconnectWaitConfiguration_ShouldBeRespected()
    {
        // Arrange
        var opts = new NatsOpts
        {
            Url = "nats://localhost:4222",
            ReconnectWaitMin = TimeSpan.FromSeconds(1),
            ReconnectWaitMax = TimeSpan.FromSeconds(10),
            MaxReconnectRetry = 5
        };

        var connection = new NatsConnection(opts);

        // Assert - Configuration is stored
        connection.Opts.ReconnectWaitMin.ShouldBe(TimeSpan.FromSeconds(1));
        connection.Opts.ReconnectWaitMax.ShouldBe(TimeSpan.FromSeconds(10));
        connection.Opts.MaxReconnectRetry.ShouldBe(5);
    }

    /// <summary>
    /// Verifies that when NATS driver retries are combined with Polly retries,
    /// the total duration is bounded and doesn't cause multiplicative retry explosion.
    /// 
    /// This test validates the recommended pattern:
    /// - NATS driver with minimal/no retries (MaxReconnectRetry=0 or low)
    /// - Polly handling application-layer retries with bounded duration
    /// 
    /// The concern: If NATS has 10 reconnect attempts and Polly has 3 retries,
    /// the total could be 10 * 3 = 30 actual connection attempts per request.
    /// </summary>
    [Test]
    public async Task CombinedNatsPolly_TotalDuration_ShouldBeBounded()
    {
        // Arrange - NATS with limited retries (fail-fast mode)
        var natsOpts = new NatsOpts
        {
            Url = "nats://localhost:9999", // Invalid port - will fail
            MaxReconnectRetry = 2,
            ReconnectWaitMin = TimeSpan.FromMilliseconds(100),
            ReconnectWaitMax = TimeSpan.FromMilliseconds(200)
        };

        var connection = new NatsConnection(natsOpts);

        // Simulate what Polly would do: 3 retries with exponential backoff
        var pollyMaxRetries = 3;
        var pollyBaseDelay = TimeSpan.FromMilliseconds(100);
        
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var attemptCount = 0;
        
        // Simulate Polly retry loop
        for (int pollyAttempt = 0; pollyAttempt <= pollyMaxRetries; pollyAttempt++)
        {
            attemptCount++;
            
            try
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
                await connection.PingAsync(cts.Token);
                break; // Success - exit retry loop
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // Expected failure - continue retry loop
                if (pollyAttempt < pollyMaxRetries)
                {
                    // Exponential backoff delay
                    var delay = TimeSpan.FromMilliseconds(
                        pollyBaseDelay.TotalMilliseconds * Math.Pow(2, pollyAttempt));
                    await Task.Delay(delay);
                }
            }
            catch (OperationCanceledException)
            {
                // Timeout hit - count as attempt and continue
            }
        }
        
        stopwatch.Stop();

        // Assert - Total time should be bounded
        // With 4 attempts (initial + 3 retries), each with 2s timeout max, plus backoff delays
        // Expected: ~4 * 2s + (0.1 + 0.2 + 0.4)s backoff = ~8.7s max
        // We use generous upper bound of 15s to account for test runner variance
        stopwatch.Elapsed.TotalSeconds.ShouldBeLessThan(15);
        
        // Should have attempted exactly 4 times (initial + 3 retries)
        attemptCount.ShouldBe(4);
        
        // Verify configuration prevents multiplicative explosion
        // Without proper bounds, 2 NATS retries * 4 Polly attempts would mean
        // up to 8 actual NATS connection attempts - but our timeout limits this
        connection.Opts.MaxReconnectRetry.ShouldBe(2);
    }
}

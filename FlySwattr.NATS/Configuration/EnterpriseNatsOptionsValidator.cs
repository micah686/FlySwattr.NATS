using System;
using System.Collections.Generic;
using FluentValidation;
using FlySwattr.NATS.Core.Configuration;
using Microsoft.Extensions.Options;

namespace FlySwattr.NATS.Configuration;

/// <summary>
/// Validates EnterpriseNatsOptions configuration at registration time.
/// Ensures all required timeouts, buckets, and sub-configs are valid before services start.
/// </summary>
internal static class EnterpriseNatsOptionsValidator
{
    public static void Validate(EnterpriseNatsOptions options)
    {
        var failures = new List<string>();

        // Validate Core configuration
        if (options.Core == null)
        {
            failures.Add($"{nameof(options.Core)} cannot be null.");
        }
        else if (string.IsNullOrWhiteSpace(options.Core.Url))
        {
            failures.Add($"{nameof(options.Core)}.{nameof(options.Core.Url)} cannot be null or empty.");
        }

        // Validate EagerConnect timeout
        if (options.EagerConnect && options.ConnectionWarmupTimeout <= TimeSpan.Zero)
        {
            failures.Add($"{nameof(options.ConnectionWarmupTimeout)} must be greater than zero when {nameof(options.EagerConnect)} is true.");
        }

        // Validate PayloadOffloading settings
        if (options.EnablePayloadOffloading)
        {
            if (string.IsNullOrWhiteSpace(options.ClaimCheckBucket))
            {
                failures.Add($"{nameof(options.ClaimCheckBucket)} cannot be null or empty when {nameof(options.EnablePayloadOffloading)} is true.");
            }

            if (options.ClaimCheckTtl <= TimeSpan.Zero)
            {
                failures.Add($"{nameof(options.ClaimCheckTtl)} must be greater than zero when {nameof(options.EnablePayloadOffloading)} is true.");
            }
        }

        // Validate ClaimCheckCleanup settings
        if (options.EnableClaimCheckCleanup && options.ClaimCheckSweepInterval <= TimeSpan.Zero)
        {
            failures.Add($"{nameof(options.ClaimCheckSweepInterval)} must be greater than zero when {nameof(options.EnableClaimCheckCleanup)} is true.");
        }

        if (failures.Count > 0)
        {
            throw new OptionsValidationException(
                nameof(EnterpriseNatsOptions),
                typeof(EnterpriseNatsOptions),
                failures);
        }
    }
}

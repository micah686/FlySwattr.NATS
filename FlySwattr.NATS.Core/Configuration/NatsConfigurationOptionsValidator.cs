using Microsoft.Extensions.Options;

namespace FlySwattr.NATS.Core.Configuration;

internal sealed class NatsConfigurationOptionsValidator : IValidateOptions<NatsConfiguration>
{
    private readonly NatsConfigurationValidator _validator = new();

    public ValidateOptionsResult Validate(string? name, NatsConfiguration options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var result = _validator.Validate(options);
        if (result.IsValid)
        {
            return ValidateOptionsResult.Success;
        }

        return ValidateOptionsResult.Fail(result.Errors.Select(error => error.ErrorMessage));
    }
}

using FluentValidation;
using FlySwattr.NATS.Abstractions;
using FlySwattr.NATS.Abstractions.Exceptions;

namespace FlySwattr.NATS.Hosting.Middleware;

/// <summary>
/// Middleware that validates incoming messages using FluentValidation validators.
/// If validation fails, throws <see cref="MessageValidationException"/> which is
/// handled specially by the consumer pipeline (routed to DLQ without retries).
/// </summary>
/// <typeparam name="T">The type of message being validated.</typeparam>
public class ValidationMiddleware<T> : IConsumerMiddleware<T>
{
    private readonly IEnumerable<IValidator<T>> _validators;

    public ValidationMiddleware(IEnumerable<IValidator<T>> validators)
    {
        _validators = validators;
    }

    public async Task InvokeAsync(IJsMessageContext<T> context, Func<Task> next, CancellationToken ct)
    {
        if (_validators.Any())
        {
            var validationContext = new ValidationContext<T>(context.Message);
            var validationResults = await Task.WhenAll(
                _validators.Select(v => v.ValidateAsync(validationContext, ct)));

            var failures = validationResults
                .SelectMany(r => r.Errors)
                .Where(f => f != null)
                .ToList();

            if (failures.Count > 0)
            {
                var errorMessages = failures.Select(f => $"{f.PropertyName}: {f.ErrorMessage}");
                throw new MessageValidationException(context.Subject, errorMessages);
            }
        }

        await next();
    }
}

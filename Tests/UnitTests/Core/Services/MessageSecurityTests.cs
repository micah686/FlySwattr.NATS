using FlySwattr.NATS.Abstractions.Exceptions;
using FlySwattr.NATS.Core.Services;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Core.Services;

[Property("nTag", "Core")]
public class MessageSecurityTests
{
    [Test]
    public void SanitizeExceptionMessage_WhenValidationException_ShouldRemoveSensitiveValues()
    {
        var sensitiveValue = "4111-1111-1111-1111";
        var exception = new MessageValidationException(
            "orders.created",
            [$"CardNumber: value '{sensitiveValue}' is invalid", "Email: invalid format"]);

        var sanitized = MessageSecurity.SanitizeExceptionMessage(exception);

        sanitized.ShouldContain("MessageValidationException");
        sanitized.ShouldContain("Fields=CardNumber, Email");
        sanitized.ShouldNotContain(sensitiveValue);
    }

    [Test]
    public void SanitizeExceptionMessage_WhenPrivacySanitizationDisabled_ShouldKeepOriginalMessage()
    {
        var sensitiveValue = "user@example.com";
        var exception = new MessageValidationException(
            "orders.created",
            [$"Email: '{sensitiveValue}' is invalid"]);

        var unsanitized = MessageSecurity.SanitizeExceptionMessage(exception, enablePrivacySanitization: false);

        unsanitized.ShouldContain(sensitiveValue);
        unsanitized.ShouldContain("Validation failed for message on subject 'orders.created'");
    }

    [Test]
    public void SanitizeExceptionMessage_ShouldNotRedactIntentionalTokenLikeValues()
    {
        var tokenValue = "token_user_set_123";
        var exception = new InvalidOperationException($"KV key lookup failed for value '{tokenValue}'");

        var sanitized = MessageSecurity.SanitizeExceptionMessage(exception);

        sanitized.ShouldContain(tokenValue);
    }
}

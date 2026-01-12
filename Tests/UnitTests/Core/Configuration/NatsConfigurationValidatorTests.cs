using FlySwattr.NATS.Core.Configuration;
using Shouldly;
using NATS.Client.Core;
using TUnit.Core;

namespace UnitTests.Core.Configuration;

[Property("nTag", "Core")]
public class NatsConfigurationValidatorTests
{
    private readonly NatsConfigurationValidator _validator = new();

    [Test]
    public void Validate_ShouldPass_WhenUrlIsValid()
    {
        var config = new NatsConfiguration { Url = "nats://localhost:4222" };
        var result = _validator.Validate(config);
        result.IsValid.ShouldBeTrue();
    }

    [Test]
    public void Validate_ShouldPass_WhenMultipleUrlsAreValid()
    {
        var config = new NatsConfiguration { Url = "nats://node1:4222,nats://node2:4222" };
        var result = _validator.Validate(config);
        result.IsValid.ShouldBeTrue();
    }

    [Test]
    public void Validate_ShouldFail_WhenUrlIsEmpty()
    {
        var config = new NatsConfiguration { Url = "" };
        var result = _validator.Validate(config);
        result.IsValid.ShouldBeFalse();
        result.Errors.ShouldContain(e => e.PropertyName == nameof(NatsConfiguration.Url));
    }

    [Test]
    public void Validate_ShouldFail_WhenUrlSchemeIsInvalid()
    {
        var config = new NatsConfiguration { Url = "http://localhost:4222" };
        var result = _validator.Validate(config);
        result.IsValid.ShouldBeFalse();
        result.Errors.ShouldContain(x => x.ErrorMessage.Contains("Check URL format"));
    }

    [Test]
    public void Validate_ShouldFail_WhenOneOfMultipleUrlsIsInvalid()
    {
        var config = new NatsConfiguration { Url = "nats://valid:4222,http://invalid:4222" };
        var result = _validator.Validate(config);
        result.IsValid.ShouldBeFalse();
    }

    [Test]
    public void Validate_ShouldPass_WhenTlsSchemeIsUsed()
    {
        var config = new NatsConfiguration { Url = "tls://localhost:4222" };
        var result = _validator.Validate(config);
        result.IsValid.ShouldBeTrue();
    }

    [Test]
    public void Validate_ShouldFail_WhenMaxConcurrencyIsZero()
    {
        var config = new NatsConfiguration { MaxConcurrency = 0 };
        var result = _validator.Validate(config);
        result.IsValid.ShouldBeFalse();
        result.Errors.ShouldContain(e => e.PropertyName == nameof(NatsConfiguration.MaxConcurrency));
    }

    [Test]
    public void Validate_ShouldFail_WhenMaxConcurrencyIsNegative()
    {
        var config = new NatsConfiguration { MaxConcurrency = -1 };
        var result = _validator.Validate(config);
        result.IsValid.ShouldBeFalse();
    }

    [Test]
    public void Validate_ShouldPass_WhenMaxConcurrencyIsPositive()
    {
        var config = new NatsConfiguration { MaxConcurrency = 10 };
        var result = _validator.Validate(config);
        result.IsValid.ShouldBeTrue();
    }

    [Test]
    public void Validate_ShouldPass_WithAuthOpts()
    {
        var config = new NatsConfiguration 
        { 
            Url = "nats://localhost:4222",
            NatsAuth = new NatsAuthOpts { Username = "user", Password = "pass" }
        };
        var result = _validator.Validate(config);
        result.IsValid.ShouldBeTrue();
    }

    [Test]
    public void Validate_ShouldFail_WhenTlsConfiguredButCaFileMissing()
    {
        var config = new NatsConfiguration 
        { 
            Url = "tls://localhost:4222",
            TlsOpts = new NatsTlsOpts { CaFile = "nonexistent.pem" }
        };
        var result = _validator.Validate(config);
        result.IsValid.ShouldBeFalse();
        result.Errors.ShouldContain(e => e.ErrorMessage.Contains("CA File not found"));
    }

    [Test]
    public void Validate_ShouldFail_WhenTlsConfiguredButCertFileMissing()
    {
        var config = new NatsConfiguration 
        { 
            Url = "tls://localhost:4222",
            TlsOpts = new NatsTlsOpts { CertFile = "nonexistent.pem" }
        };
        var result = _validator.Validate(config);
        result.IsValid.ShouldBeFalse();
    }

    [Test]
    public void Validate_ShouldFail_WhenTlsConfiguredButKeyFileMissing()
    {
        var config = new NatsConfiguration 
        { 
            Url = "tls://localhost:4222",
            TlsOpts = new NatsTlsOpts { KeyFile = "nonexistent.pem" }
        };
        var result = _validator.Validate(config);
        result.IsValid.ShouldBeFalse();
    }

    [Test]
    public void Validate_ShouldPass_WhenUrlHasWhitespace()
    {
        var config = new NatsConfiguration { Url = " nats://localhost:4222 , nats://other:4222 " };
        var result = _validator.Validate(config);
        result.IsValid.ShouldBeTrue();
    }

    [Test]
    public void Validate_ShouldFail_WhenUrlIsJustWhitespace()
    {
        var config = new NatsConfiguration { Url = "   " };
        var result = _validator.Validate(config);
        result.IsValid.ShouldBeFalse();
    }

    [Test]
    public void Validate_ShouldPass_ForWebsocketSchemes()
    {
        var config = new NatsConfiguration { Url = "ws://localhost:8080,wss://secure:8443" };
        var result = _validator.Validate(config);
        result.IsValid.ShouldBeTrue();
    }

    [Test]
    public void Validate_ShouldFail_ForUnknownSchemes()
    {
        var config = new NatsConfiguration { Url = "tcp://localhost:4222" };
        var result = _validator.Validate(config);
        result.IsValid.ShouldBeFalse();
    }

    [Test]
    public void Validate_ShouldFail_ForMalformedUrl()
    {
        var config = new NatsConfiguration { Url = "not_a_url" };
        var result = _validator.Validate(config);
        result.IsValid.ShouldBeFalse();
    }

    [Test]
    public void Validate_ShouldPass_WithTokenAuth()
    {
        var config = new NatsConfiguration 
        { 
            Url = "nats://localhost:4222",
            NatsAuth = new NatsAuthOpts { Token = "secret" }
        };
        var result = _validator.Validate(config);
        result.IsValid.ShouldBeTrue();
    }

    [Test]
    public void Validate_ShouldPass_WithUserPassAuth()
    {
        var config = new NatsConfiguration 
        { 
            Url = "nats://localhost:4222",
            NatsAuth = new NatsAuthOpts { Username = "u", Password = "p" }
        };
        var result = _validator.Validate(config);
        result.IsValid.ShouldBeTrue();
    }

    [Test]
    public void Validate_ShouldPass_WithCredsFile()
    {
        var config = new NatsConfiguration 
        { 
            Url = "nats://localhost:4222",
            NatsAuth = new NatsAuthOpts { CredsFile = "user.creds" }
        };
        var result = _validator.Validate(config);
        result.IsValid.ShouldBeTrue();
    }

    [Test]
    public void Validate_ShouldPass_WhenMaxConcurrencyIsDefault()
    {
        var config = new NatsConfiguration(); // Default is 100
        var result = _validator.Validate(config);
        result.IsValid.ShouldBeTrue();
    }

    [Test]
    public void Validate_ShouldPass_WhenTlsOptsAreNull()
    {
        var config = new NatsConfiguration { TlsOpts = null };
        var result = _validator.Validate(config);
        result.IsValid.ShouldBeTrue();
    }

    [Test]
    public void Validate_ShouldPass_WhenTlsOptsAreEmpty()
    {
        var config = new NatsConfiguration { TlsOpts = new NatsTlsOpts() };
        var result = _validator.Validate(config);
        result.IsValid.ShouldBeTrue();
    }
}

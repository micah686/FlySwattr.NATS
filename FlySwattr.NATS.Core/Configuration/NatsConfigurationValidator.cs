using FluentValidation;

namespace FlySwattr.NATS.Core.Configuration;

internal class NatsConfigurationValidator : AbstractValidator<NatsConfiguration>
{
    public NatsConfigurationValidator()
    {
        RuleFor(x => x.Url)
            .NotEmpty()
            .Must(url =>
            {
                if (string.IsNullOrWhiteSpace(url)) return false;
                var urls = url.Split(',');
                foreach (var u in urls)
                {
                    if (!Uri.TryCreate(u.Trim(), UriKind.Absolute, out var uri)) return false;
                    if (uri.Scheme != "nats" && uri.Scheme != "tls" && uri.Scheme != "ws" && uri.Scheme != "wss") return false;
                }
                return true;
            })
            .WithMessage("NATS Configuration validation failed. Check URL format.");

        RuleFor(x => x.Url)
            .Must(url =>
            {
                if (string.IsNullOrWhiteSpace(url)) return true; // other rule reports invalid URLs
                foreach (var u in url.Split(','))
                {
                    if (!Uri.TryCreate(u.Trim(), UriKind.Absolute, out var uri)) return true;
                    if (uri.Scheme != "tls" && uri.Scheme != "wss") return false;
                }
                return true;
            })
            .When(x => !x.AllowInsecureTransport)
            .WithMessage("NATS Configuration rejects plaintext transport: all URLs must use 'tls' or 'wss' when AllowInsecureTransport is false.");

        When(x => x.TlsOpts != null, () =>
        {
            RuleFor(x => x.TlsOpts!.CaFile)
                .Must(File.Exists)
                .When(x => !string.IsNullOrEmpty(x.TlsOpts!.CaFile))
                .WithMessage(x => $"CA File not found: {x.TlsOpts!.CaFile}");

            RuleFor(x => x.TlsOpts!.CertFile)
                .Must(File.Exists)
                .When(x => !string.IsNullOrEmpty(x.TlsOpts!.CertFile))
                .WithMessage(x => $"Cert File not found: {x.TlsOpts!.CertFile}");

            RuleFor(x => x.TlsOpts!.KeyFile)
                .Must(File.Exists)
                .When(x => !string.IsNullOrEmpty(x.TlsOpts!.KeyFile))
                .WithMessage(x => $"Key File not found: {x.TlsOpts!.KeyFile}");
        });

        RuleFor(x => x.MaxConcurrency)
            .GreaterThan(0)
            .WithMessage("MaxConcurrency must be greater than 0");
    }
}

using System.Text.RegularExpressions;
using Vogen;
// ReSharper disable once IdentifierTypo
// ReSharper disable CheckNamespace
namespace FlySwattr.NATS.Abstractions;

[ValueObject<string>]
public partial struct StreamName
{
    [GeneratedRegex(@"^[a-zA-Z0-9_-]+$")]
    private static partial Regex ValidPattern();
    
    private static Validation Validate(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return Validation.Invalid("Stream name cannot be empty");
        if (value.Length > 256)
            return Validation.Invalid("Stream name cannot exceed 256 characters");
        if (!ValidPattern().IsMatch(value))
            return Validation.Invalid("Stream name can only contain letters, numbers, underscores, and hyphens");
        return Validation.Ok;
    }
}

[ValueObject<string>]
public partial struct ConsumerName
{
    [GeneratedRegex(@"^[a-zA-Z0-9_-]+$")]
    private static partial Regex ValidPattern();
    
    private static Validation Validate(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return Validation.Invalid("Consumer name cannot be empty");
        if (value.Length > 256)
            return Validation.Invalid("Consumer name cannot exceed 256 characters");
        if (!ValidPattern().IsMatch(value))
            return Validation.Invalid("Consumer name can only contain letters, numbers, underscores, and hyphens");
        return Validation.Ok;
    }
}

[ValueObject<string>]
public partial struct SubjectName
{
    // NATS subjects allow alphanumeric, dots (hierarchy), and wildcards (* for single token, > for multi)
    // Wildcards should only appear in subscription patterns, not in publish subjects
    [GeneratedRegex(@"^[a-zA-Z0-9_\-.*>]+$")]
    private static partial Regex ValidPattern();
    
    private static Validation Validate(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return Validation.Invalid("Subject name cannot be empty");
        if (value.Length > 1024)
            return Validation.Invalid("Subject name cannot exceed 1024 characters");

        var tokens = value.Split('.');
        foreach (var token in tokens)
        {
             if (string.IsNullOrEmpty(token))
                 return Validation.Invalid("Subject cannot have empty tokens");

             if (token.Contains('*') && token != "*")
                 return Validation.Invalid("Single-level wildcard (*) must be a complete token");

             if (token.Contains('>') && token != ">")
                 return Validation.Invalid("Multi-level wildcard (>) must be a complete token");
        }
    
        // Allow more flexible patterns while maintaining safety
        if (value.Contains("..") || value.Contains("**") || value.Contains(">>"))
            return Validation.Invalid("Subject contains invalid pattern sequences");

        if (!ValidPattern().IsMatch(value))
            return Validation.Invalid("Subject name contains invalid characters");
        // Check for invalid wildcard positions (> must be at end if present)
        if (value.Contains('>') && !value.EndsWith('>'))
            return Validation.Invalid("Multi-level wildcard (>) must be at the end of subject");

        return Validation.Ok;
    }
}

[ValueObject<string>]
public partial struct BucketName
{
    // KV/Object store bucket names: alphanumeric, underscores, hyphens only
    [GeneratedRegex(@"^[a-zA-Z0-9_-]+$")]
    private static partial Regex ValidPattern();
    
    private static Validation Validate(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return Validation.Invalid("Bucket name cannot be empty");
        if (value.Length > 256)
            return Validation.Invalid("Bucket name cannot exceed 256 characters");
        if (!ValidPattern().IsMatch(value))
            return Validation.Invalid("Bucket name can only contain letters, numbers, underscores, and hyphens");
        return Validation.Ok;
    }
}

[ValueObject<string>]
public partial struct QueueGroup
{
    // Queue groups: no spaces, dots, or wildcards
    [GeneratedRegex(@"^[a-zA-Z0-9_-]+$")]
    private static partial Regex ValidPattern();
    
    private static Validation Validate(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return Validation.Invalid("Queue group cannot be empty");
        if (value.Length > 256)
            return Validation.Invalid("Queue group cannot exceed 256 characters");
        if (!ValidPattern().IsMatch(value))
            return Validation.Invalid("Queue group can only contain letters, numbers, underscores, and hyphens");
        return Validation.Ok;
    }
}
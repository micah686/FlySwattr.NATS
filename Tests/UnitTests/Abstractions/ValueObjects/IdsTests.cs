using FlySwattr.NATS.Abstractions;
using Vogen;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Abstractions.ValueObjects;

[Property("nTag", "Abstractions")]
public class IdsTests
{
    [Test]
    public void StreamName_ShouldCreate_WhenValid()
    {
        var result = StreamName.From("valid_name");
        result.Value.ShouldBe("valid_name");
    }


    [Test]
    public void StreamName_ShouldThrow_WhenEmpty()
    {
        Assert.Throws<ValueObjectValidationException>(() => StreamName.From(""));
    }

    [Test]
    public void StreamName_ShouldThrow_WhenInvalidCharacters()
    {
        Assert.Throws<ValueObjectValidationException>(() => StreamName.From("invalid.name"));
    }

    [Test]
    public void ConsumerName_ShouldCreate_WhenValid()
    {
        var result = ConsumerName.From("valid-consumer");
        result.Value.ShouldBe("valid-consumer");
    }

    [Test]
    public void ConsumerName_ShouldThrow_WhenTooLong()
    {
        var longName = new string('a', 257);
        Assert.Throws<ValueObjectValidationException>(() => ConsumerName.From(longName));
    }

    [Test]
    public void SubjectName_ShouldCreate_WhenValid()
    {
        var result = SubjectName.From("valid.subject.foo");
        result.Value.ShouldBe("valid.subject.foo");
    }

    [Test]
    public void SubjectName_ShouldAllowWildcards()
    {
        SubjectName.From("valid.*.foo").Value.ShouldBe("valid.*.foo");
        SubjectName.From("valid.>").Value.ShouldBe("valid.>");
    }

    [Test]
    public void SubjectName_ShouldThrow_WhenWildcardInvalid()
    {
        Assert.Throws<ValueObjectValidationException>(() => SubjectName.From("valid.foo>")); // > must be standalone token
    }
    
    [Test]
    public void SubjectName_ShouldThrow_WhenMultiWildcardNotAtEnd()
    {
        Assert.Throws<ValueObjectValidationException>(() => SubjectName.From("valid.>.foo"));
    }

    [Test]
    public void SubjectName_ShouldThrow_WhenEmptyTokens()
    {
        Assert.Throws<ValueObjectValidationException>(() => SubjectName.From("valid..foo"));
    }

    [Test]
    public void BucketName_ShouldCreate_WhenValid()
    {
        var result = BucketName.From("valid_bucket");
        result.Value.ShouldBe("valid_bucket");
    }

    [Test]
    public void BucketName_ShouldThrow_WhenInvalidChars()
    {
        Assert.Throws<ValueObjectValidationException>(() => BucketName.From("invalid/bucket"));
    }

    [Test]
    public void QueueGroup_ShouldCreate_WhenValid()
    {
        var result = QueueGroup.From("workers");
        result.Value.ShouldBe("workers");
    }

    [Test]
    public void QueueGroup_ShouldThrow_WhenHasDots()
    {
        Assert.Throws<ValueObjectValidationException>(() => QueueGroup.From("workers.group"));
    }
}

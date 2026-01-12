using FlySwattr.NATS.Abstractions;
using Shouldly;
using TUnit.Core;

namespace UnitTests.Abstractions.Specs;

[Property("nTag", "Abstractions")]
public class ConsumerSpecTests
{
    [Test]
    public void GetFilterSubjects_ShouldReturnEmpty_WhenNoFiltersSet()
    {
        var spec = new ConsumerSpec();
        spec.GetFilterSubjects().ShouldBeEmpty();
    }

    [Test]
    public void GetFilterSubjects_ShouldReturnFilterSubject_WhenSingleSet()
    {
        var spec = new ConsumerSpec { FilterSubject = "test.subject" };
        var subjects = spec.GetFilterSubjects();
        
        subjects.Count.ShouldBe(1);
        subjects[0].ShouldBe("test.subject");
    }

    [Test]
    public void GetFilterSubjects_ShouldReturnFilterSubjects_WhenListSet()
    {
        var list = new List<string> { "a", "b" };
        var spec = new ConsumerSpec { FilterSubjects = list };
        
        var subjects = spec.GetFilterSubjects();
        
        subjects.Count.ShouldBe(2);
        subjects.ShouldContain("a");
        subjects.ShouldContain("b");
    }

    [Test]
    public void GetFilterSubjects_ShouldPrioritizeList_WhenBothSet()
    {
        var list = new List<string> { "a", "b" };
        var spec = new ConsumerSpec 
        { 
            FilterSubject = "ignored", 
            FilterSubjects = list 
        };
        
        var subjects = spec.GetFilterSubjects();
        
        subjects.Count.ShouldBe(2);
        subjects.ShouldContain("a");
        subjects.ShouldContain("b");
        subjects.ShouldNotContain("ignored");
    }
}

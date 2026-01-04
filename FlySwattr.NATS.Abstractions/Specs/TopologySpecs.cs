// ReSharper disable CheckNamespace
// ReSharper disable once IdentifierTypo
namespace FlySwattr.NATS.Abstractions;

public class StreamSpec
{
    public StreamName Name { get; set; } = StreamName.From("default");
    public string[] Subjects { get; set; } = Array.Empty<string>();
    public long MaxBytes { get; set; } = -1;
    public long MaxMsgSize { get; set; } = -1;
    public TimeSpan MaxAge { get; set; } = TimeSpan.Zero; // 0 = unlimited
    public int Replicas { get; set; } = 1;
    public StorageType StorageType { get; set; } = StorageType.File;
    public StreamRetention RetentionPolicy { get; set; } = StreamRetention.Limits;
}

public class ConsumerSpec
{
    public StreamName StreamName { get; set; } = StreamName.From("default");
    public ConsumerName DurableName { get; set; } = ConsumerName.From("default");
    public string? Description { get; set; }

    private string? _filterSubject;
    private List<string> _filterSubjects = new();

    public string? FilterSubject
    {
        get => _filterSubject;
        set => _filterSubject = value;
    }

    public List<string> FilterSubjects
    {
        get => _filterSubjects;
        set => _filterSubjects = value;
    }

    public IReadOnlyList<string> GetFilterSubjects()
    {
        if (_filterSubjects.Count > 0)
            return _filterSubjects;
        if (!string.IsNullOrEmpty(_filterSubject))
            return new[] { _filterSubject };
        return Array.Empty<string>();
    }

    public DeliverPolicy DeliverPolicy { get; set; } = DeliverPolicy.All;
    public AckPolicy AckPolicy { get; set; } = AckPolicy.Explicit;
    public TimeSpan AckWait { get; set; } = TimeSpan.FromSeconds(30);
    public int MaxDeliver { get; set; } = -1;
    public int? DegreeOfParallelism { get; set; }
    public TimeSpan[]? Backoff { get; set; }
    public DeadLetterPolicy? DeadLetterPolicy { get; set; }
}

public class DeadLetterPolicy
{
    public required string SourceStream { get; init; }
    public required string SourceConsumer { get; init; }
    public required StreamName TargetStream { get; init; }
    public required string TargetSubject { get; init; }
}

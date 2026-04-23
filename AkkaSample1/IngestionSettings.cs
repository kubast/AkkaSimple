namespace AkkaSample1;

public sealed class IngestionSettings
{
    public int WorkerPoolSize { get; init; } = 4;
    public int MaxInFlightRecords { get; init; } = 32;
    public int StorageBatchSize { get; init; } = 50;
    public TimeSpan StorageFlushInterval { get; init; } = TimeSpan.FromSeconds(2);
    public TimeSpan WorkerAskTimeout { get; init; } = TimeSpan.FromSeconds(5);
}


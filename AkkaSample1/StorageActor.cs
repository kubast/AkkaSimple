using Akka.Actor;

namespace AkkaSample1;

public sealed class StorageActor : ReceiveActor
{
    private readonly int _batchSize;
    private readonly TimeSpan _flushInterval;
    private readonly List<ValidRecord> _buffer = [];
    private ICancelable? _flushSchedule;
    private long _storedRecords;
    private int _writtenBatches;

    public StorageActor(int batchSize, TimeSpan flushInterval)
    {
        _batchSize = batchSize;
        _flushInterval = flushInterval;

        ReceiveAsync<StoreValidRecord>(HandleStoreValidRecordAsync);
        ReceiveAsync<FlushTick>(_ => FlushAsync());
        ReceiveAsync<CompleteStorage>(HandleCompleteStorageAsync);
    }

    protected override void PreStart()
    {
        _flushSchedule = Context.System.Scheduler.ScheduleTellRepeatedlyCancelable(
            _flushInterval,
            _flushInterval,
            Self,
            new FlushTick(),
            Self);
    }

    protected override void PostStop()
    {
        _flushSchedule?.Cancel();
    }

    private async Task HandleStoreValidRecordAsync(StoreValidRecord message)
    {
        _buffer.Add(message.Record);
        if (_buffer.Count >= _batchSize)
        {
            await FlushAsync();
        }
    }

    private async Task HandleCompleteStorageAsync(CompleteStorage message)
    {
        await FlushAsync();
        message.ReplyTo.Tell(new StorageCompleted(_storedRecords, _writtenBatches));
    }

    private async Task FlushAsync()
    {
        if (_buffer.Count == 0)
        {
            return;
        }

        var currentBatch = _buffer.Count;
        await Task.Delay(20);

        _storedRecords += currentBatch;
        _writtenBatches++;
        _buffer.Clear();
    }
}


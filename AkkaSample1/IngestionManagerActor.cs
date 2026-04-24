using Akka.Actor;
using Akka.Event;
using Akka.Routing;

namespace AkkaSample1;

public sealed class IngestionManagerActor : ReceiveActor
{
    private readonly IngestionSettings _settings;
    private readonly ILoggingAdapter _log = Context.GetLogger();
    private readonly IActorRef _storageActor;
    private readonly IActorRef _fileParserActor;

    private IActorRef? _requester;
    private DateTimeOffset _startedAt;
    private string _filePath = string.Empty;
    private long _totalRecords;
    private long _validRecords;
    private long _invalidRecords;
    private bool _parserFinished;
    private bool _storageFinished;
    private long _storedRecords;
    private int _writtenBatches;

    public IngestionManagerActor(IngestionSettings settings)
    {
        _settings = settings;

        var workerSupervisor = new OneForOneStrategy(
            maxNrOfRetries: 10,
            withinTimeRange: TimeSpan.FromMinutes(1),
            localOnlyDecider: ex => ex switch
            {
                InvalidOperationException => Directive.Restart,
                _ => Directive.Restart
            });

        var routerProps = Props.Create(() => new RecordWorkerActor(_settings))
            .WithRouter(new RoundRobinPool(_settings.WorkerPoolSize, null, workerSupervisor, null, false));

        var recordProcessorRouter = Context.ActorOf(routerProps, "record-processor-router");
        _storageActor = Context.ActorOf(Props.Create(() => new StorageActor(_settings.StorageBatchSize, _settings.StorageFlushInterval)), "storage");
        _fileParserActor = Context.ActorOf(Props.Create(() => new FileParserActor(recordProcessorRouter, _settings)), "file-parser");

        Receive<StartIngestion>(HandleStartIngestion);
        Receive<ValidRecord>(HandleValidRecord);
        Receive<InvalidRecord>(HandleInvalidRecord);
        Receive<ParserCompleted>(HandleParserCompleted);
        Receive<ParserFailed>(HandleParserFailed);
        Receive<StorageCompleted>(HandleStorageCompleted);
    }

    private void HandleStartIngestion(StartIngestion message)
    {
        _requester = Sender;
        _startedAt = DateTimeOffset.UtcNow;
        _filePath = message.FilePath;

        _log.Info("Starting ingestion for file {0}", message.FilePath);
        _fileParserActor.Tell(new BeginFileParsing(message.FilePath, Self));
    }

    private void HandleValidRecord(ValidRecord message)
    {
        _validRecords++;
        _storageActor.Tell(new StoreValidRecord(message));
    }

    private void HandleInvalidRecord(InvalidRecord message)
    {
        _invalidRecords++;
        _log.Warning("Invalid record at line {0}: {1}", message.LineNumber, message.Reason);
    }

    private void HandleParserCompleted(ParserCompleted message)
    {
        _totalRecords = message.TotalRecords;
        _parserFinished = true;
        _storageActor.Tell(new CompleteStorage(Self));
        TryCompleteIngestion();
    }

    private void HandleParserFailed(ParserFailed message)
    {
        _requester?.Tell(new Status.Failure(new InvalidOperationException($"Parser failed: {message.Reason}")));
        Context.Stop(Self);
    }

    private void HandleStorageCompleted(StorageCompleted message)
    {
        _storedRecords = message.TotalStoredRecords;
        _writtenBatches = message.TotalBatches;
        _storageFinished = true;
        TryCompleteIngestion();
    }

    private void TryCompleteIngestion()
    {
        if (!_parserFinished || !_storageFinished || _requester is null)
        {
            return;
        }

        var duration = DateTimeOffset.UtcNow - _startedAt;
        _requester.Tell(new IngestionSummary(
            _filePath,
            _totalRecords,
            _validRecords,
            _invalidRecords,
            _storedRecords,
            _writtenBatches,
            duration));
    }
}


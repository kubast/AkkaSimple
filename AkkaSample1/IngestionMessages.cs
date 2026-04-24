using Akka.Actor;

namespace AkkaSample1;

public sealed record StartIngestion(string FilePath);
public sealed record BeginFileParsing(string FilePath, IActorRef Manager);
public sealed record ProcessRecord(long LineNumber, string[] Fields, string RawLine);
public sealed record StoreValidRecord(ValidRecord Record);
public sealed record CompleteStorage(IActorRef ReplyTo);
public sealed record ParserCompleted(long TotalRecords);
public sealed record ParserFailed(string Reason);
public sealed record StorageCompleted(long TotalStoredRecords, int TotalBatches);
public sealed record FlushTick;

public interface IRecordProcessingResult
{
    long LineNumber { get; }
}

public sealed record ValidRecord(
    long LineNumber,
    string Id,
    DateTime EventDate,
    string Payload,
    string Checksum) : IRecordProcessingResult;

public sealed record InvalidRecord(
    long LineNumber,
    string RawLine,
    string Reason) : IRecordProcessingResult;

public sealed record IngestionSummary(
    string FilePath,
    long TotalRecords,
    long ValidRecords,
    long InvalidRecords,
    long StoredRecords,
    int WrittenBatches,
    TimeSpan Duration);


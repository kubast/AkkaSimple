using System.Text;
using Akka.Actor;
using Akka.IO;
using Akka.Streams;
using Akka.Streams.Dsl;

namespace AkkaSample1;

public sealed class FileParserActor : ReceiveActor
{
    private readonly IActorRef _recordProcessorRouter;
    private readonly IMaterializer _materializer;
    private readonly IngestionSettings _settings;

    public FileParserActor(IActorRef recordProcessorRouter, IMaterializer materializer, IngestionSettings settings)
    {
        _recordProcessorRouter = recordProcessorRouter;
        _materializer = materializer;
        _settings = settings;

        ReceiveAsync<BeginFileParsing>(HandleBeginFileParsingAsync);
    }

    private async Task HandleBeginFileParsingAsync(BeginFileParsing message)
    {
        try
        {
            var processedRecords = 0L;
            await FileIO.FromFile(new FileInfo(message.FilePath))
                .Via(Framing.Delimiter(ByteString.FromString(Environment.NewLine), 64 * 1024, allowTruncation: true))
                .Select(bytes => bytes.ToString(Encoding.UTF8))
                .Where(line => !string.IsNullOrWhiteSpace(line))
                .ZipWithIndex()
                .Select(item => new ProcessRecord(item.Item2 + 1, item.Item1))
                .SelectAsync(_settings.MaxInFlightRecords, command => ProcessLineAsync(command))
                .RunForeach(result =>
                {
                    message.Manager.Tell(result);
                    processedRecords++;
                }, _materializer);

            message.Manager.Tell(new ParserCompleted(processedRecords));
        }
        catch (Exception ex)
        {
            message.Manager.Tell(new ParserFailed(ex.Message));
        }
    }

    private async Task<IRecordProcessingResult> ProcessLineAsync(ProcessRecord command)
    {
        try
        {
            return await _recordProcessorRouter.Ask<IRecordProcessingResult>(command, _settings.WorkerAskTimeout);
        }
        catch (Exception ex)
        {
            return new InvalidRecord(command.LineNumber, command.RawLine, $"Worker processing failure: {ex.Message}");
        }
    }
}


using System.Text;
using Akka.Actor;
using Microsoft.VisualBasic.FileIO;

namespace AkkaSample1;

public sealed class FileParserActor : ReceiveActor
{
    private readonly IActorRef _recordProcessorRouter;
    private readonly IngestionSettings _settings;

    public FileParserActor(IActorRef recordProcessorRouter, IngestionSettings settings)
    {
        _recordProcessorRouter = recordProcessorRouter;
        _settings = settings;

        ReceiveAsync<BeginFileParsing>(HandleBeginFileParsingAsync);
    }

    private async Task HandleBeginFileParsingAsync(BeginFileParsing message)
    {
        try
        {
            ValidateFileExtension(message.FilePath);

            var processedRecords = 0L;

            using var parser = CreateParser(message.FilePath);
            var lineNumber = 0L;
            var skippedHeader = false;

            while (!parser.EndOfData)
            {
                string[]? fields;
                try
                {
                    fields = parser.ReadFields();
                }
                catch (MalformedLineException ex)
                {
                    var invalidLine = parser.ErrorLine ?? string.Empty;
                    var invalidNumber = parser.ErrorLineNumber > 0 ? parser.ErrorLineNumber : lineNumber + 1;
                    message.Manager.Tell(new InvalidRecord(invalidNumber, invalidLine, $"Malformed delimited record: {ex.Message}"));
                    continue;
                }

                if (fields is null || fields.All(string.IsNullOrWhiteSpace))
                {
                    continue;
                }

                if (!skippedHeader && IsHeaderRow(fields))
                {
                    skippedHeader = true;
                    continue;
                }

                lineNumber++;
                var rawLine = string.Join(_settings.FieldSeparator, fields.Select(SanitizeField));
                var command = new ProcessRecord(lineNumber, fields, rawLine);
                var result = await ProcessLineAsync(command);
                message.Manager.Tell(result);
                processedRecords++;
            }

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

    private TextFieldParser CreateParser(string filePath)
    {
        ValidateSeparators();

        var parser = new TextFieldParser(filePath, Encoding.UTF8)
        {
            TextFieldType = FieldType.Delimited,
            HasFieldsEnclosedInQuotes = true,
            TrimWhiteSpace = false
        };
        parser.SetDelimiters(_settings.FieldSeparator);
        return parser;
    }

    private static void ValidateFileExtension(string filePath)
    {
        var extension = Path.GetExtension(filePath);
        if (extension.Equals(".csv", StringComparison.OrdinalIgnoreCase) ||
            extension.Equals(".dat", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        throw new InvalidOperationException($"Unsupported input extension '{extension}'. Supported extensions: .csv, .dat.");
    }

    private static bool IsHeaderRow(string[] fields)
    {
        if (fields.Length < 3)
        {
            return false;
        }

        return fields[0].Trim().Equals("Id", StringComparison.OrdinalIgnoreCase) &&
               fields[1].Trim().Equals("EventDate", StringComparison.OrdinalIgnoreCase) &&
               fields[2].Trim().StartsWith("Payload", StringComparison.OrdinalIgnoreCase);
    }

    private void ValidateSeparators()
    {
        if (string.IsNullOrWhiteSpace(_settings.FieldSeparator))
        {
            throw new InvalidOperationException("Field separator cannot be empty.");
        }

        if (string.IsNullOrEmpty(_settings.TextQualifier) || _settings.TextQualifier != "\"")
        {
            throw new InvalidOperationException("Text qualifier must be a double quote (\") for the current parser.");
        }

        if (string.IsNullOrEmpty(_settings.RecordSeparator))
        {
            throw new InvalidOperationException("Record separator cannot be empty.");
        }
    }

    private string SanitizeField(string field)
    {
        if (field.Contains(_settings.TextQualifier, StringComparison.Ordinal))
        {
            field = field.Replace(_settings.TextQualifier, $"{_settings.TextQualifier}{_settings.TextQualifier}", StringComparison.Ordinal);
        }

        if (field.Contains(_settings.FieldSeparator, StringComparison.Ordinal) ||
            field.Contains('\r') ||
            field.Contains('\n') ||
            field.Contains(_settings.RecordSeparator, StringComparison.Ordinal))
        {
            return $"{_settings.TextQualifier}{field}{_settings.TextQualifier}";
        }

        return field;
    }
}


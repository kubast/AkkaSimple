using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Akka.Actor;

namespace AkkaSample1;

public sealed class RecordWorkerActor : ReceiveActor
{
    public RecordWorkerActor(IngestionSettings settings)
    {
        if (string.IsNullOrWhiteSpace(settings.FieldSeparator))
        {
            throw new ArgumentException("Field separator cannot be empty.", nameof(settings));
        }

        ReceiveAsync<ProcessRecord>(HandleProcessRecordAsync);
    }

    private async Task HandleProcessRecordAsync(ProcessRecord command)
    {
        if (command.RawLine.Contains("THROW", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("Forced worker crash for supervision demo.");
        }

        var parts = command.Fields;
        if (parts.Length < 3)
        {
            Sender.Tell(new InvalidRecord(command.LineNumber, command.RawLine, "Expected 3 columns: Id,EventDate,Payload."));
            return;
        }

        var id = parts[0].Trim();
        if (string.IsNullOrWhiteSpace(id))
        {
            Sender.Tell(new InvalidRecord(command.LineNumber, command.RawLine, "Id cannot be empty."));
            return;
        }

        if (!DateTime.TryParseExact(parts[1].Trim(), "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var eventDate))
        {
            Sender.Tell(new InvalidRecord(command.LineNumber, command.RawLine, "EventDate must use yyyy-MM-dd format."));
            return;
        }

        await Task.Delay(2);

        var checksum = ComputeMd5(command.RawLine);
        Sender.Tell(new ValidRecord(command.LineNumber, id, eventDate, parts[2].Trim(), checksum));
    }

    private static string ComputeMd5(string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        var hash = MD5.HashData(bytes);
        return Convert.ToHexString(hash);
    }
}


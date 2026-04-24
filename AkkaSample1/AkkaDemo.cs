using Akka.Actor;
using Akka.Configuration;

namespace AkkaSample1;

public static class AkkaDemo
{
    public static async Task RunAsync(string inputPath)
    {
        var hocon = ConfigurationFactory.ParseString("""
            akka {
              loglevel = INFO
              stdout-loglevel = INFO
            }
            """);

        using var actorSystem = ActorSystem.Create("ingestion-system", hocon);

        var settings = LoadSettingsFromEnvironment();
        var normalizedInputPath = NormalizePath(inputPath, settings.PathDelimiter);
        EnsureSampleFileExists(normalizedInputPath, settings);

        var manager = actorSystem.ActorOf(Props.Create(() => new IngestionManagerActor(settings)), "ingestion-manager");
        var summary = await manager.Ask<IngestionSummary>(new StartIngestion(normalizedInputPath), TimeSpan.FromMinutes(2));

        Console.WriteLine();
        Console.WriteLine("=== Ingestion Summary ===");
        Console.WriteLine($"File: {summary.FilePath}");
        Console.WriteLine($"Total records: {summary.TotalRecords}");
        Console.WriteLine($"Valid records: {summary.ValidRecords}");
        Console.WriteLine($"Invalid records: {summary.InvalidRecords}");
        Console.WriteLine($"Stored records: {summary.StoredRecords}");
        Console.WriteLine($"Written batches: {summary.WrittenBatches}");
        Console.WriteLine($"Duration: {summary.Duration.TotalMilliseconds:N0} ms");
    }

    private static IngestionSettings LoadSettingsFromEnvironment()
    {
        return new IngestionSettings
        {
            WorkerPoolSize = 4,
            MaxInFlightRecords = 32,
            StorageBatchSize = 50,
            StorageFlushInterval = TimeSpan.FromSeconds(2),
            WorkerAskTimeout = TimeSpan.FromSeconds(4),
            FieldSeparator = Environment.GetEnvironmentVariable("INGEST_FIELD_SEPARATOR") ?? ",",
            TextQualifier = Environment.GetEnvironmentVariable("INGEST_TEXT_QUALIFIER") ?? "\"",
            RecordSeparator = Environment.GetEnvironmentVariable("INGEST_RECORD_SEPARATOR") ?? Environment.NewLine,
            PathDelimiter = Environment.GetEnvironmentVariable("INGEST_PATH_DELIMITER") ?? "/"
        };
    }

    private static string NormalizePath(string path, string configuredPathDelimiter)
    {
        if (string.IsNullOrEmpty(configuredPathDelimiter))
        {
            return path;
        }

        var systemDelimiter = Path.DirectorySeparatorChar.ToString();
        if (configuredPathDelimiter == systemDelimiter)
        {
            return path;
        }

        return path.Replace(configuredPathDelimiter, systemDelimiter, StringComparison.Ordinal);
    }

    private static void EnsureSampleFileExists(string inputPath, IngestionSettings settings)
    {
        if (File.Exists(inputPath))
        {
            return;
        }

        Directory.CreateDirectory(Path.GetDirectoryName(inputPath) ?? AppContext.BaseDirectory);
        var sampleRows = new[]
        {
            new[] { "user-1", "2026-04-20", "OrderCreated" },
            new[] { "user-2", "2026-04-21", "InvoiceGenerated" },
            new[] { "", "2026-04-21", "MissingId" },
            new[] { "user-3", "2026/04/21", "BadDateFormat" },
            new[] { "user-4", "2026-04-22", "THROW" },
            new[] { "user-5", "2026-04-22", "PaymentCaptured" }
        };

        var lines = sampleRows.Select(row => string.Join(settings.FieldSeparator, row));
        var content = string.Join(settings.RecordSeparator, lines);
        File.WriteAllText(inputPath, content);
    }
}

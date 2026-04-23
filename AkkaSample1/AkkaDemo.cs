using Akka.Actor;
using Akka.Configuration;

namespace AkkaSample1;

public static class AkkaDemo
{
    public static async Task RunAsync(string inputPath)
    {
        EnsureSampleFileExists(inputPath);

        var hocon = ConfigurationFactory.ParseString("""
            akka {
              loglevel = INFO
              stdout-loglevel = INFO
            }
            """);

        using var actorSystem = ActorSystem.Create("ingestion-system", hocon);

        var settings = new IngestionSettings
        {
            WorkerPoolSize = 4,
            MaxInFlightRecords = 32,
            StorageBatchSize = 50,
            StorageFlushInterval = TimeSpan.FromSeconds(2),
            WorkerAskTimeout = TimeSpan.FromSeconds(4)
        };

        var manager = actorSystem.ActorOf(Props.Create(() => new IngestionManagerActor(settings)), "ingestion-manager");
        var summary = await manager.Ask<IngestionSummary>(new StartIngestion(inputPath), TimeSpan.FromMinutes(2));

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

    private static void EnsureSampleFileExists(string inputPath)
    {
        if (File.Exists(inputPath))
        {
            return;
        }

        Directory.CreateDirectory(Path.GetDirectoryName(inputPath) ?? AppContext.BaseDirectory);
        File.WriteAllLines(inputPath,
        [
            "user-1,2026-04-20,OrderCreated",
            "user-2,2026-04-21,InvoiceGenerated",
            ",2026-04-21,MissingId",
            "user-3,2026/04/21,BadDateFormat",
            "user-4,2026-04-22,THROW",
            "user-5,2026-04-22,PaymentCaptured"
        ]);
    }
}

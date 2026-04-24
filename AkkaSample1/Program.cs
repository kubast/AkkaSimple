using AkkaSample1;

var inputPath = args.Length > 0
    ? args[0]
    : Path.Combine(AppContext.BaseDirectory, "sample-data.csv");

await AkkaDemo.RunAsync(inputPath);
// complete the file ingestion
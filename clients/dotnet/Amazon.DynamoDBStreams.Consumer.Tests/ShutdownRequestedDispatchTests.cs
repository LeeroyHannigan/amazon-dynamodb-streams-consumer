using System.Runtime.CompilerServices;
using Xunit;

namespace Amazon.DynamoDBStreams.Consumer.Tests;

// Verifies that a server "shutdown_requested" message is dispatched to the
// processor's ShutdownRequested callback (and that no checkpoint is sent for
// it). Driven through the shared replay_sidecar.py — no AWS, no real sidecar.
public class ShutdownRequestedDispatchTests
{
    private static string ConfDir([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        return Path.GetFullPath(Path.Combine(dir, "..", "..", "..", "conformance"));
    }

    private sealed class Collector : IRecordProcessor
    {
        public List<string> Requested { get; } = new();
        public List<string> Ended { get; } = new();
        public int Batches { get; private set; }

        public void ProcessRecords(IReadOnlyList<Record> records) => Batches++;
        public void ShardEnded(string shardId) => Ended.Add(shardId);
        public void ShutdownRequested(string shardId) => Requested.Add(shardId);
    }

    [Fact]
    public async Task ShutdownRequested_IsDispatchedToProcessor()
    {
        var conf = ConfDir();
        var replay = Path.Combine(conf, "replay_sidecar.py");

        // Emit a single shutdown_requested message then end (stdout close → clean stop).
        var fixture = Path.Combine(Path.GetTempPath(), $"shutdown_requested_{Guid.NewGuid():N}.json");
        await File.WriteAllTextAsync(
            fixture,
            "{\"server_script\":[{\"emit\":{\"type\":\"shutdown_requested\",\"shard\":\"shard-1\"}}],"
                + "\"expect\":{}}");

        try
        {
            var c = new Collector();
            var worker = new Worker(new WorkerConfig
            {
                StreamArn = "arn:aws:dynamodb:us-east-1:1:table/T/stream/2026",
                LeaseTable = "leases",
                Processor = c,
                SidecarCmd = new[] { "python3", replay, fixture },
            });

            var code = await worker.RunAsync();

            Assert.Equal(0, code);
            Assert.Equal(new[] { "shard-1" }, c.Requested);
            Assert.Empty(c.Ended);
            Assert.Equal(0, c.Batches);
        }
        finally
        {
            File.Delete(fixture);
        }
    }
}

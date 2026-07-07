package com.amazon.dynamodbstreams.consumer;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Drives the Worker's server-message dispatch directly: a trivial sidecar (sh)
// emits a shutdown_requested line then exits, and we assert the processor's
// shutdownRequested callback fires with the shard id (and that no
// records/checkpoints occur).
class ShutdownRequestedDispatchTest {

    private static final class Collector implements RecordProcessor {
        final List<String> requested = new ArrayList<>();
        final List<String> ended = new ArrayList<>();
        int batches = 0;

        @Override
        public void processRecords(List<Record> records) {
            batches++;
        }

        @Override
        public void shardEnded(String shardId) {
            ended.add(shardId);
        }

        @Override
        public void shutdownRequested(String shardId) {
            requested.add(shardId);
        }
    }

    @Test
    void shutdownRequestedDispatchesToProcessor() throws Exception {
        Collector c = new Collector();
        // Sidecar reads our ready message from stdin (ignored), emits one
        // shutdown_requested message, then closes stdout -> Worker.run() returns.
        Worker w = new Worker(WorkerConfig.builder()
                .streamArn("arn:aws:dynamodb:us-east-1:1:table/T/stream/2026")
                .leaseTable("leases")
                .processor(c)
                .sidecarCmd(List.of("sh", "-c",
                        "read _; printf '%s\\n' '{\"type\":\"shutdown_requested\",\"shard\":\"shard-000001\"}'"))
                .build());

        int code = w.run();

        assertEquals(0, code, "sidecar exit code");
        assertEquals(List.of("shard-000001"), c.requested, "shutdownRequested shard ids");
        assertTrue(c.ended.isEmpty(), "shardEnded must not fire on shutdown_requested");
        assertEquals(0, c.batches, "no record batches on shutdown_requested");
    }
}

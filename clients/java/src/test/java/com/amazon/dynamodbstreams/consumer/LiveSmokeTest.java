package com.amazon.dynamodbstreams.consumer;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

// Live smoke test: runs the Worker against the REAL Rust sidecar and a REAL
// DynamoDB stream. Skipped unless DDB_STREAMS_CONSUMER_IT=1. The stream + lease
// table are provisioned out-of-band and passed via env:
//   DDB_STREAMS_CONSUMER_STREAM_ARN, DDB_STREAMS_CONSUMER_LEASE_TABLE,
//   DDB_STREAMS_CONSUMER_SIDECAR (path to the built binary), AWS_REGION.
class LiveSmokeTest {

    private static final class Collector implements RecordProcessor {
        final List<Record> records = new ArrayList<>();

        @Override
        public synchronized void processRecords(List<Record> recs) {
            records.addAll(recs);
        }

        synchronized int count() {
            return records.size();
        }

        synchronized List<Record> snapshot() {
            return new ArrayList<>(records);
        }
    }

    @Test
    void liveConsume() throws Exception {
        assumeTrue(System.getenv("DDB_STREAMS_CONSUMER_IT") != null,
                "set DDB_STREAMS_CONSUMER_IT=1 to run the live smoke test");

        String arn = System.getenv("DDB_STREAMS_CONSUMER_STREAM_ARN");
        String leaseTable = System.getenv("DDB_STREAMS_CONSUMER_LEASE_TABLE");
        assertTrue(arn != null && !arn.isEmpty(), "DDB_STREAMS_CONSUMER_STREAM_ARN must be set");
        assertTrue(leaseTable != null && !leaseTable.isEmpty(), "DDB_STREAMS_CONSUMER_LEASE_TABLE must be set");

        Collector c = new Collector();
        String region = System.getenv("AWS_REGION");
        Worker worker = new Worker(WorkerConfig.builder()
                .streamArn(arn)
                .leaseTable(leaseTable)
                .processor(c)
                .region(region == null ? "us-east-1" : region)
                .recordFormat(RecordFormat.NATIVE)
                .pollIntervalMs(200)
                .build());

        CompletableFuture<Integer> run = CompletableFuture.supplyAsync(() -> {
            try {
                return worker.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        for (int i = 0; i < 60 && c.count() < 5; i++) {
            Thread.sleep(500);
        }
        worker.stop();
        run.get();

        assertTrue(c.count() >= 5, "expected >= 5 records, got " + c.count());

        // Native decoding: the partition key is exposed as a bare String.
        Record withKey = c.snapshot().stream()
                .filter(r -> r.keys().containsKey("pk"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no record carried key 'pk'"));
        assertTrue(withKey.keys().get("pk") instanceof String, "native key 'pk' should be a String");
    }
}

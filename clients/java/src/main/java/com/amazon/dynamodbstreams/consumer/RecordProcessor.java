package com.amazon.dynamodbstreams.consumer;

import java.util.List;

/** Customer business logic: called with ordered batches for a single shard. */
public interface RecordProcessor {
    /**
     * Deliver a batch of records, already in per-shard sequence order. Returning
     * normally acknowledges the batch, advancing the durable checkpoint to its
     * last record (at-least-once).
     */
    void processRecords(List<Record> records);

    /** Called when the shard reaches SHARD_END. Default: no-op. */
    default void shardEnded(String shardId) {
        // no-op by default
    }
}

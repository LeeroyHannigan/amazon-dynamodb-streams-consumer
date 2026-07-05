package com.amazon.dynamodbstreams.consumer;

import java.util.Collections;
import java.util.Map;

/**
 * A DynamoDB Streams change record delivered to a {@link RecordProcessor}.
 *
 * <p>Item images ({@link #keys()}, {@link #newImage()}, {@link #oldImage()}) are
 * decoded according to the worker's {@link RecordFormat}: in
 * {@link RecordFormat#NATIVE} each value is a plain Java object
 * ({@link String}, {@link Boolean}, {@code null}, {@code byte[]},
 * {@link java.util.List}, or nested {@code Map<String, Object>}); in
 * {@link RecordFormat#DDB_JSON} each value is a single-entry map in canonical
 * DynamoDB JSON form (e.g. {@code {"S": "x"}}).
 */
public final class Record {
    private final String shardId;
    private final String eventName;
    private final String sequenceNumber;
    private final String streamViewType;
    private final Map<String, Object> keys;
    private final Map<String, Object> newImage;
    private final Map<String, Object> oldImage;

    public Record(String shardId, String eventName, String sequenceNumber, String streamViewType,
                  Map<String, Object> keys, Map<String, Object> newImage, Map<String, Object> oldImage) {
        this.shardId = shardId;
        this.eventName = eventName;
        this.sequenceNumber = sequenceNumber;
        this.streamViewType = streamViewType;
        this.keys = keys == null ? Collections.emptyMap() : keys;
        this.newImage = newImage;
        this.oldImage = oldImage;
    }

    /** The shard this record was delivered from. */
    public String shardId() {
        return shardId;
    }

    /** INSERT / MODIFY / REMOVE. */
    public String eventName() {
        return eventName;
    }

    /** The record's sequence number. */
    public String sequenceNumber() {
        return sequenceNumber;
    }

    /** KEYS_ONLY / NEW_IMAGE / OLD_IMAGE / NEW_AND_OLD_IMAGES. */
    public String streamViewType() {
        return streamViewType;
    }

    /** The key attributes of the changed item. */
    public Map<String, Object> keys() {
        return keys;
    }

    /** The item image after the change, or {@code null} when absent. */
    public Map<String, Object> newImage() {
        return newImage;
    }

    /** The item image before the change, or {@code null} when absent. */
    public Map<String, Object> oldImage() {
        return oldImage;
    }
}

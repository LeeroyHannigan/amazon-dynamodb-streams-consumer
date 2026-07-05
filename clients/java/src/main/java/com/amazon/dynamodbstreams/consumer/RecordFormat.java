package com.amazon.dynamodbstreams.consumer;

/**
 * How item-image attribute values are surfaced on a {@link Record}. Set once on
 * the {@link WorkerConfig}; applies to every record. Mirrors the
 * {@code record_format} option in the Python/Go/Node/.NET clients.
 */
public enum RecordFormat {
    /**
     * Plain Java values: {@code S}/{@code N} → {@link String} (numbers stay
     * canonical strings, lossless), {@code Bool} → {@link Boolean}, {@code Null}
     * → {@code null}, {@code B} → {@code byte[]}, sets → {@code List}, {@code M}
     * → {@code Map}, {@code L} → {@code List}. No {@code {"S": ...}} wrappers.
     */
    NATIVE,

    /**
     * Canonical DynamoDB JSON
     * ({@code {"S"|"N"|"BOOL"|"NULL"|"B"|"M"|"L"|"SS"|"NS"|"BS"}}), the shape the
     * AWS SDK consumes — for SDK interop or migrating from KCL.
     */
    DDB_JSON,

    /**
     * The AWS SDK for Java v2 typed model: each attribute is a
     * {@code software.amazon.awssdk.services.dynamodb.model.AttributeValue}, so a
     * record's images drop straight into the SDK (PutItem, the enhanced client,
     * transactions) — full KCL/KCA parity. Values in {@link Record#keys()} /
     * {@link Record#newImage()} / {@link Record#oldImage()} are {@code AttributeValue}
     * instances; use {@code SdkAttributeValues.toItem(...)} for a typed
     * {@code Map<String, AttributeValue>}. Requires {@code software.amazon.awssdk:dynamodb}
     * on the classpath (a {@code provided} dependency of this client).
     */
    SDK
}

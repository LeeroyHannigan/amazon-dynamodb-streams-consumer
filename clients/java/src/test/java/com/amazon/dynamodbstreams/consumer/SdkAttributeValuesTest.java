package com.amazon.dynamodbstreams.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SdkAttributeValuesTest {
    private static final ObjectMapper M = new ObjectMapper();

    private static final String WIRE = "{"
            + "\"s\":{\"S\":\"widget\"},"
            + "\"n\":{\"N\":\"42\"},"
            + "\"active\":{\"Bool\":true},"
            + "\"deleted\":\"Null\","
            + "\"blob\":{\"B\":[1,2,3]},"
            + "\"tags\":{\"Ss\":[\"a\",\"b\"]},"
            + "\"meta\":{\"M\":{\"k\":{\"S\":\"v\"}}},"
            + "\"list\":{\"L\":[{\"N\":\"7\"},\"Null\"]}"
            + "}";

    @Test
    void decodesToSdkAttributeValues() throws Exception {
        JsonNode wire = M.readTree(WIRE);
        Map<String, AttributeValue> item = SdkAttributeValues.toItem(SdkAttributeValues.decodeItem(wire));

        assertEquals("widget", item.get("s").s());
        assertEquals("42", item.get("n").n());
        assertEquals(Boolean.TRUE, item.get("active").bool());
        assertTrue(item.get("deleted").nul());
        assertEquals("widget", item.get("s").s());

        byte[] blob = item.get("blob").b().asByteArray();
        assertEquals(3, blob.length);
        assertEquals(1, blob[0]);

        assertEquals(java.util.List.of("a", "b"), item.get("tags").ss());
        assertEquals("v", item.get("meta").m().get("k").s());
        assertEquals("7", item.get("list").l().get(0).n());
        assertTrue(item.get("list").l().get(1).nul());
    }
}

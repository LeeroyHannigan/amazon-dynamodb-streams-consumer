package com.amazon.dynamodbstreams.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AttributeValuesTest {
    private static final ObjectMapper M = new ObjectMapper();

    // A wire item exercising every AttrValue variant, serde-externally-tagged.
    private static final String WIRE = "{"
            + "\"s\":{\"S\":\"widget\"},"
            + "\"n\":{\"N\":\"42\"},"
            + "\"active\":{\"Bool\":true},"
            + "\"deleted\":\"Null\","
            + "\"blob\":{\"B\":[1,2,3]},"
            + "\"tags\":{\"Ss\":[\"a\",\"b\"]},"
            + "\"scores\":{\"Ns\":[\"1\",\"2.5\"]},"
            + "\"blobs\":{\"Bs\":[[1,2],[3]]},"
            + "\"meta\":{\"M\":{\"k\":{\"S\":\"v\"}}},"
            + "\"list\":{\"L\":[{\"N\":\"7\"},\"Null\"]}"
            + "}";

    private static JsonNode wire() throws Exception {
        return M.readTree(WIRE);
    }

    @Test
    @SuppressWarnings("unchecked")
    void nativeHasNoTypeWrappersAndKeepsNumbersAsStrings() throws Exception {
        Map<String, Object> item = AttributeValues.decodeItem(wire(), RecordFormat.NATIVE);

        assertEquals("widget", item.get("s"));
        assertEquals("42", item.get("n")); // number stays a string (lossless)
        assertEquals(Boolean.TRUE, item.get("active"));
        assertNull(item.get("deleted"));
        assertArrayEquals(new byte[] {1, 2, 3}, (byte[]) item.get("blob"));
        assertEquals(List.of("a", "b"), item.get("tags"));
        assertEquals(List.of("1", "2.5"), item.get("scores"));

        List<byte[]> blobs = (List<byte[]>) item.get("blobs");
        assertArrayEquals(new byte[] {1, 2}, blobs.get(0));
        assertArrayEquals(new byte[] {3}, blobs.get(1));

        Map<String, Object> meta = (Map<String, Object>) item.get("meta");
        assertEquals("v", meta.get("k"));

        List<Object> list = (List<Object>) item.get("list");
        assertEquals("7", list.get(0));
        assertNull(list.get(1));
    }

    @Test
    @SuppressWarnings("unchecked")
    void ddbJsonIsCanonicalTypedForm() throws Exception {
        Map<String, Object> item = AttributeValues.decodeItem(wire(), RecordFormat.DDB_JSON);

        assertEquals("widget", wrapper(item.get("s"), "S"));
        assertEquals("42", wrapper(item.get("n"), "N"));
        assertEquals(Boolean.TRUE, wrapper(item.get("active"), "BOOL"));
        assertEquals(Boolean.TRUE, wrapper(item.get("deleted"), "NULL"));
        assertEquals("AQID", wrapper(item.get("blob"), "B")); // base64 of [1,2,3]
        assertEquals(List.of("a", "b"), wrapper(item.get("tags"), "SS"));
        assertEquals(List.of("AQI=", "Aw=="), wrapper(item.get("blobs"), "BS"));

        Map<String, Object> m = (Map<String, Object>) wrapper(item.get("meta"), "M");
        assertEquals("v", wrapper(m.get("k"), "S"));

        List<Object> l = (List<Object>) wrapper(item.get("list"), "L");
        assertEquals("7", wrapper(l.get(0), "N"));
        assertEquals(Boolean.TRUE, wrapper(l.get(1), "NULL"));
    }

    @SuppressWarnings("unchecked")
    private static Object wrapper(Object v, String tag) {
        Map<String, Object> dict = (Map<String, Object>) v;
        assertTrue(dict.containsKey(tag), "expected tag " + tag + ", got " + dict.keySet());
        return dict.get(tag);
    }
}

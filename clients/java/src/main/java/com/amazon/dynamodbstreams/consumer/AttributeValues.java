package com.amazon.dynamodbstreams.consumer;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts serde-externally-tagged wire attribute values (see
 * {@code protocol/src/lib.rs}) into either native Java objects or canonical
 * DynamoDB JSON. The bare string {@code "Null"} is the null variant; every other
 * variant is a single-key object like {@code {"S":"x"}}. Byte values arrive as
 * JSON arrays of integers.
 */
final class AttributeValues {
    private AttributeValues() {
    }

    static Map<String, Object> decodeItem(JsonNode item, RecordFormat fmt) {
        Map<String, Object> out = new LinkedHashMap<>();
        if (item == null || !item.isObject()) {
            return out;
        }
        Iterator<Map.Entry<String, JsonNode>> it = item.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            out.put(e.getKey(), fmt == RecordFormat.DDB_JSON ? toDdbJson(e.getValue()) : toNative(e.getValue()));
        }
        return out;
    }

    private static Map.Entry<String, JsonNode> singleTag(JsonNode v) {
        if (!v.isObject() || v.size() != 1) {
            throw new IllegalArgumentException("attribute must have exactly one type tag: " + v);
        }
        return v.fields().next();
    }

    static Object toNative(JsonNode v) {
        if (v.isTextual()) {
            if ("Null".equals(v.textValue())) {
                return null;
            }
            throw new IllegalArgumentException("invalid attribute value: " + v);
        }
        Map.Entry<String, JsonNode> e = singleTag(v);
        String tag = e.getKey();
        JsonNode val = e.getValue();
        switch (tag) {
            case "S":
            case "N":
                return val.asText();
            case "Bool":
                return val.asBoolean();
            case "B":
                return bytes(val);
            case "Ss":
            case "Ns":
                return stringList(val);
            case "Bs":
                return byteArrayList(val);
            case "M":
                return decodeItem(val, RecordFormat.NATIVE);
            case "L": {
                List<Object> list = new ArrayList<>();
                for (JsonNode n : val) {
                    list.add(toNative(n));
                }
                return list;
            }
            default:
                throw new IllegalArgumentException("unknown attribute type tag: " + tag);
        }
    }

    static Map<String, Object> toDdbJson(JsonNode v) {
        Map<String, Object> out = new LinkedHashMap<>();
        if (v.isTextual()) {
            if ("Null".equals(v.textValue())) {
                out.put("NULL", Boolean.TRUE);
                return out;
            }
            throw new IllegalArgumentException("invalid attribute value: " + v);
        }
        Map.Entry<String, JsonNode> e = singleTag(v);
        String tag = e.getKey();
        JsonNode val = e.getValue();
        switch (tag) {
            case "S":
                out.put("S", val.asText());
                break;
            case "N":
                out.put("N", val.asText());
                break;
            case "Bool":
                out.put("BOOL", val.asBoolean());
                break;
            case "B":
                out.put("B", base64(val));
                break;
            case "Ss":
                out.put("SS", stringList(val));
                break;
            case "Ns":
                out.put("NS", stringList(val));
                break;
            case "Bs":
                out.put("BS", base64List(val));
                break;
            case "M":
                out.put("M", decodeItem(val, RecordFormat.DDB_JSON));
                break;
            case "L": {
                List<Object> list = new ArrayList<>();
                for (JsonNode n : val) {
                    list.add(toDdbJson(n));
                }
                out.put("L", list);
                break;
            }
            default:
                throw new IllegalArgumentException("unknown attribute type tag: " + tag);
        }
        return out;
    }

    private static byte[] bytes(JsonNode arr) {
        byte[] b = new byte[arr.size()];
        for (int i = 0; i < arr.size(); i++) {
            b[i] = (byte) arr.get(i).asInt();
        }
        return b;
    }

    private static List<String> stringList(JsonNode arr) {
        List<String> list = new ArrayList<>();
        for (JsonNode n : arr) {
            list.add(n.asText());
        }
        return list;
    }

    private static List<byte[]> byteArrayList(JsonNode arr) {
        List<byte[]> list = new ArrayList<>();
        for (JsonNode n : arr) {
            list.add(bytes(n));
        }
        return list;
    }

    private static String base64(JsonNode arr) {
        return Base64.getEncoder().encodeToString(bytes(arr));
    }

    private static List<String> base64List(JsonNode arr) {
        List<String> list = new ArrayList<>();
        for (JsonNode n : arr) {
            list.add(base64(n));
        }
        return list;
    }
}

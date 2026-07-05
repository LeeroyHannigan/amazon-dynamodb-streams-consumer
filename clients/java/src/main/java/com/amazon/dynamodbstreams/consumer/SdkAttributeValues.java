package com.amazon.dynamodbstreams.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts serde-externally-tagged wire attribute values into the AWS SDK for
 * Java v2 typed model ({@link AttributeValue}) for {@link RecordFormat#SDK}.
 * A record decoded in SDK mode carries {@code AttributeValue} instances as its
 * image values, so they drop straight into the SDK.
 */
public final class SdkAttributeValues {
    private SdkAttributeValues() {
    }

    /**
     * View a record image (decoded in {@link RecordFormat#SDK}) as a typed
     * {@code Map<String, AttributeValue>} ready for the SDK (e.g.
     * {@code ddb.putItem(b -> b.tableName(t).item(SdkAttributeValues.toItem(r.newImage())))}).
     *
     * @throws ClassCastException if the image was not decoded in SDK mode.
     */
    public static Map<String, AttributeValue> toItem(Map<String, Object> image) {
        Map<String, AttributeValue> out = new LinkedHashMap<>();
        if (image == null) {
            return out;
        }
        for (Map.Entry<String, Object> e : image.entrySet()) {
            out.put(e.getKey(), (AttributeValue) e.getValue());
        }
        return out;
    }

    static Map<String, Object> decodeItem(JsonNode item) {
        Map<String, Object> out = new LinkedHashMap<>();
        if (item == null || !item.isObject()) {
            return out;
        }
        item.fields().forEachRemaining(e -> out.put(e.getKey(), toAttributeValue(e.getValue())));
        return out;
    }

    static AttributeValue toAttributeValue(JsonNode v) {
        if (v.isTextual()) {
            if ("Null".equals(v.textValue())) {
                return AttributeValue.fromNul(true);
            }
            throw new IllegalArgumentException("invalid attribute value: " + v);
        }
        if (!v.isObject() || v.size() != 1) {
            throw new IllegalArgumentException("attribute must have exactly one type tag: " + v);
        }
        Map.Entry<String, JsonNode> e = v.fields().next();
        String tag = e.getKey();
        JsonNode val = e.getValue();
        switch (tag) {
            case "S":
                return AttributeValue.fromS(val.asText());
            case "N":
                return AttributeValue.fromN(val.asText());
            case "Bool":
                return AttributeValue.fromBool(val.asBoolean());
            case "B":
                return AttributeValue.fromB(SdkBytes.fromByteArray(bytes(val)));
            case "Ss":
                return AttributeValue.fromSs(stringList(val));
            case "Ns":
                return AttributeValue.fromNs(stringList(val));
            case "Bs": {
                List<SdkBytes> bs = new ArrayList<>();
                for (JsonNode n : val) {
                    bs.add(SdkBytes.fromByteArray(bytes(n)));
                }
                return AttributeValue.fromBs(bs);
            }
            case "M": {
                Map<String, AttributeValue> m = new LinkedHashMap<>();
                val.fields().forEachRemaining(en -> m.put(en.getKey(), toAttributeValue(en.getValue())));
                return AttributeValue.fromM(m);
            }
            case "L": {
                List<AttributeValue> l = new ArrayList<>();
                for (JsonNode n : val) {
                    l.add(toAttributeValue(n));
                }
                return AttributeValue.fromL(l);
            }
            default:
                throw new IllegalArgumentException("unknown attribute type tag: " + tag);
        }
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
}

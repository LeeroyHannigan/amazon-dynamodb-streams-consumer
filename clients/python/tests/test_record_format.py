"""Tests for the worker-level record_format option (native vs ddb_json)."""

import unittest

from dynamodb_streams_consumer.record import Record
from dynamodb_streams_consumer.worker import Worker


# A wire record covering every attribute type (sidecar's externally-tagged form).
WIRE = {
    "sequence_number": "100",
    "event_name": "INSERT",
    "stream_view_type": "NEW_AND_OLD_IMAGES",
    "keys": {"pk": {"S": "k1"}, "sk": {"N": "42"}},
    "new_image": {
        "pk": {"S": "k1"},
        "sk": {"N": "42"},
        "active": {"Bool": True},
        "note": "Null",
        "blob": {"B": [1, 2, 3]},
        "tags": {"Ss": ["a", "b"]},
        "nums": {"Ns": ["1", "2.5"]},
        "blobs": {"Bs": [[9]]},
        "nested": {"M": {"inner": {"N": "7"}}},
        "list": {"L": [{"S": "x"}, "Null"]},
    },
}


class RecordFormatTest(unittest.TestCase):
    def test_native_default(self):
        r = Record.from_wire("shard-1", WIRE)  # default native
        self.assertEqual(r.keys, {"pk": "k1", "sk": "42"})  # N stays string
        ni = r.new_image
        self.assertEqual(ni["active"], True)
        self.assertIsNone(ni["note"])
        self.assertEqual(ni["blob"], b"\x01\x02\x03")
        self.assertEqual(ni["tags"], ["a", "b"])
        self.assertEqual(ni["nums"], ["1", "2.5"])
        self.assertEqual(ni["blobs"], [b"\x09"])
        self.assertEqual(ni["nested"], {"inner": "7"})
        self.assertEqual(ni["list"], ["x", None])

    def test_ddb_json(self):
        r = Record.from_wire("shard-1", WIRE, "ddb_json")
        # Canonical DynamoDB JSON — consumable by boto3 TypeDeserializer / the SDK.
        self.assertEqual(r.keys, {"pk": {"S": "k1"}, "sk": {"N": "42"}})
        ni = r.new_image
        self.assertEqual(ni["active"], {"BOOL": True})
        self.assertEqual(ni["note"], {"NULL": True})
        self.assertEqual(ni["blob"], {"B": "AQID"})  # base64 of 01 02 03
        self.assertEqual(ni["tags"], {"SS": ["a", "b"]})
        self.assertEqual(ni["nums"], {"NS": ["1", "2.5"]})
        self.assertEqual(ni["blobs"], {"BS": ["CQ=="]})  # base64 of 0x09
        self.assertEqual(ni["nested"], {"M": {"inner": {"N": "7"}}})
        self.assertEqual(ni["list"], {"L": [{"S": "x"}, {"NULL": True}]})

    def test_ddb_json_roundtrips_through_boto3_deserializer(self):
        try:
            from boto3.dynamodb.types import TypeDeserializer
        except Exception:
            self.skipTest("boto3 not available")
        r = Record.from_wire("shard-1", WIRE, "ddb_json")
        d = TypeDeserializer()
        native = {k: d.deserialize(v) for k, v in r.keys.items()}
        self.assertEqual(native["pk"], "k1")

    def test_worker_rejects_bad_format(self):
        with self.assertRaises(ValueError):
            Worker(
                stream_arn="arn:x",
                lease_table="t",
                processor=object(),
                record_format="bogus",
                sidecar_cmd=["true"],  # avoid sidecar discovery
            )


if __name__ == "__main__":
    unittest.main()

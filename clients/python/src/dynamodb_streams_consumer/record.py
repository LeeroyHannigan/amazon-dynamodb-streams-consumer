"""Typed DynamoDB Streams change record, decoded from the sidecar wire format.

The sidecar serializes the Rust ``AttrValue`` enum externally-tagged, e.g.
``{"S": "k1"}``, ``{"N": "42"}``, ``{"Bool": true}``, ``"Null"``,
``{"M": {...}}``, ``{"L": [...]}``, ``{"Ss": [...]}``.

Records are exposed in one of two shapes, chosen once at the ``Worker`` level via
``record_format`` (see :class:`~dynamodb_streams_consumer.worker.Worker`):

* ``"native"`` (default) — native Python values (:func:`decode_attr`); numbers
  stay as strings exactly as DynamoDB represents them (lossless, no float
  rounding). Removes the DynamoDB-JSON unmarshalling burden.
* ``"ddb_json"`` — canonical DynamoDB JSON (:func:`to_ddb_json`), the
  ``{"S": ...}`` / ``{"N": ...}`` / ``{"BOOL": ...}`` / ``{"NULL": true}`` /
  ``{"B": <base64>}`` / ``{"SS"|"NS"|"BS": ...}`` shape that ``boto3``'s
  ``TypeDeserializer`` and the AWS SDKs consume. For SDK interop / KCL parity.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

NATIVE = "native"
DDB_JSON = "ddb_json"


def decode_attr(av: Any) -> Any:
    """Decode one wire attribute value into a native Python value."""
    if av == "Null":
        return None
    if not isinstance(av, dict) or len(av) != 1:
        # Be permissive: unknown shapes pass through unchanged.
        return av
    tag, val = next(iter(av.items()))
    if tag == "S":
        return val
    if tag == "N":
        return val  # keep DynamoDB's canonical string form (lossless)
    if tag == "Bool":
        return val
    if tag == "B":
        # Rust serializes bytes as a JSON array of u8.
        return bytes(val)
    if tag == "M":
        return {k: decode_attr(v) for k, v in val.items()}
    if tag == "L":
        return [decode_attr(v) for v in val]
    if tag == "Ss":
        return list(val)
    if tag == "Ns":
        return list(val)  # numeric set members stay strings
    if tag == "Bs":
        return [bytes(b) for b in val]
    return av


def decode_item(item: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not item:
        return {}
    return {k: decode_attr(v) for k, v in item.items()}


def to_ddb_json(av: Any) -> Any:
    """Convert one wire attribute value into canonical DynamoDB JSON.

    Maps the sidecar's externally-tagged form to the standard DynamoDB attribute
    value shape (the form ``boto3.dynamodb.types.TypeDeserializer`` and the
    low-level SDK accept), so a processor can hand it straight to the SDK."""
    if av == "Null":
        return {"NULL": True}
    if not isinstance(av, dict) or len(av) != 1:
        return av
    tag, val = next(iter(av.items()))
    if tag == "S":
        return {"S": val}
    if tag == "N":
        return {"N": val}
    if tag == "Bool":
        return {"BOOL": val}
    if tag == "B":
        return {"B": base64.b64encode(bytes(val)).decode("ascii")}
    if tag == "M":
        return {"M": {k: to_ddb_json(v) for k, v in val.items()}}
    if tag == "L":
        return {"L": [to_ddb_json(v) for v in val]}
    if tag == "Ss":
        return {"SS": list(val)}
    if tag == "Ns":
        return {"NS": list(val)}
    if tag == "Bs":
        return {"BS": [base64.b64encode(bytes(b)).decode("ascii") for b in val]}
    return av


def ddb_json_item(item: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not item:
        return {}
    return {k: to_ddb_json(v) for k, v in item.items()}


@dataclass
class Record:
    """One item-level change delivered from a DynamoDB stream shard."""

    shard_id: str
    sequence_number: Optional[str]
    event_name: Optional[str]  # INSERT / MODIFY / REMOVE
    stream_view_type: Optional[str]
    keys: Dict[str, Any] = field(default_factory=dict)
    new_image: Optional[Dict[str, Any]] = None
    old_image: Optional[Dict[str, Any]] = None

    @classmethod
    def from_wire(
        cls, shard_id: str, wire: Dict[str, Any], record_format: str = NATIVE
    ) -> "Record":
        ni = wire.get("new_image")
        oi = wire.get("old_image")
        conv_item = ddb_json_item if record_format == DDB_JSON else decode_item
        return cls(
            shard_id=shard_id,
            sequence_number=wire.get("sequence_number"),
            event_name=wire.get("event_name"),
            stream_view_type=wire.get("stream_view_type"),
            keys=conv_item(wire.get("keys")),
            new_image=conv_item(ni) if ni else None,
            old_image=conv_item(oi) if oi else None,
        )

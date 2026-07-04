// Attribute-value model + decoder, matching the Rust serde wire tags
// (protocol/src/lib.rs). Source of truth for the emitted .d.ts.

export type AttrValue = string | boolean | null | Buffer | AttrValue[] | { [k: string]: AttrValue };
export type Item = { [k: string]: AttrValue };

export interface Record {
  shardId: string;
  eventName: string | null;
  sequenceNumber: string | null;
  streamViewType: string | null;
  keys: Item;
  newImage: Item | null;
  oldImage: Item | null;
}

interface WireRecord {
  event_name?: string | null;
  sequence_number?: string | null;
  stream_view_type?: string | null;
  keys?: { [k: string]: unknown } | null;
  new_image?: { [k: string]: unknown } | null;
  old_image?: { [k: string]: unknown } | null;
}

// Decodes one serde-externally-tagged AttrValue. Null is the bare string
// "Null"; every other variant is a single-key object like {"S":"x"}.
export function decodeAttr(v: unknown): AttrValue {
  if (v === 'Null') return null;
  if (typeof v !== 'object' || v === null || Array.isArray(v)) {
    throw new Error(`invalid attribute value: ${JSON.stringify(v)}`);
  }
  const obj = v as { [k: string]: unknown };
  const tags = Object.keys(obj);
  if (tags.length !== 1) {
    throw new Error(`attribute must have exactly one type tag, got ${tags.length}`);
  }
  const tag = tags[0];
  const val = obj[tag];
  switch (tag) {
    case 'S':
    case 'N':
      return String(val);
    case 'Bool':
      return Boolean(val);
    case 'B':
      return Buffer.from(val as number[]); // array of byte ints
    case 'Ss':
    case 'Ns':
      return (val as unknown[]).map(String);
    case 'Bs':
      return (val as number[][]).map((a) => Buffer.from(a));
    case 'M':
      return decodeItem(val as { [k: string]: unknown });
    case 'L':
      return (val as unknown[]).map(decodeAttr);
    default:
      throw new Error(`unknown attribute type tag: ${tag}`);
  }
}

export function decodeItem(item?: { [k: string]: unknown } | null): Item {
  const out: Item = {};
  if (!item) return out;
  for (const [k, v] of Object.entries(item)) out[k] = decodeAttr(v);
  return out;
}

// Record shape selector, set once at the Worker level.
//   'native'   (default) — decoded native values (decodeAttr)
//   'ddb_json'           — canonical DynamoDB JSON ({"S"|"N"|"BOOL"|"NULL"|
//                          "B"(base64)|"M"|"L"|"SS"|"NS"|"BS"}), the shape the
//                          AWS SDK consumes (SDK interop / KCL parity).
export type RecordFormat = 'native' | 'ddb_json';

// Converts one wire AttrValue into canonical DynamoDB JSON.
export function toDdbJson(v: unknown): AttrValue {
  if (v === 'Null') return { NULL: true } as unknown as AttrValue;
  if (typeof v !== 'object' || v === null || Array.isArray(v)) {
    throw new Error(`invalid attribute value: ${JSON.stringify(v)}`);
  }
  const obj = v as { [k: string]: unknown };
  const tags = Object.keys(obj);
  if (tags.length !== 1) {
    throw new Error(`attribute must have exactly one type tag, got ${tags.length}`);
  }
  const tag = tags[0];
  const val = obj[tag];
  switch (tag) {
    case 'S':
      return { S: String(val) } as unknown as AttrValue;
    case 'N':
      return { N: String(val) } as unknown as AttrValue;
    case 'Bool':
      return { BOOL: Boolean(val) } as unknown as AttrValue;
    case 'B':
      return { B: Buffer.from(val as number[]).toString('base64') } as unknown as AttrValue;
    case 'Ss':
      return { SS: (val as unknown[]).map(String) } as unknown as AttrValue;
    case 'Ns':
      return { NS: (val as unknown[]).map(String) } as unknown as AttrValue;
    case 'Bs':
      return {
        BS: (val as number[][]).map((a) => Buffer.from(a).toString('base64')),
      } as unknown as AttrValue;
    case 'M':
      return { M: ddbJsonItem(val as { [k: string]: unknown }) } as unknown as AttrValue;
    case 'L':
      return { L: (val as unknown[]).map(toDdbJson) } as unknown as AttrValue;
    default:
      throw new Error(`unknown attribute type tag: ${tag}`);
  }
}

export function ddbJsonItem(item?: { [k: string]: unknown } | null): Item {
  const out: Item = {};
  if (!item) return out;
  for (const [k, v] of Object.entries(item)) out[k] = toDdbJson(v);
  return out;
}

export function recordFromWire(shard: string, w: WireRecord, format: RecordFormat = 'native'): Record {
  const conv = format === 'ddb_json' ? ddbJsonItem : decodeItem;
  return {
    shardId: shard,
    eventName: w.event_name ?? null,
    sequenceNumber: w.sequence_number ?? null,
    streamViewType: w.stream_view_type ?? null,
    keys: conv(w.keys),
    newImage: w.new_image ? conv(w.new_image) : null,
    oldImage: w.old_image ? conv(w.old_image) : null,
  };
}

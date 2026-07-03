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

export function recordFromWire(shard: string, w: WireRecord): Record {
  return {
    shardId: shard,
    eventName: w.event_name ?? null,
    sequenceNumber: w.sequence_number ?? null,
    streamViewType: w.stream_view_type ?? null,
    keys: decodeItem(w.keys),
    newImage: w.new_image ? decodeItem(w.new_image) : null,
    oldImage: w.old_image ? decodeItem(w.old_image) : null,
  };
}

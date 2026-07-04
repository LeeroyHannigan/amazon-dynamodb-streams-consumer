'use strict';

const test = require('node:test');
const assert = require('node:assert');
const { decodeAttr, recordFromWire, toDdbJson } = require('../dist/record');

test('scalars', () => {
  assert.strictEqual(decodeAttr({ S: 'hi' }), 'hi');
  assert.strictEqual(decodeAttr({ N: '42' }), '42'); // numbers stay canonical strings
  assert.strictEqual(decodeAttr({ Bool: true }), true);
  assert.strictEqual(decodeAttr('Null'), null);
});

test('binary and sets', () => {
  assert.deepStrictEqual(decodeAttr({ B: [0, 1, 255] }), Buffer.from([0, 1, 255]));
  assert.deepStrictEqual(decodeAttr({ Ss: ['a', 'b'] }), ['a', 'b']);
  assert.deepStrictEqual(decodeAttr({ Ns: ['1', '2.5'] }), ['1', '2.5']);
  assert.deepStrictEqual(decodeAttr({ Bs: [[1, 2], [3]] }), [Buffer.from([1, 2]), Buffer.from([3])]);
});

test('nested map and list', () => {
  assert.deepStrictEqual(decodeAttr({ M: { x: { N: '1' }, y: { S: 'z' }, n: 'Null' } }), {
    x: '1',
    y: 'z',
    n: null,
  });
  assert.deepStrictEqual(decodeAttr({ L: [{ S: 'a' }, 'Null', { Bool: false }] }), ['a', null, false]);
});

test('record from wire', () => {
  const w = {
    event_name: 'MODIFY',
    sequence_number: '100',
    stream_view_type: 'NEW_AND_OLD_IMAGES',
    keys: { pk: { S: 'k1' } },
    new_image: { pk: { S: 'k1' }, active: { Bool: true } },
    old_image: null,
  };
  const r = recordFromWire('shardId-1', w);
  assert.strictEqual(r.shardId, 'shardId-1');
  assert.strictEqual(r.eventName, 'MODIFY');
  assert.strictEqual(r.sequenceNumber, '100');
  assert.deepStrictEqual(r.keys, { pk: 'k1' });
  assert.deepStrictEqual(r.newImage, { pk: 'k1', active: true });
  assert.strictEqual(r.oldImage, null);
});

test('ddb_json canonical shape', () => {
  assert.deepStrictEqual(toDdbJson({ S: 'k1' }), { S: 'k1' });
  assert.deepStrictEqual(toDdbJson({ N: '42' }), { N: '42' });
  assert.deepStrictEqual(toDdbJson({ Bool: true }), { BOOL: true });
  assert.deepStrictEqual(toDdbJson('Null'), { NULL: true });
  assert.deepStrictEqual(toDdbJson({ B: [1, 2, 3] }), { B: 'AQID' });
  assert.deepStrictEqual(toDdbJson({ Ss: ['a', 'b'] }), { SS: ['a', 'b'] });
  assert.deepStrictEqual(toDdbJson({ Ns: ['1', '2.5'] }), { NS: ['1', '2.5'] });
  assert.deepStrictEqual(toDdbJson({ Bs: [[9]] }), { BS: ['CQ=='] });
  assert.deepStrictEqual(toDdbJson({ M: { inner: { N: '7' } } }), { M: { inner: { N: '7' } } });
  assert.deepStrictEqual(toDdbJson({ L: [{ S: 'x' }, 'Null'] }), { L: [{ S: 'x' }, { NULL: true }] });
});

test('recordFromWire ddb_json format', () => {
  const wire = { keys: { pk: { S: 'k1' }, sk: { N: '42' } }, new_image: { a: { Bool: true } } };
  const r = recordFromWire('shard-1', wire, 'ddb_json');
  assert.deepStrictEqual(r.keys, { pk: { S: 'k1' }, sk: { N: '42' } });
  assert.deepStrictEqual(r.newImage, { a: { BOOL: true } });
  // native default unchanged
  const n = recordFromWire('shard-1', wire);
  assert.deepStrictEqual(n.keys, { pk: 'k1', sk: '42' });
});

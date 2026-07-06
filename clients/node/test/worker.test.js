'use strict';

// Worker message-dispatch tests using a tiny inline fake sidecar (no AWS, no
// real sidecar). Verifies that server messages are routed to the matching
// optional processor callbacks.

const test = require('node:test');
const assert = require('node:assert');
const { Worker } = require('../dist/index');

// A fake sidecar that emits the given newline-delimited JSON messages on
// stdout, then exits 0. Runs as `node -e <SRC>` with messages in argv.
const FAKE_SIDECAR_SRC =
  'const msgs = JSON.parse(process.argv[1]);' +
  "for (const m of msgs) process.stdout.write(JSON.stringify(m) + '\\n');" +
  'process.stdout.end(() => process.exit(0));';

function fakeSidecar(messages) {
  return ['node', '-e', FAKE_SIDECAR_SRC, JSON.stringify(messages)];
}

test('lease_lost dispatches to processor.leaseLost with the shard id', async () => {
  const lost = [];
  const w = new Worker({
    streamArn: 'arn:aws:dynamodb:us-east-1:1:table/T/stream/2026',
    leaseTable: 'leases',
    processor: {
      processRecords() {},
      leaseLost(shardId) {
        lost.push(shardId);
      },
    },
    sidecarCmd: fakeSidecar([
      { type: 'lease_lost', shard: 'shard-000000000001' },
      { type: 'shutdown' },
    ]),
  });

  const code = await w.run();
  assert.strictEqual(code, 0);
  assert.deepStrictEqual(lost, ['shard-000000000001']);
});

test('lease_lost is a no-op when the processor omits leaseLost', async () => {
  const w = new Worker({
    streamArn: 'arn:aws:dynamodb:us-east-1:1:table/T/stream/2026',
    leaseTable: 'leases',
    processor: {
      processRecords() {},
    },
    sidecarCmd: fakeSidecar([
      { type: 'lease_lost', shard: 'shard-000000000001' },
      { type: 'shutdown' },
    ]),
  });

  // Should not throw despite the optional callback being absent.
  const code = await w.run();
  assert.strictEqual(code, 0);
});

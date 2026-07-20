# Formal model — `max_processing_concurrency` (multiplexing)

A TLA+ model of the multiplexing scheduler, checked once with TLC. **Not part of
CI or the Cargo build** — it is a one-time correctness proof / design artifact.

## What it models

`Multiplexing.tla` abstracts `worker/src/fleet.rs`: a worker owns a set of
shards, each processed strictly in sequence; a shared pool of `Cap` slots (the
tokio `Semaphore` in `process_shard`) bounds concurrent delivery; completing a
record advances a durable per-shard checkpoint by one; a crash frees the slot
without advancing the checkpoint (so the record is redelivered — at-least-once).

## Properties proven

| Property | Meaning |
|---|---|
| `BoundOK` | never more than `Cap` shards processing at once (the cap always binds) |
| `CheckpointOK` | durable checkpoint stays in `0..MaxSeq` and only advances by +1 (no skip) |
| `AtLeastOnce` | `delivered[s] >= checkpoint[s]` — every checkpointed record was delivered ≥ once; crashes add duplicates, never loss |
| `Termination` | under fair (FIFO) scheduling every shard is eventually fully processed — no starvation, no permanent loss |

## How to run

Requires Java + `tla2tools.jar`
(https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar).

```
java -cp tla2tools.jar tlc2.TLC -deadlock -config Multiplexing.cfg Multiplexing.tla
```

`-deadlock` is required: the intended terminal state (every shard fully
processed, no slot in flight) has no successor, which TLC's default deadlock
check would otherwise flag. It is a valid end state, not a deadlock.

Model bounds (`Multiplexing.cfg`): `Shards = {s1,s2,s3}`, `Cap = 2`,
`MaxSeq = 2`, `MaxCrashes = 2`.

## Result (TLC 2.19, 2026-07-20)

```
Model checking completed. No error has been found.
3025 states generated, 1170 distinct states found, 0 states left on queue.
```

All safety invariants and the `Termination` liveness property hold across the
full 1,170-state space. Raising the bounds (more shards, larger `MaxSeq`/`Cap`,
more crashes) only enlarges the state space; the argument is unchanged.

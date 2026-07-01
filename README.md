# ddbstreams-kcl

A native, **JVM-free** Kinesis Client Library (KCL)–style consumer for **Amazon DynamoDB Streams**, delivering KCL semantics (shard discovery, leasing, checkpointing, fault-tolerant multi-worker coordination, and **ordering guarantees**) to non-Java languages.

## Why
The DynamoDB Streams Kinesis Adapter is Java-only. Non-Java consumers (e.g. Go) must hand-roll KCL's hardest logic on the raw SDK — most dangerously **parent-before-child ordering** across the ~4-hour shard rotations. This project fills that gap.

## Approach
One correctness-critical **Rust core** owns coordination + ordering; thin **per-language bindings** attach on top (Go first). Ordering is enforced in the core, so every binding inherits it.

- Single-owner-per-shard → in-sequence delivery per shard.
- **Parent-before-child** → a child shard is not processed until its parent reaches `SHARD_END` and is checkpointed.

## Layout
```
core/               Rust engine (coordination, ordering, checkpointing).
                    AWS behind StreamSource / LeaseStore traits.
bindings/           Per-language clients (Go first) — to be added.
```

## Status
Feasibility spike. The core ordering engine is implemented and **unit-tested with zero network** (parent-before-child + per-shard sequence order pass). Real `aws-sdk-dynamodbstreams` / `aws-sdk-dynamodb` adapters and the binding layer are next.

Design doc: *DynamoDB Streams KCL for Non-Java Languages (Go-first)* (internal Quip).

## Build
```
cd core && cargo test
```

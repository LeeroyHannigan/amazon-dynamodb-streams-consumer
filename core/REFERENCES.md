# REFERENCES — authoritative sources for robustness

This engine is **not** a naive reimplementation. Every correctness-critical behavior
is grounded in the authoritative Java sources we have access to. This file maps each
behavior to its source so reviewers can verify fidelity.

## Authoritative packages
| Package | Role | Version referenced |
|---|---|---|
| `awslabs/amazon-kinesis-client` (KCL v3) | Lease coordination, shard sync, checkpointing, lifecycle | 3.4.x (`v2.x`+ / internal `kclv3_prism`) |
| `awslabs/dynamodb-streams-kinesis-adapter` | DDB-Streams-specific shard detection, data fetch, sleep/catch-up, lease mgmt | 2.3.0 (KCL 3.4.3) |
| `AWSBifrostLeaseManager` (internal) | KCL lease-coordination *distillation* into a generic library — reference for extraction | mainline |
| `AmazonKinesisClientLibraryExternalRelease` (internal mirror) | Source of truth for KCL classes below | v2.x / kclv3_prism |

## DDB adapter classes to mirror (per `.../streamsadapter/`)
| Class | What we take from it |
|---|---|
| `DynamoDBStreamsShardDetector` | `DescribeStream` pagination → shard list; the `StreamSource.describe_shards` impl |
| `DynamoDBStreamsShardSyncer` (46KB) | Parent-before-child lease creation; parent-open-child-open inconsistency handling; lineage-replay-safe cleanup |
| `DynamoDBStreamsDataFetcher` | `GetShardIterator`/`GetRecords`; `Trimmed`/`ExpiredIterator`/`ResourceNotFound` handling; the `StreamSource.get_records` impl |
| `DynamoDBStreamsSleepTimeController` + `polling/` | Catch-up polling rate (`catchupEnabled`, `millisBehindLatestThreshold`, `scalingFactor`); recommended `MaxRecords=1000`, `IdleTimeInMillis=500` |
| `DynamoDBStreamsLeaseManagementFactory` | Lease table wiring specifics for DDB Streams |
| `StreamsSchedulerFactory` | Config surface (Checkpoint/Coordinator/Lease/Lifecycle/Metrics/Processor/Retrieval); single vs multi-stream tracker; enforces `DynamoDBStreamsShardRecordProcessor` + `DynamoDBStreamsPollingConfig` |

## Robustness behaviors → source mapping

### Ordering (the imperative requirement)
- **Parent-before-child**: a child lease is created/processed only after its parent reaches `SHARD_END`. Children initialized at `TRIM_HORIZON` to avoid gaps.
  - Source: KCL `HierarchicalShardSyncer`; lease lifecycle doc (`CREATION` only when eligible).
  - Encoded: `Scheduler.eligible()` + `merge_child_waits_for_both_parents` / `parent_before_child_ordering` tests.
- **Merge child requires BOTH parents**: a shard can have up to two parents; if only one parent lease is present (partial lineage), defer via `BlockedOnParentShardException`, with 1-in-N probability of dropping the lease so another worker retries.
  - Source: KCL `ShutdownTask.createLeasesForChildShardsIfNotExist`.
  - Encoded: `ShardMeta.parents: Vec<ShardId>` + `eligible()` requires ALL complete. **TODO:** port the partial-lineage defer/drop policy when we add real shard sync.
- **Per-shard in-sequence delivery + mandatory SHARD_END checkpoint** (unblocks children).
  - Source: lease lifecycle (`SHARD_END` → `DELETION`); KCL checkpointer.
  - Encoded: `Scheduler.run()` delivers in seq order, checkpoints per batch, checkpoints at shard end.

### Lease coordination (multi-worker, phase P2+)
- **Optimistic locking**: all lease mutations conditional on `leaseCounter` match.
- **Timing model**: renewer at `duration/3 - epsilon` (fixed rate); taker at `(duration + epsilon) * 2` (fixed delay).
- **Take order**: expired-first, then steal from most-loaded; **very-old leases** (> `3 * leaseDuration`) taken first; **stale-scan re-fetch** if a scan took > `renewerInterval * 0.5`.
- **Spurious-failure recovery**: on conditional-write failure, re-read the lease to detect a successful-but-reported-failed write.
- **Graceful handoff** (KCL 3.x): `checkpointOwner`/handoff-target protocol lets the current owner finish before transfer.
  - Source: KCL `DynamoDBLeaseTaker` / `DynamoDBLeaseRenewer` / `DynamoDBLeaseCoordinator`; distilled in `AWSBifrostLeaseManager` (`plan/03-DISTILLATION-MAP.md`, `references/kcl-distillation-deepdive.md`).

### Lineage-replay safety
- **Parent lease deleted only after child lease(s) enter PROCESSING** (tombstone → prevents replay of a completed shard's lineage).
  - Source: KCL `LeaseCleanupManager.cleanupLeaseForCompletedShard`.
  - **TODO:** encode ordering constraint in the `LeaseStore` cleanup contract.

### Bootstrap & shard sync at scale
- **Empty lease table bootstrap** uses `ShardFilter` (`AT_TRIM_HORIZON` / `AT_LATEST` / `AT_TIMESTAMP`) to create leases only for a snapshot of open shards.
- **Incremental sync** thereafter driven by `ChildShards` in `GetRecords` responses (avoids full re-enumeration); leader-only `PeriodicShardSyncManager`.
  - Source: KCL 2.3.0+ changelog; `PeriodicShardSyncManager`.
  - Edge case to guard: paginated shard enumeration can produce an **incomplete hash range** when the trim horizon advances mid-pagination (observed at 130K shards) — retry/validate hash-range completeness.

### DDB Streams specifics
- ~4-hour virtual shard rollover → frequent `SHARD_END` + child creation.
- 24h retention; polling only (no enhanced fan-out).
- `TrimmedDataAccessException` / `ExpiredIteratorException` / `ResourceNotFoundException` → restart shard at `TRIM_HORIZON`.
  - Source: `DynamoDBStreamsDataFetcher`; DDB Streams KCL runbook.

## Operational anti-patterns to avoid (from internal KCL patterns)
1. Use child **processes**, not threads, per partition (crash isolation) — matches our daemon+IPC design.
2. Stable lease-owner id (`taskARN:pid` / `podName` / `instanceId:pid`), never hostname.
3. Lease TTL ≥ 10s (15–30s for containers); never shorter than GC/throttle pauses.
4. Always use fencing tokens; never let a zombie holder write after lease loss.
5. SIGTERM handlers to release leases promptly.
6. Auto-scaling cooldown ≥ lease duration (avoid oscillation).

## Provenance
Sourced 2026-07-01 from the awslabs public repos and internal mirrors listed above.
Line-anchored KCL refs (subject to drift): `LeaseCleanupManager.java#L263-L294`,
`LeaseManagementConfig.java#L112-L128` (lease-cleanup config).

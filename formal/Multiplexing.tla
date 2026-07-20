---------------------------- MODULE Multiplexing ----------------------------
(***************************************************************************)
(* Formal model of `max_processing_concurrency` (multiplexing) in the      *)
(* amazon-dynamodb-streams-consumer worker.                                *)
(*                                                                         *)
(* Abstraction: a worker owns a set of Shards. Each shard is processed     *)
(* strictly in sequence (records 1..MaxSeq). A shared pool of `Cap`        *)
(* processing slots bounds how many shards deliver concurrently — this is  *)
(* exactly the tokio Semaphore in `process_shard`. Processing a record     *)
(* delivers it and advances a DURABLE per-shard checkpoint by one. A crash *)
(* while processing frees the slot WITHOUT advancing the checkpoint, so    *)
(* the record is redelivered later (at-least-once), never lost.            *)
(*                                                                         *)
(* Properties checked (see Multiplexing.cfg):                              *)
(*   BoundOK      - never more than Cap shards processing at once.         *)
(*   CheckpointOK - durable progress stays within 0..MaxSeq (never skips). *)
(*   AtLeastOnce  - every checkpointed record was delivered >= once.       *)
(*   Termination  - every shard is eventually fully processed (no          *)
(*                  starvation, no permanent loss) under fair scheduling.  *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Shards, Cap, MaxSeq, MaxCrashes

ASSUME CapOK     == Cap \in Nat /\ Cap >= 1
ASSUME MaxSeqOK  == MaxSeq \in Nat /\ MaxSeq >= 1
ASSUME CrashesOK == MaxCrashes \in Nat

VARIABLES
    inflight,    \* set of shards currently holding a processing slot
    checkpoint,  \* [Shards -> 0..MaxSeq] durable per-shard progress
    delivered,   \* [Shards -> Nat] total deliveries (may exceed checkpoint => dupes)
    crashes      \* number of crashes so far (bounded by MaxCrashes)

vars == <<inflight, checkpoint, delivered, crashes>>

TypeOK ==
    /\ inflight \subseteq Shards
    /\ checkpoint \in [Shards -> 0..MaxSeq]
    /\ delivered  \in [Shards -> 0..(MaxSeq + MaxCrashes)]
    /\ crashes    \in 0..MaxCrashes

Init ==
    /\ inflight   = {}
    /\ checkpoint = [s \in Shards |-> 0]
    /\ delivered  = [s \in Shards |-> 0]
    /\ crashes    = 0

(* Acquire a processing slot for shard s: not already processing, work left, *)
(* and a permit is free. `Cardinality(inflight) < Cap` is where the cap binds.*)
Acquire(s) ==
    /\ s \notin inflight
    /\ checkpoint[s] < MaxSeq
    /\ Cardinality(inflight) < Cap
    /\ inflight' = inflight \cup {s}
    /\ UNCHANGED <<checkpoint, delivered, crashes>>

(* Complete the in-flight record: deliver it and advance the durable          *)
(* checkpoint by exactly one, releasing the slot.                             *)
Complete(s) ==
    /\ s \in inflight
    /\ delivered'  = [delivered  EXCEPT ![s] = @ + 1]
    /\ checkpoint' = [checkpoint EXCEPT ![s] = @ + 1]
    /\ inflight'   = inflight \ {s}
    /\ UNCHANGED crashes

(* Crash while processing s: the record may have been delivered (at-least-once)*)
(* but the checkpoint is NOT advanced; the slot frees and the same seq is      *)
(* reprocessed later (possible duplicate, never loss).                         *)
Crash(s) ==
    /\ s \in inflight
    /\ crashes < MaxCrashes
    /\ delivered' = [delivered EXCEPT ![s] = @ + 1]
    /\ inflight'  = inflight \ {s}
    /\ crashes'   = crashes + 1
    /\ UNCHANGED checkpoint

Next ==
    \/ \E s \in Shards : Acquire(s)
    \/ \E s \in Shards : Complete(s)
    \/ \E s \in Shards : Crash(s)

(* Strong fairness on acquire + complete models the FIFO (fair) tokio         *)
(* Semaphore: a shard that can repeatedly obtain a slot eventually does, so    *)
(* no shard starves even when the cap is saturated by others.                  *)
Fairness ==
    /\ \A s \in Shards : SF_vars(Acquire(s))
    /\ \A s \in Shards : SF_vars(Complete(s))

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(* Safety *)
BoundOK      == Cardinality(inflight) <= Cap
CheckpointOK == \A s \in Shards : checkpoint[s] \in 0..MaxSeq
AtLeastOnce  == \A s \in Shards : delivered[s] >= checkpoint[s]

(* Liveness *)
Done        == \A s \in Shards : checkpoint[s] = MaxSeq
Termination == <>Done
=============================================================================

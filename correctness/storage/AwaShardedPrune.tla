---- MODULE AwaShardedPrune ----
EXTENDS TLC, Naturals, FiniteSets

\* Focused ADR-025 prune model.
\*
\* The full AwaSegmentedStorage model treats one `(queue, priority,
\* enqueue_shard)` lane as the unit under test. That is the right size for
\* lifecycle and receipt-plane safety, but it cannot expose a cross-shard
\* prune bug where two independent shards both have `lane_seq = 1`.
\*
\* This model isolates that surface:
\* - each shard has an independent `nextSeq`
\* - ready rows are keyed by `(shard, seq)`
\* - done rows are also keyed by `(shard, seq)`
\* - queue-ring prune may truncate a sealed ready/done segment only if every
\*   ready row in that segment has a matching done row on the SAME shard
\*
\* `Spec` uses the production predicate. `SpecBroken` models the historical
\* bug: the prune anti-join ignored `enqueue_shard`, so a done row from shard A
\* could satisfy a still-pending ready row from shard B when their `lane_seq`
\* values collided.

CONSTANTS Jobs,
          Shards,
          ReadySegmentCount,
          MaxSeq

ReadySegments == 1..ReadySegmentCount
SegmentStates == {"open", "sealed", "pruned"}

NoShard == "no_shard"
NoJob == "no_job"

VARIABLES readyRows,
          doneRows,
          readySegments,
          readySegmentCursor,
          nextSeq,
          lostPendingReady

vars == <<readyRows,
          doneRows,
          readySegments,
          readySegmentCursor,
          nextSeq,
          lostPendingReady>>

NextReadySegment(s) == IF s = ReadySegmentCount THEN 1 ELSE s + 1

ReadyRow(j, shard, seq, seg) ==
    [job |-> j, shard |-> shard, seq |-> seq, seg |-> seg]

DoneRowFromReady(r) ==
    [job |-> r.job, shard |-> r.shard, seq |-> r.seq, seg |-> r.seg]

RowsInSegment(rows, seg) == {r \in rows : r.seg = seg}

DoneMatchesShard(r) ==
    \E d \in doneRows : d.shard = r.shard /\ d.seq = r.seq

DoneMatchesIgnoringShard(r) ==
    \E d \in doneRows : d.seq = r.seq

PendingReadyInSegment(seg) ==
    \E r \in readyRows : r.seg = seg /\ ~DoneMatchesShard(r)

PendingReadyIgnoringShardInSegment(seg) ==
    \E r \in readyRows : r.seg = seg /\ ~DoneMatchesIgnoringShard(r)

InitSegments ==
    [s \in ReadySegments |-> IF s = 1 THEN "open" ELSE "pruned"]

Init ==
    /\ readyRows = {}
    /\ doneRows = {}
    /\ readySegments = InitSegments
    /\ readySegmentCursor = 1
    /\ nextSeq = [s \in Shards |-> 1]
    /\ lostPendingReady = FALSE

Enqueue(j, shard) ==
    /\ j \in Jobs
    /\ shard \in Shards
    /\ ~(\E r \in readyRows : r.job = j)
    /\ nextSeq[shard] <= MaxSeq
    /\ readySegments[readySegmentCursor] = "open"
    /\ readyRows' =
        readyRows \cup {ReadyRow(j, shard, nextSeq[shard], readySegmentCursor)}
    /\ nextSeq' = [nextSeq EXCEPT ![shard] = @ + 1]
    /\ UNCHANGED <<doneRows,
                   readySegments,
                   readySegmentCursor,
                   lostPendingReady>>

Complete(j) ==
    /\ j \in Jobs
    /\ \E r \in readyRows :
        /\ r.job = j
        /\ ~(\E d \in doneRows : d.job = j)
        /\ doneRows' = doneRows \cup {DoneRowFromReady(r)}
    /\ UNCHANGED <<readyRows,
                   readySegments,
                   readySegmentCursor,
                   nextSeq,
                   lostPendingReady>>

RotateReady ==
    LET next == NextReadySegment(readySegmentCursor) IN
    /\ readySegments[readySegmentCursor] = "open"
    /\ readySegments[next] = "pruned"
    /\ readySegments' = [readySegments EXCEPT
                           ![readySegmentCursor] = "sealed",
                           ![next] = "open"]
    /\ readySegmentCursor' = next
    /\ UNCHANGED <<readyRows, doneRows, nextSeq, lostPendingReady>>

PruneReadyCorrect(seg) ==
    /\ seg \in ReadySegments
    /\ readySegments[seg] = "sealed"
    /\ ~PendingReadyInSegment(seg)
    /\ readyRows' = readyRows \ RowsInSegment(readyRows, seg)
    /\ doneRows' = doneRows \ RowsInSegment(doneRows, seg)
    /\ readySegments' = [readySegments EXCEPT ![seg] = "pruned"]
    /\ UNCHANGED <<readySegmentCursor, nextSeq, lostPendingReady>>

PruneReadyBroken(seg) ==
    /\ seg \in ReadySegments
    /\ readySegments[seg] = "sealed"
    /\ ~PendingReadyIgnoringShardInSegment(seg)
    /\ readyRows' = readyRows \ RowsInSegment(readyRows, seg)
    /\ doneRows' = doneRows \ RowsInSegment(doneRows, seg)
    /\ readySegments' = [readySegments EXCEPT ![seg] = "pruned"]
    /\ lostPendingReady' = (lostPendingReady \/ PendingReadyInSegment(seg))
    /\ UNCHANGED <<readySegmentCursor, nextSeq>>

Stutter == UNCHANGED vars

Next ==
    \/ \E j \in Jobs, s \in Shards : Enqueue(j, s)
    \/ \E j \in Jobs : Complete(j)
    \/ RotateReady
    \/ \E seg \in ReadySegments : PruneReadyCorrect(seg)
    \/ Stutter

NextBroken ==
    \/ \E j \in Jobs, s \in Shards : Enqueue(j, s)
    \/ \E j \in Jobs : Complete(j)
    \/ RotateReady
    \/ \E seg \in ReadySegments : PruneReadyBroken(seg)
    \/ Stutter

Spec == Init /\ [][Next]_vars
SpecBroken == Init /\ [][NextBroken]_vars

TypeOK ==
    /\ Jobs # {}
    /\ Shards # {}
    /\ ReadySegmentCount >= 2
    /\ MaxSeq >= Cardinality(Jobs)
    /\ readyRows \subseteq [job: Jobs,
                            shard: Shards,
                            seq: 1..MaxSeq,
                            seg: ReadySegments]
    /\ doneRows \subseteq [job: Jobs,
                           shard: Shards,
                           seq: 1..MaxSeq,
                           seg: ReadySegments]
    /\ readySegments \in [ReadySegments -> SegmentStates]
    /\ readySegmentCursor \in ReadySegments
    /\ nextSeq \in [Shards -> 1..(MaxSeq + 1)]
    /\ lostPendingReady \in BOOLEAN

OneOpenReadySegment ==
    Cardinality({s \in ReadySegments : readySegments[s] = "open"}) = 1

ReadyCursorIsOpen ==
    readySegments[readySegmentCursor] = "open"

ReadyKeyUnique ==
    \A r1, r2 \in readyRows :
        (r1 # r2 /\ r1.shard = r2.shard) => r1.seq # r2.seq

DoneKeyUnique ==
    \A d1, d2 \in doneRows :
        (d1 # d2 /\ d1.shard = d2.shard) => d1.seq # d2.seq

DoneRowsComeFromReadyRows ==
    \A d \in doneRows :
        \E r \in readyRows :
            /\ r.job = d.job
            /\ r.shard = d.shard
            /\ r.seq = d.seq
            /\ r.seg = d.seg

NoPendingReadyDropped ==
    lostPendingReady = FALSE

=============================================================================

---- MODULE AwaSegmentedStorage ----
EXTENDS TLC, Naturals, FiniteSets

\* Focused storage model for the segmented 0.6+ runtime layout.
\*
\* It checks the relationships behind:
\* - ready_entries
\* - ready_claim_attempts (as a Claim transition refinement)
\* - deferred_jobs
\* - leases, including the waiting_external lease state
\* - attempt_state
\* - terminal_entries (the logical public terminal history exposed by
\*   done_entries and compact receipt completion batches)
\* - dlq_entries
\* - lane_state
\* - ready / lease / terminal segment families
\* - claim segment family (ADR-023/026): lease_claims +
\*   lease_claim_closures + lease_claim_closure_batches
\*   partitioned BY LIST (claim_slot), reclaimed by rotation + TRUNCATE
\*   instead of the earlier open_receipt_claims INSERT+DELETE frontier
\*
\* Heartbeat freshness is tracked on active_leases (not attempt_state) to
\* match the Rust implementation, where a claimed short job carries a
\* heartbeat timestamp on its lease row without ever materialising an
\* attempt_state record. This lets short-job rescue be reachable in the
\* model.
\*
\* Deferred and DLQ modelling: deferred_jobs and dlq_entries are unpartitioned
\* row-vacuum tables in the Rust implementation. The model tracks membership
\* and lifecycle transitions for those families, but does not model separate
\* deferred/DLQ segment rotation.
\*
\* DLQ modelling: both the executor-initiated FailToDlq path (terminal
\* failure with DLQ enabled) and the admin-initiated MoveFailedToDlq path
\* (awa dlq move) land rows into dlq_entries. RetryFromDlq re-appends a
\* fresh runnable entry with runLease reset to zero, matching the Rust contract.
\*
\* Terminal family modelling: terminal_entries is the durable public terminal
\* fact. Physically, legacy/non-success terminal rows live in done_entries,
\* while successful receipt completions can live in compact
\* receipt_completion_batches and expand through terminal_jobs. In both cases
\* immutable job-body fields are retained in ready_entries and hydrated by
\* joining on the ready segment + lane key while the queue-ring slot remains
\* visible. Queue-ring prune reclaims the retained ready body and terminal
\* fact together; MoveFailedToDlq removes from the terminal family before
\* re-appending to DLQ.
\*
\* Claim family modelling (ADR-023/026): every Claim action opens an
\* append-only receipt in the current claim segment. Non-success exits
\* (FailToDlq / RetryToDeferred / RescueToReady /
\* CancelWaitingToTerminal / TimeoutWaitingToDlq /
\* TimeoutWaitingToReady) write an explicit closure row in that same
\* segment. Successful compact receipt completion records the public
\* terminal fact in compact terminal history and separately appends compact
\* claim-local closure evidence in that same claim segment. ParkToWaiting
\* does NOT close the receipt:
\* waiting_external remains a row in `leases`, so the attempt is still
\* alive in callback wait and its receipt stays open.
\* ResumeWaitingToRunning also keeps the receipt open because the handler
\* continues the same run_lease. PruneClaimSegment requires every claim in the
\* sealed partition to be closed before TRUNCATE fires, and rescue can
\* force-close stragglers via RescueStaleReceipt. This replaces the
\* earlier open_receipt_claims INSERT+DELETE frontier: the entire
\* receipt plane is reclaimed by partition rotation.
\* The Rust implementation also keeps per-claim-slot rescue cursors
\* in claim_ring_slots. This model treats those cursors as implementation
\* refinements over claimOpen / claimClosed history rather than lifecycle
\* state. The stale-receipt cursor is a cyclic bounded sweep over
\* (claimed_at, job_id, run_lease): fresh claims may be skipped for this
\* pass and revisited after wrap, while stale open claims stop cursor
\* advancement until a rescue step closes them. The deadline cursor is a
\* separate bounded sweep over (deadline_at, job_id, run_lease): closed
\* and lease-managed claims are skipped, expired open receipt claims are
\* force-closed, and the first open future-deadline claim stops
\* advancement until it expires or closes through normal completion.
\* Both cursors refine the same storage facts modelled here: an open
\* claim is removed from claimOpen and matching durable closure evidence
\* is represented in claimClosed.
\* The Rust implementation also writes `ready_claim_attempts` in the
\* ready row's queue-ring slot during Claim. That table is a cursor-recovery
\* proof that the lane already emitted run_lease+1; in this lifecycle model
\* the same fact is represented by the `claimOpen' = claimOpen \cup ...`
\* transition in Claim. Queue prune reclaims that proof with readyEntries.
\*
\* Enqueue-shard modelling (ADR-025): `laneState` here represents one
\* `(queue, priority, enqueue_shard)` triple. The Rust implementation
\* allocates one shard row per triple, and shards within the same
\* (queue, priority) are independent: each shard owns its own
\* `appendSeq` / `claimSeq` counters, and producers pick a shard per
\* batch via the rotor in `shard_for_enqueue`. The invariants
\* `LaneStateMonotonic` (appendSeq strictly increases) and
\* `LaneStateClaimNeverExceedsAppend` (claimSeq <= appendSeq) therefore
\* apply per shard. Cross-shard ordering is not modeled here because
\* the receipt-plane and terminal-plane row keys include `enqueue_shard`
\* — at the model level, two shards form two disjoint copies of this
\* state machine sharing the same `Jobs` set. The cross-shard
\* invariant ("each enqueue transaction touches one shard's head
\* rows") is modeled in AwaStorageLockOrder.tla as
\* SingleShardPerTransaction.

CONSTANTS Jobs,
          Workers,
          ReadySegmentCount,
          LeaseSegmentCount,
          TerminalSegmentCount,
          ClaimSegmentCount,
          MaxRunLease,
          MaxAppendSeq

ReadySegments == 1..ReadySegmentCount
LeaseSegments == 1..LeaseSegmentCount
TerminalSegments == 1..TerminalSegmentCount
ClaimSegments == 1..ClaimSegmentCount
SegmentStates == {"open", "sealed", "pruned"}

\* Claim-receipt rows are identified by (job, run_lease) — each attempt
\* carries a distinct run_lease, so the bookkeeping for an old attempt's
\* receipt survives even after the same job has been re-claimed in a
\* newer partition. This is what lets the model reach race orderings
\* like "rescue closes the old partition's receipt *after* the next
\* claim has fired into a newer partition" without losing track of the
\* physical row in the old partition. The Rust implementation is atomic
\* in that ordering today (rescue_stale_receipt_claims_tx writes the
\* closure inside the same transaction that re-claims), but the spec
\* needs the resolution to model concurrent rescue / re-claim races
\* that the prior Jobs-keyed abstraction couldn't reach.
RunLeaseValues == 1..MaxRunLease
ClaimKeys == Jobs \X RunLeaseValues

NoWorker == "none"
NoReadySegment == 0
NoLeaseSegment == 0
NoTerminalSegment == 0
NoClaimSegment == 0
NoLaneSeq == 0

VARIABLES readyEntries,
          deferredEntries,
          waitingLeases,
          terminalEntries,
          dlqEntries,
          activeLeases,
          leaseOwner,
          runLease,
          taskLease,
          attemptState,
          heartbeatFresh,
          progressTouched,
          laneSeq,
          readySegmentOf,
          leaseSegmentOf,
          terminalSegmentOf,
          claimSegmentOf,
          claimOpen,
          claimClosed,
          readySegments,
          leaseSegments,
          terminalSegments,
          claimSegments,
          readySegmentCursor,
          leaseSegmentCursor,
          terminalSegmentCursor,
          claimSegmentCursor,
          laneState,
          readyTombstones

vars == <<readyEntries,
          deferredEntries,
          waitingLeases,
          terminalEntries,
          dlqEntries,
          activeLeases,
          leaseOwner,
          runLease,
          taskLease,
          attemptState,
          heartbeatFresh,
          progressTouched,
          laneSeq,
          readySegmentOf,
          leaseSegmentOf,
          terminalSegmentOf,
          claimSegmentOf,
          claimOpen,
          claimClosed,
          readySegments,
          leaseSegments,
          terminalSegments,
          claimSegments,
          readySegmentCursor,
          leaseSegmentCursor,
          terminalSegmentCursor,
          claimSegmentCursor,
          laneState,
          readyTombstones>>

NextReadySegment(s) == IF s = ReadySegmentCount THEN 1 ELSE s + 1
NextLeaseSegment(s) == IF s = LeaseSegmentCount THEN 1 ELSE s + 1
NextTerminalSegment(s) == IF s = TerminalSegmentCount THEN 1 ELSE s + 1
NextClaimSegment(s) == IF s = ClaimSegmentCount THEN 1 ELSE s + 1

ReceiptClaimJobs == {j \in Jobs : \E r \in RunLeaseValues : <<j, r>> \in claimOpen}

ReadyTombstone(j) == [
    job |-> j,
    segment |-> readySegmentOf[j],
    laneSeq |-> laneSeq[j]
]

CurrentReady ==
    {j \in readyEntries :
        /\ j \notin terminalEntries
        /\ j \notin activeLeases
        /\ j \notin ReceiptClaimJobs
        /\ j \notin deferredEntries
        /\ j \notin dlqEntries
        /\ ReadyTombstone(j) \notin readyTombstones
        /\ laneSeq[j] >= laneState.claimSeq}

JobsInReadySegment(seg) == {j \in Jobs : readySegmentOf[j] = seg}
JobsInTerminalSegment(seg) == {j \in Jobs : terminalSegmentOf[j] = seg}
KeysInClaimSegment(seg) == {k \in ClaimKeys : claimSegmentOf[k] = seg}
ClaimKeyJob(k) == k[1]

InitSegments(Range) == [s \in Range |-> IF s = 1 THEN "open" ELSE "pruned"]

Init ==
    /\ readyEntries = {}
    /\ deferredEntries = {}
    /\ waitingLeases = {}
    /\ terminalEntries = {}
    /\ dlqEntries = {}
    /\ activeLeases = {}
    /\ leaseOwner = [j \in Jobs |-> NoWorker]
    /\ runLease = [j \in Jobs |-> 0]
    /\ taskLease = [w \in Workers |-> [j \in Jobs |-> 0]]
    /\ attemptState = {}
    /\ heartbeatFresh = {}
    /\ progressTouched = {}
    /\ laneSeq = [j \in Jobs |-> NoLaneSeq]
    /\ readySegmentOf = [j \in Jobs |-> NoReadySegment]
    /\ leaseSegmentOf = [j \in Jobs |-> NoLeaseSegment]
    /\ terminalSegmentOf = [j \in Jobs |-> NoTerminalSegment]
    /\ claimSegmentOf = [k \in ClaimKeys |-> NoClaimSegment]
    /\ claimOpen = {}
    /\ claimClosed = {}
    /\ readySegments = InitSegments(ReadySegments)
    /\ leaseSegments = InitSegments(LeaseSegments)
    /\ terminalSegments = InitSegments(TerminalSegments)
    /\ claimSegments = InitSegments(ClaimSegments)
    /\ readySegmentCursor = 1
    /\ leaseSegmentCursor = 1
    /\ terminalSegmentCursor = 1
    /\ claimSegmentCursor = 1
    /\ laneState = [appendSeq |-> 1, claimSeq |-> 1, readyCount |-> 0, leasedCount |-> 0]
    /\ readyTombstones = {}

\* Segment-state variables for every family that are never touched by a
\* data-level action. Every data action unchanges this bundle.
UnchangedSegmentState ==
    UNCHANGED <<readySegments,
                leaseSegments,
                terminalSegments,
                claimSegments,
                readySegmentCursor,
                leaseSegmentCursor,
                terminalSegmentCursor,
                claimSegmentCursor>>

\* Receipt-plane data variables (claim-ring). Claim() opens a receipt.
\* Explicit non-success exits append closure rows in claimClosed. Successful
\* compact receipt completion appends terminal-history evidence that also
\* closes the claim.
UnchangedClaimData ==
    UNCHANGED <<claimSegmentOf, claimOpen, claimClosed>>

EnqueueReady(j) ==
    /\ j \in Jobs
    /\ runLease[j] = 0
    /\ j \notin readyEntries
    /\ j \notin deferredEntries
    /\ j \notin waitingLeases
    /\ j \notin terminalEntries
    /\ j \notin dlqEntries
    /\ laneState.appendSeq <= MaxAppendSeq
    /\ readySegments[readySegmentCursor] = "open"
    /\ readyEntries' = readyEntries \cup {j}
    /\ laneSeq' = [laneSeq EXCEPT ![j] = laneState.appendSeq]
    /\ readySegmentOf' = [readySegmentOf EXCEPT ![j] = readySegmentCursor]
    /\ laneState' = [laneState EXCEPT
                        !.appendSeq = laneState.appendSeq + 1,
                        !.readyCount = laneState.readyCount + 1]
    /\ UNCHANGED <<deferredEntries,
                   waitingLeases,
                   terminalEntries,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   progressTouched,
                   leaseSegmentOf,
                   terminalSegmentOf,
                   readyTombstones>>
    /\ UnchangedSegmentState
    /\ UnchangedClaimData

EnqueueDeferred(j) ==
    /\ j \in Jobs
    /\ runLease[j] = 0
    /\ j \notin readyEntries
    /\ j \notin deferredEntries
    /\ j \notin waitingLeases
    /\ j \notin terminalEntries
    /\ j \notin dlqEntries
    /\ deferredEntries' = deferredEntries \cup {j}
    /\ UNCHANGED <<readyEntries,
                   waitingLeases,
                   terminalEntries,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   leaseSegmentOf,
                   terminalSegmentOf,
                   laneState,
                   readyTombstones>>
    /\ UnchangedSegmentState
    /\ UnchangedClaimData

PromoteDeferred(j) ==
    /\ j \in deferredEntries
    /\ j \notin terminalEntries
    /\ laneState.appendSeq <= MaxAppendSeq
    /\ readySegments[readySegmentCursor] = "open"
    /\ deferredEntries' = deferredEntries \ {j}
    /\ readyEntries' = readyEntries \cup {j}
    /\ readySegmentOf' = [readySegmentOf EXCEPT ![j] = readySegmentCursor]
    /\ laneSeq' = [laneSeq EXCEPT ![j] = laneState.appendSeq]
    /\ laneState' = [laneState EXCEPT
                        !.appendSeq = laneState.appendSeq + 1,
                        !.readyCount = laneState.readyCount + 1]
    /\ UNCHANGED <<waitingLeases,
                   terminalEntries,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   progressTouched,
                   leaseSegmentOf,
                   terminalSegmentOf,
                   readyTombstones>>
    /\ UnchangedSegmentState
    /\ UnchangedClaimData

AdvanceClaimCursor ==
    /\ laneState.claimSeq < laneState.appendSeq
    /\ ~ (\E j \in CurrentReady : laneSeq[j] = laneState.claimSeq)
    /\ laneState' = [laneState EXCEPT !.claimSeq = laneState.claimSeq + 1]
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingLeases,
                   terminalEntries,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   leaseSegmentOf,
                   terminalSegmentOf,
                   readyTombstones>>
    /\ UnchangedSegmentState
    /\ UnchangedClaimData

\* Claim now also appends a claim receipt into the current claim segment.
\* The receipt lives in `claim_slot = claimSegmentCursor`; explicit
\* closure rows target the same slot, which is why
\* `claimSegmentOf[<<j, r>>]` is preserved across close until the whole
\* partition is pruned. Each
\* attempt has its own (j, run_lease) key so the previous attempt's
\* receipt bookkeeping is unaffected by the next claim.
Claim(w, j) ==
    LET newKey == <<j, runLease[j] + 1>>
    IN
    /\ w \in Workers
    /\ j \in CurrentReady
    /\ laneSeq[j] = laneState.claimSeq
    /\ runLease[j] < MaxRunLease
    /\ leaseSegments[leaseSegmentCursor] = "open"
    /\ claimSegments[claimSegmentCursor] = "open"
    /\ activeLeases' = activeLeases \cup {j}
    /\ leaseOwner' = [leaseOwner EXCEPT ![j] = w]
    /\ runLease' = [runLease EXCEPT ![j] = runLease[j] + 1]
    /\ taskLease' = [taskLease EXCEPT ![w][j] = runLease[j] + 1]
    /\ leaseSegmentOf' = [leaseSegmentOf EXCEPT ![j] = leaseSegmentCursor]
    /\ heartbeatFresh' = heartbeatFresh \cup {j}
    /\ claimSegmentOf' = [claimSegmentOf EXCEPT ![newKey] = claimSegmentCursor]
    /\ claimOpen' = claimOpen \cup {newKey}
    \* The previous attempt's closure bookkeeping (if any) lives under
    \* its own (j, old_run_lease) key in claimClosed and stays put;
    \* prune is what eventually removes it once the old partition seals
    \* and is reclaimed.
    /\ UNCHANGED claimClosed
    /\ laneState' = [laneState EXCEPT
                        !.claimSeq = laneState.claimSeq + 1,
                        !.readyCount = laneState.readyCount - 1,
                        !.leasedCount = laneState.leasedCount + 1]
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingLeases,
                   terminalEntries,
                   dlqEntries,
                   attemptState,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   terminalSegmentOf,
                   readyTombstones>>
    /\ UnchangedSegmentState

MaterializeAttemptState(j) ==
    /\ j \in activeLeases
    /\ j \notin attemptState
    /\ attemptState' = attemptState \cup {j}
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingLeases,
                   terminalEntries,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   heartbeatFresh,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   leaseSegmentOf,
                   terminalSegmentOf,
                   laneState,
                   readyTombstones>>
    /\ UnchangedSegmentState
    /\ UnchangedClaimData

Heartbeat(j) ==
    /\ j \in activeLeases
    /\ j \notin waitingLeases
    /\ heartbeatFresh' = heartbeatFresh \cup {j}
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingLeases,
                   terminalEntries,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   leaseSegmentOf,
                   terminalSegmentOf,
                   laneState,
                   readyTombstones>>
    /\ UnchangedSegmentState
    /\ UnchangedClaimData

LoseHeartbeat(j) ==
    /\ j \in activeLeases
    /\ j \in heartbeatFresh
    /\ heartbeatFresh' = heartbeatFresh \ {j}
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingLeases,
                   terminalEntries,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   leaseSegmentOf,
                   terminalSegmentOf,
                   laneState,
                   readyTombstones>>
    /\ UnchangedSegmentState
    /\ UnchangedClaimData

ProgressFlush(j) ==
    /\ j \in attemptState
    /\ progressTouched' = progressTouched \cup {j}
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingLeases,
                   terminalEntries,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   laneSeq,
                   readySegmentOf,
                   leaseSegmentOf,
                   terminalSegmentOf,
                   laneState,
                   readyTombstones>>
    /\ UnchangedSegmentState
    /\ UnchangedClaimData

\* Parking to callback wait does NOT close the receipt: the attempt is
\* still alive, the claim is still the authoritative claim for this
\* run_lease, and the waiting lifecycle will either resume, timeout, or
\* cancel — each of which does close the receipt.
ParkToWaiting(w, j) ==
    /\ w \in Workers
    /\ j \in activeLeases
    /\ j \in attemptState
    /\ leaseOwner[j] = w
    /\ taskLease[w][j] = runLease[j]
    /\ waitingLeases' = waitingLeases \cup {j}
    /\ heartbeatFresh' = heartbeatFresh \ {j}
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   terminalEntries,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   taskLease,
                   runLease,
                   attemptState,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   leaseSegmentOf,
                   laneState,
                   terminalSegmentOf,
                   readyTombstones>>
    /\ UnchangedSegmentState
    /\ UnchangedClaimData

\* Callback resume clears waiting_external and returns the same lease row
\* to running. It does not close the receipt or append a new ready row.
ResumeWaitingToRunning(j) ==
    /\ j \in waitingLeases
    /\ j \in attemptState
    /\ waitingLeases' = waitingLeases \ {j}
    /\ heartbeatFresh' = heartbeatFresh \cup {j}
    /\ UNCHANGED <<deferredEntries,
                   readyEntries,
                   terminalEntries,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   leaseSegmentOf,
                   terminalSegmentOf,
                   laneState,
                   readyTombstones>>
    /\ UnchangedSegmentState
    /\ UnchangedClaimData

\* Callback timeout with attempts remaining: waiting -> ready and close
\* the old receipt (the retry will open a new one on its next Claim).
TimeoutWaitingToReady(j) ==
    /\ j \in waitingLeases
    /\ j \in attemptState
    /\ laneState.appendSeq <= MaxAppendSeq
    /\ readySegments[readySegmentCursor] = "open"
    /\ activeLeases' = activeLeases \ {j}
    /\ waitingLeases' = waitingLeases \ {j}
    /\ readyEntries' = readyEntries \cup {j}
    /\ leaseOwner' = [leaseOwner EXCEPT ![j] = NoWorker]
    /\ taskLease' = [w \in Workers |-> [taskLease[w] EXCEPT ![j] = 0]]
    /\ attemptState' = attemptState \ {j}
    /\ progressTouched' = progressTouched \ {j}
    /\ heartbeatFresh' = heartbeatFresh \ {j}
    /\ readySegmentOf' = [readySegmentOf EXCEPT ![j] = readySegmentCursor]
    /\ laneSeq' = [laneSeq EXCEPT ![j] = laneState.appendSeq]
    /\ leaseSegmentOf' = [leaseSegmentOf EXCEPT ![j] = NoLeaseSegment]
    /\ claimOpen' = claimOpen \ {<<j, runLease[j]>>}
    /\ claimClosed' = claimClosed \cup {<<j, runLease[j]>>}
    /\ laneState' = [laneState EXCEPT
                        !.appendSeq = laneState.appendSeq + 1,
                        !.readyCount = laneState.readyCount + 1,
                        !.leasedCount = laneState.leasedCount - 1]
    /\ UNCHANGED <<deferredEntries,
                   terminalEntries,
                   dlqEntries,
                   runLease,
                   terminalSegmentOf,
                   claimSegmentOf,
                   readyTombstones>>
    /\ UnchangedSegmentState

\* Callback-timeout rescue on exhausted attempts lands a waiting entry
\* directly into the DLQ rather than re-enqueuing for another attempt.
TimeoutWaitingToDlq(j) ==
    /\ j \in waitingLeases
    /\ j \in attemptState
    /\ activeLeases' = activeLeases \ {j}
    /\ waitingLeases' = waitingLeases \ {j}
    /\ dlqEntries' = dlqEntries \cup {j}
    /\ leaseOwner' = [leaseOwner EXCEPT ![j] = NoWorker]
    /\ taskLease' = [w \in Workers |-> [taskLease[w] EXCEPT ![j] = 0]]
    /\ attemptState' = attemptState \ {j}
    /\ progressTouched' = progressTouched \ {j}
    /\ heartbeatFresh' = heartbeatFresh \ {j}
    /\ leaseSegmentOf' = [leaseSegmentOf EXCEPT ![j] = NoLeaseSegment]
    /\ claimOpen' = claimOpen \ {<<j, runLease[j]>>}
    /\ claimClosed' = claimClosed \cup {<<j, runLease[j]>>}
    /\ laneState' = [laneState EXCEPT !.leasedCount = laneState.leasedCount - 1]
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   terminalEntries,
                   runLease,
                   laneSeq,
                   readySegmentOf,
                   terminalSegmentOf,
                   claimSegmentOf,
                   readyTombstones>>
    /\ UnchangedSegmentState

FastComplete(w, j) ==
    /\ w \in Workers
    /\ j \in activeLeases
    /\ j \notin waitingLeases
    /\ leaseOwner[j] = w
    /\ taskLease[w][j] = runLease[j]
    /\ j \notin attemptState
    /\ terminalSegments[terminalSegmentCursor] = "open"
    /\ terminalEntries' = terminalEntries \cup {j}
    /\ terminalSegmentOf' = [terminalSegmentOf EXCEPT ![j] = terminalSegmentCursor]
    /\ activeLeases' = activeLeases \ {j}
    /\ leaseOwner' = [leaseOwner EXCEPT ![j] = NoWorker]
    /\ taskLease' = [taskLease EXCEPT ![w][j] = 0]
    /\ heartbeatFresh' = heartbeatFresh \ {j}
    /\ leaseSegmentOf' = [leaseSegmentOf EXCEPT ![j] = NoLeaseSegment]
    /\ claimOpen' = claimOpen \ {<<j, runLease[j]>>}
    /\ claimClosed' = claimClosed \cup {<<j, runLease[j]>>}
    /\ laneState' = [laneState EXCEPT !.leasedCount = laneState.leasedCount - 1]
    /\ UNCHANGED <<deferredEntries,
                   waitingLeases,
                   dlqEntries,
                   runLease,
                   attemptState,
                   progressTouched,
                   readyEntries,
                   laneSeq,
                   readySegmentOf,
                   claimSegmentOf,
                   readyTombstones>>
    /\ UnchangedSegmentState

StatefulComplete(w, j) ==
    /\ w \in Workers
    /\ j \in activeLeases
    /\ j \notin waitingLeases
    /\ leaseOwner[j] = w
    /\ taskLease[w][j] = runLease[j]
    /\ j \in attemptState
    /\ terminalSegments[terminalSegmentCursor] = "open"
    /\ terminalEntries' = terminalEntries \cup {j}
    /\ terminalSegmentOf' = [terminalSegmentOf EXCEPT ![j] = terminalSegmentCursor]
    /\ activeLeases' = activeLeases \ {j}
    /\ leaseOwner' = [leaseOwner EXCEPT ![j] = NoWorker]
    /\ taskLease' = [taskLease EXCEPT ![w][j] = 0]
    /\ attemptState' = attemptState \ {j}
    /\ heartbeatFresh' = heartbeatFresh \ {j}
    /\ progressTouched' = progressTouched \ {j}
    /\ leaseSegmentOf' = [leaseSegmentOf EXCEPT ![j] = NoLeaseSegment]
    /\ claimOpen' = claimOpen \ {<<j, runLease[j]>>}
    /\ claimClosed' = claimClosed \cup {<<j, runLease[j]>>}
    /\ laneState' = [laneState EXCEPT !.leasedCount = laneState.leasedCount - 1]
    /\ UNCHANGED <<deferredEntries,
                   waitingLeases,
                   dlqEntries,
                   runLease,
                   readyEntries,
                   laneSeq,
                   readySegmentOf,
                   claimSegmentOf,
                   readyTombstones>>
    /\ UnchangedSegmentState

\* Executor-side terminal failure with DLQ enabled. The attempt's lease
\* and attempt_state are cleared and the job is appended to the DLQ
\* instead of terminal_entries.
FailToDlq(w, j) ==
    /\ w \in Workers
    /\ j \in activeLeases
    /\ j \notin waitingLeases
    /\ leaseOwner[j] = w
    /\ taskLease[w][j] = runLease[j]
    /\ activeLeases' = activeLeases \ {j}
    /\ dlqEntries' = dlqEntries \cup {j}
    /\ leaseOwner' = [leaseOwner EXCEPT ![j] = NoWorker]
    /\ taskLease' = [taskLease EXCEPT ![w][j] = 0]
    /\ attemptState' = attemptState \ {j}
    /\ heartbeatFresh' = heartbeatFresh \ {j}
    /\ progressTouched' = progressTouched \ {j}
    /\ leaseSegmentOf' = [leaseSegmentOf EXCEPT ![j] = NoLeaseSegment]
    /\ claimOpen' = claimOpen \ {<<j, runLease[j]>>}
    /\ claimClosed' = claimClosed \cup {<<j, runLease[j]>>}
    /\ laneState' = [laneState EXCEPT !.leasedCount = laneState.leasedCount - 1]
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingLeases,
                   terminalEntries,
                   runLease,
                   laneSeq,
                   readySegmentOf,
                   terminalSegmentOf,
                   claimSegmentOf,
                   readyTombstones>>
    /\ UnchangedSegmentState

RetryToDeferred(w, j) ==
    /\ w \in Workers
    /\ j \in activeLeases
    /\ j \notin waitingLeases
    /\ leaseOwner[j] = w
    /\ taskLease[w][j] = runLease[j]
    /\ activeLeases' = activeLeases \ {j}
    /\ deferredEntries' = deferredEntries \cup {j}
    /\ leaseOwner' = [leaseOwner EXCEPT ![j] = NoWorker]
    /\ taskLease' = [taskLease EXCEPT ![w][j] = 0]
    /\ attemptState' = attemptState \ {j}
    /\ heartbeatFresh' = heartbeatFresh \ {j}
    /\ progressTouched' = progressTouched \ {j}
    /\ leaseSegmentOf' = [leaseSegmentOf EXCEPT ![j] = NoLeaseSegment]
    /\ claimOpen' = claimOpen \ {<<j, runLease[j]>>}
    /\ claimClosed' = claimClosed \cup {<<j, runLease[j]>>}
    /\ laneState' = [laneState EXCEPT !.leasedCount = laneState.leasedCount - 1]
    /\ UNCHANGED <<readyEntries,
                   waitingLeases,
                   terminalEntries,
                   dlqEntries,
                   runLease,
                   laneSeq,
                   readySegmentOf,
                   terminalSegmentOf,
                   claimSegmentOf,
                   readyTombstones>>
    /\ UnchangedSegmentState

\* Heartbeat or deadline rescue. No longer requires attempt_state, so
\* short jobs that claimed and immediately lost their heartbeat (e.g.
\* worker crash before the first heartbeat publish) are reachable here.
\* Rescue closes the old run_lease's receipt; the next Claim will open a
\* fresh receipt in the current claim segment.
RescueToReady(j) ==
    /\ j \in activeLeases
    /\ j \notin waitingLeases
    /\ j \notin heartbeatFresh
    /\ laneState.appendSeq <= MaxAppendSeq
    /\ readySegments[readySegmentCursor] = "open"
    /\ activeLeases' = activeLeases \ {j}
    /\ readyEntries' = readyEntries \cup {j}
    /\ leaseOwner' = [leaseOwner EXCEPT ![j] = NoWorker]
    /\ attemptState' = attemptState \ {j}
    /\ progressTouched' = progressTouched \ {j}
    /\ laneSeq' = [laneSeq EXCEPT ![j] = laneState.appendSeq]
    /\ readySegmentOf' = [readySegmentOf EXCEPT ![j] = readySegmentCursor]
    /\ leaseSegmentOf' = [leaseSegmentOf EXCEPT ![j] = NoLeaseSegment]
    /\ claimOpen' = claimOpen \ {<<j, runLease[j]>>}
    /\ claimClosed' = claimClosed \cup {<<j, runLease[j]>>}
    /\ laneState' = [laneState EXCEPT
                        !.appendSeq = laneState.appendSeq + 1,
                        !.readyCount = laneState.readyCount + 1,
                        !.leasedCount = laneState.leasedCount - 1]
    /\ UNCHANGED <<deferredEntries,
                   waitingLeases,
                   terminalEntries,
                   dlqEntries,
                   runLease,
                   taskLease,
                   heartbeatFresh,
                   terminalSegmentOf,
                   claimSegmentOf,
                   readyTombstones>>
    /\ UnchangedSegmentState

\* ADR-023 Tier-A rescue: force-close a straggling receipt whose
\* attempt is no longer alive on this (j, run_lease). The action takes
\* the explicit key so the spec can model rescue firing on an old
\* attempt's receipt *concurrently with* a newer Claim having already
\* opened a fresh receipt under (j, runLease[j]). Either of two
\* preconditions makes the targeted receipt dead:
\*   - `runLease[j] > r` — the job has already moved on to a newer
\*     attempt, so the receipt under `r` is definitely abandoned.
\*   - the job is fully off the ready / leased / waiting lifecycle —
\*     no live runtime is processing this attempt anywhere.
\* This models the rescue-before-truncate precondition that
\* `prune_oldest_claims` takes before calling TRUNCATE.
RescueStaleReceipt(j, r) ==
    /\ <<j, r>> \in claimOpen
    /\ \/ runLease[j] > r
       \/ /\ j \notin activeLeases
          /\ j \notin waitingLeases
    /\ claimOpen' = claimOpen \ {<<j, r>>}
    /\ claimClosed' = claimClosed \cup {<<j, r>>}
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingLeases,
                   terminalEntries,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   leaseSegmentOf,
                   terminalSegmentOf,
                   claimSegmentOf,
                   laneState,
                   readyTombstones>>
    /\ UnchangedSegmentState

CancelWaitingToTerminal(j) ==
    /\ j \in waitingLeases
    /\ terminalSegments[terminalSegmentCursor] = "open"
    /\ terminalEntries' = terminalEntries \cup {j}
    /\ terminalSegmentOf' = [terminalSegmentOf EXCEPT ![j] = terminalSegmentCursor]
    /\ activeLeases' = activeLeases \ {j}
    /\ waitingLeases' = waitingLeases \ {j}
    /\ leaseOwner' = [leaseOwner EXCEPT ![j] = NoWorker]
    /\ taskLease' = [w \in Workers |-> [taskLease[w] EXCEPT ![j] = 0]]
    /\ attemptState' = attemptState \ {j}
    /\ heartbeatFresh' = heartbeatFresh \ {j}
    /\ progressTouched' = progressTouched \ {j}
    /\ leaseSegmentOf' = [leaseSegmentOf EXCEPT ![j] = NoLeaseSegment]
    /\ claimOpen' = claimOpen \ {<<j, runLease[j]>>}
    /\ claimClosed' = claimClosed \cup {<<j, runLease[j]>>}
    /\ laneState' = [laneState EXCEPT !.leasedCount = laneState.leasedCount - 1]
    /\ UNCHANGED <<deferredEntries,
                   dlqEntries,
                   runLease,
                   readyEntries,
                   laneSeq,
                   readySegmentOf,
                   claimSegmentOf,
                   readyTombstones>>
    /\ UnchangedSegmentState

\* Admin cancel of a running (lease-backed) job. Mirrors
\* `cancel_job_tx`'s lease branch in awa-model/src/queue_storage.rs:
\* DELETE FROM leases ... RETURNING, INSERT INTO done_entries (state =
\* cancelled), close_receipt_tx (writes the closure into the matching
\* claim partition), pg_notify('awa:cancel', ...). Equivalent to
\* StatefulComplete on the runtime side but routes to terminal with
\* the cancelled label rather than completed; the receipt closure is
\* identical so the claim-partition prune precondition still holds.
CancelRunningToTerminal(j) ==
    /\ j \in activeLeases
    /\ j \notin waitingLeases
    /\ terminalSegments[terminalSegmentCursor] = "open"
    /\ terminalEntries' = terminalEntries \cup {j}
    /\ terminalSegmentOf' = [terminalSegmentOf EXCEPT ![j] = terminalSegmentCursor]
    /\ activeLeases' = activeLeases \ {j}
    /\ leaseOwner' = [leaseOwner EXCEPT ![j] = NoWorker]
    \* Cancel has no worker context (admin path), so clear every
    \* worker's taskLease snapshot for this job. Worker-driven
    \* terminal transitions clear only their own; here we keep
    \* TaskLeaseBounded by zeroing all of them, mirroring the runtime
    \* behaviour where the pg_notify wakes whichever local worker (if
    \* any) was running this attempt and the StaleCompleteRejected
    \* path discards any subsequent completion that does come back.
    /\ taskLease' = [w \in Workers |-> [taskLease[w] EXCEPT ![j] = 0]]
    /\ attemptState' = attemptState \ {j}
    /\ heartbeatFresh' = heartbeatFresh \ {j}
    /\ progressTouched' = progressTouched \ {j}
    /\ leaseSegmentOf' = [leaseSegmentOf EXCEPT ![j] = NoLeaseSegment]
    /\ claimOpen' = claimOpen \ {<<j, runLease[j]>>}
    /\ claimClosed' = claimClosed \cup {<<j, runLease[j]>>}
    /\ laneState' = [laneState EXCEPT !.leasedCount = laneState.leasedCount - 1]
    /\ UNCHANGED <<deferredEntries,
                   waitingLeases,
                   dlqEntries,
                   runLease,
                   readyEntries,
                   laneSeq,
                   readySegmentOf,
                   claimSegmentOf,
                   readyTombstones>>
    /\ UnchangedSegmentState

\* Admin cancel of a job whose attempt is running on the receipt-only
\* short path (open claim, no `leases` row materialized). Mirrors the
\* receipt-only branch in `cancel_job_tx`: SELECT FROM lease_claims
\* FOR UPDATE, INSERT done row, INSERT closure, defensive DELETE FROM
\* leases (defensively sweeping any concurrent materialization), pg_notify.
\* Distinct from CancelRunningToTerminal because no lease ever existed,
\* so activeLeases is unchanged and laneState's leasedCount stays put.
CancelReceiptOnlyToTerminal(j) ==
    /\ <<j, runLease[j]>> \in claimOpen
    /\ j \notin activeLeases
    /\ j \notin waitingLeases
    /\ terminalSegments[terminalSegmentCursor] = "open"
    /\ terminalEntries' = terminalEntries \cup {j}
    /\ terminalSegmentOf' = [terminalSegmentOf EXCEPT ![j] = terminalSegmentCursor]
    \* Same rationale as CancelRunningToTerminal: zero every worker's
    \* taskLease snapshot since admin cancel has no worker context.
    /\ taskLease' = [w \in Workers |-> [taskLease[w] EXCEPT ![j] = 0]]
    /\ claimOpen' = claimOpen \ {<<j, runLease[j]>>}
    /\ claimClosed' = claimClosed \cup {<<j, runLease[j]>>}
    /\ UNCHANGED <<deferredEntries,
                   waitingLeases,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   attemptState,
                   heartbeatFresh,
                   progressTouched,
                   readyEntries,
                   laneSeq,
                   readySegmentOf,
                   leaseSegmentOf,
                   claimSegmentOf,
                   laneState,
                   readyTombstones>>
    /\ UnchangedSegmentState

CancelReadyToTerminal(j) ==
    /\ j \in CurrentReady
    /\ j \notin activeLeases
    /\ j \notin waitingLeases
    /\ j \notin terminalEntries
    /\ j \notin deferredEntries
    /\ j \notin dlqEntries
    /\ terminalSegments[terminalSegmentCursor] = "open"
    /\ terminalEntries' = terminalEntries \cup {j}
    /\ terminalSegmentOf' = [terminalSegmentOf EXCEPT ![j] = terminalSegmentCursor]
    /\ readyTombstones' = readyTombstones \cup {ReadyTombstone(j)}
    /\ laneState' = [laneState EXCEPT !.readyCount = laneState.readyCount - 1]
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingLeases,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   leaseSegmentOf,
                   claimSegmentOf,
                   claimOpen,
                   claimClosed>>
    /\ UnchangedSegmentState

ReprioritizeReady(j) ==
    /\ j \in CurrentReady
    /\ j \notin activeLeases
    /\ j \notin waitingLeases
    /\ j \notin terminalEntries
    /\ j \notin deferredEntries
    /\ j \notin dlqEntries
    /\ laneState.appendSeq <= MaxAppendSeq
    /\ readySegments[readySegmentCursor] = "open"
    /\ readyTombstones' = readyTombstones \cup {ReadyTombstone(j)}
    /\ laneSeq' = [laneSeq EXCEPT ![j] = laneState.appendSeq]
    /\ readySegmentOf' = [readySegmentOf EXCEPT ![j] = readySegmentCursor]
    /\ laneState' = [laneState EXCEPT
                        !.appendSeq = laneState.appendSeq + 1]
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingLeases,
                   terminalEntries,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   progressTouched,
                   leaseSegmentOf,
                   terminalSegmentOf,
                   claimSegmentOf,
                   claimOpen,
                   claimClosed>>
    /\ UnchangedSegmentState

\* Batch move_queue is the same abstract storage transition as
\* reprioritize in this one-lane model: tombstone the source ready lane
\* and append a replacement ready row with a fresh lane sequence. The
\* production implementation may also rewrite the queue label; queue labels
\* are outside this focused lifecycle model.
MoveQueueReady(j) == ReprioritizeReady(j)

StaleCompleteRejected(w, j) ==
    /\ w \in Workers
    /\ j \in Jobs
    /\ taskLease[w][j] # 0
    /\ j \notin activeLeases \/ taskLease[w][j] # runLease[j]
    /\ taskLease' = [taskLease EXCEPT ![w][j] = 0]
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingLeases,
                   terminalEntries,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   attemptState,
                   heartbeatFresh,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   leaseSegmentOf,
                   terminalSegmentOf,
                   laneState,
                   readyTombstones>>
    /\ UnchangedSegmentState
    /\ UnchangedClaimData

\* Admin-initiated move of a terminal row into the DLQ. Models the
\* `awa dlq move` command path. Clears the terminalSegmentOf pointer so
\* the source terminal segment can eventually be pruned.
MoveFailedToDlq(j) ==
    /\ j \in terminalEntries
    /\ j \notin dlqEntries
    /\ terminalEntries' = terminalEntries \ {j}
    /\ terminalSegmentOf' = [terminalSegmentOf EXCEPT ![j] = NoTerminalSegment]
    /\ dlqEntries' = dlqEntries \cup {j}
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingLeases,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   leaseSegmentOf,
                   laneState,
                   readyTombstones>>
    /\ UnchangedSegmentState
    /\ UnchangedClaimData

\* Retry a DLQ row back into live queue storage. Rust resets run_lease
\* because the new claim row lands in a fresh claim partition. The model
\* abstracts claim_slot away from the receipt key, so it preserves
\* runLease[j] and lets the next claim use runLease[j] + 1 instead of
\* colliding with an old (j, run_lease) key still tracked in claimClosed.
RetryFromDlq(j) ==
    /\ j \in dlqEntries
    /\ laneState.appendSeq <= MaxAppendSeq
    /\ readySegments[readySegmentCursor] = "open"
    /\ dlqEntries' = dlqEntries \ {j}
    /\ readyEntries' = readyEntries \cup {j}
    /\ laneSeq' = [laneSeq EXCEPT ![j] = laneState.appendSeq]
    /\ readySegmentOf' = [readySegmentOf EXCEPT ![j] = readySegmentCursor]
    /\ laneState' = [laneState EXCEPT
                        !.appendSeq = laneState.appendSeq + 1,
                        !.readyCount = laneState.readyCount + 1]
    /\ UNCHANGED <<deferredEntries,
                   waitingLeases,
                   terminalEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   progressTouched,
                   leaseSegmentOf,
                   terminalSegmentOf,
                   readyTombstones>>
    /\ UnchangedSegmentState
    /\ UnchangedClaimData

\* behaviour where the row is deleted but its segment partition only
\* rotates out once all live rows have drained.
PurgeDlq(j) ==
    /\ j \in dlqEntries
    /\ dlqEntries' = dlqEntries \ {j}
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingLeases,
                   terminalEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   leaseSegmentOf,
                   terminalSegmentOf,
                   laneState,
                   readyTombstones>>
    /\ UnchangedSegmentState
    /\ UnchangedClaimData

RotateReadySegments ==
    LET next == NextReadySegment(readySegmentCursor) IN
    /\ readySegments[readySegmentCursor] = "open"
    /\ readySegments[next] = "pruned"
    /\ readySegments' = [readySegments EXCEPT
                           ![readySegmentCursor] = "sealed",
                           ![next] = "open"]
    /\ readySegmentCursor' = next
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingLeases,
                   terminalEntries,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   leaseSegmentOf,
                   terminalSegmentOf,
                   leaseSegments,
                   terminalSegments,
                   claimSegments,
                   leaseSegmentCursor,
                   terminalSegmentCursor,
                   claimSegmentCursor,
                   laneState,
                   readyTombstones>>
    /\ UnchangedClaimData

RotateLeaseSegments ==
    LET next == NextLeaseSegment(leaseSegmentCursor) IN
    /\ leaseSegments[leaseSegmentCursor] = "open"
    /\ leaseSegments[next] = "pruned"
    /\ leaseSegments' = [leaseSegments EXCEPT
                           ![leaseSegmentCursor] = "sealed",
                           ![next] = "open"]
    /\ leaseSegmentCursor' = next
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingLeases,
                   terminalEntries,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   leaseSegmentOf,
                   terminalSegmentOf,
                   readySegments,
                   terminalSegments,
                   claimSegments,
                   readySegmentCursor,
                   terminalSegmentCursor,
                   claimSegmentCursor,
                   laneState,
                   readyTombstones>>
    /\ UnchangedClaimData

RotateTerminalSegments ==
    LET next == NextTerminalSegment(terminalSegmentCursor) IN
    /\ terminalSegments[terminalSegmentCursor] = "open"
    /\ terminalSegments[next] = "pruned"
    /\ terminalSegments' = [terminalSegments EXCEPT
                              ![terminalSegmentCursor] = "sealed",
                              ![next] = "open"]
    /\ terminalSegmentCursor' = next
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingLeases,
                   terminalEntries,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   leaseSegmentOf,
                   terminalSegmentOf,
                   readySegments,
                   leaseSegments,
                   claimSegments,
                   readySegmentCursor,
                   leaseSegmentCursor,
                   claimSegmentCursor,
                   laneState,
                   readyTombstones>>
    /\ UnchangedClaimData

\* ADR-023 rotation of the claim-ring control plane. Mirrors the
\* lease-ring rotation pattern — advance the cursor to the next pruned
\* slot, mark the old open slot sealed, mark the new slot open.
RotateClaimSegments ==
    LET next == NextClaimSegment(claimSegmentCursor) IN
    /\ claimSegments[claimSegmentCursor] = "open"
    /\ claimSegments[next] = "pruned"
    /\ claimSegments' = [claimSegments EXCEPT
                           ![claimSegmentCursor] = "sealed",
                           ![next] = "open"]
    /\ claimSegmentCursor' = next
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingLeases,
                   terminalEntries,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   leaseSegmentOf,
                   terminalSegmentOf,
                   readySegments,
                   leaseSegments,
                   terminalSegments,
                   readySegmentCursor,
                   leaseSegmentCursor,
                   terminalSegmentCursor,
                   laneState,
                   readyTombstones>>
    /\ UnchangedClaimData

PruneReadySegment(seg) ==
    /\ seg \in ReadySegments
    /\ readySegments[seg] = "sealed"
    /\ \A j \in Jobs : readySegmentOf[j] = seg => laneSeq[j] < laneState.claimSeq /\ j \notin activeLeases /\ j \notin ReceiptClaimJobs
    /\ \A k \in ClaimKeys :
          ((claimSegmentOf[k] # NoClaimSegment /\ readySegmentOf[ClaimKeyJob(k)] = seg) => k \in claimClosed)
    /\ readyEntries' = readyEntries \ JobsInReadySegment(seg)
    /\ terminalEntries' = terminalEntries \ JobsInReadySegment(seg)
    /\ readyTombstones' = {t \in readyTombstones : t.segment # seg}
    /\ laneSeq' = [j \in Jobs |-> IF readySegmentOf[j] = seg THEN NoLaneSeq ELSE laneSeq[j]]
    /\ readySegmentOf' = [j \in Jobs |-> IF readySegmentOf[j] = seg THEN NoReadySegment ELSE readySegmentOf[j]]
    /\ terminalSegmentOf' = [j \in Jobs |-> IF readySegmentOf[j] = seg THEN NoTerminalSegment ELSE terminalSegmentOf[j]]
    /\ readySegments' = [readySegments EXCEPT ![seg] = "pruned"]
    /\ UNCHANGED <<deferredEntries,
                   waitingLeases,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   progressTouched,
                   leaseSegmentOf,
                   leaseSegments,
                   terminalSegments,
                   claimSegments,
                   readySegmentCursor,
                   leaseSegmentCursor,
                   terminalSegmentCursor,
                   claimSegmentCursor,
                   laneState>>
    /\ UnchangedClaimData

PruneLeaseSegment(seg) ==
    /\ seg \in LeaseSegments
    /\ leaseSegments[seg] = "sealed"
    /\ \A j \in Jobs : leaseSegmentOf[j] = seg => j \notin activeLeases /\ j \notin attemptState
    /\ leaseSegmentOf' = [j \in Jobs |-> IF leaseSegmentOf[j] = seg THEN NoLeaseSegment ELSE leaseSegmentOf[j]]
    /\ leaseSegments' = [leaseSegments EXCEPT ![seg] = "pruned"]
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingLeases,
                   terminalEntries,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   terminalSegmentOf,
                   readySegments,
                   terminalSegments,
                   claimSegments,
                   readySegmentCursor,
                   leaseSegmentCursor,
                   terminalSegmentCursor,
                   claimSegmentCursor,
                   laneState,
                   readyTombstones>>
    /\ UnchangedClaimData

PruneTerminalSegment(seg) ==
    /\ seg \in TerminalSegments
    /\ terminalSegments[seg] = "sealed"
    /\ \A j \in Jobs : terminalSegmentOf[j] = seg => j \notin terminalEntries
    /\ terminalSegmentOf' = [j \in Jobs |-> IF terminalSegmentOf[j] = seg THEN NoTerminalSegment ELSE terminalSegmentOf[j]]
    /\ terminalSegments' = [terminalSegments EXCEPT ![seg] = "pruned"]
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingLeases,
                   terminalEntries,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   leaseSegmentOf,
                   readySegments,
                   leaseSegments,
                   claimSegments,
                   readySegmentCursor,
                   leaseSegmentCursor,
                   terminalSegmentCursor,
                   claimSegmentCursor,
                   laneState,
                   readyTombstones>>
    /\ UnchangedClaimData

\* ADR-023 prune of the claim ring. The precondition
\* `\A k : claimSegmentOf[k] = seg => k \notin claimOpen` captures
\* rescue-before-truncate: every claim row in the partition must be
\* closed by durable closure evidence
\* before `prune_oldest_claims` can TRUNCATE the lease_claims /
\* lease_claim_closures / lease_claim_closure_batches children for that
\* slot. Ready prune is separately
\* guarded so it cannot remove terminal history before matching closure
\* evidence exists. Note this iterates over ClaimKeys, so an old (j, r)
\* receipt left behind by a previous attempt is correctly counted even
\* when the same job has been re-claimed under (j, r+1) into a newer
\* partition.
PruneClaimSegment(seg) ==
    /\ seg \in ClaimSegments
    /\ claimSegments[seg] = "sealed"
    /\ seg # claimSegmentCursor
    /\ \A k \in ClaimKeys : claimSegmentOf[k] = seg => k \notin claimOpen
    /\ claimSegmentOf' = [k \in ClaimKeys |-> IF claimSegmentOf[k] = seg THEN NoClaimSegment ELSE claimSegmentOf[k]]
    /\ claimClosed' = claimClosed \ KeysInClaimSegment(seg)
    /\ claimSegments' = [claimSegments EXCEPT ![seg] = "pruned"]
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingLeases,
                   terminalEntries,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   leaseSegmentOf,
                   terminalSegmentOf,
                   claimOpen,
                   readySegments,
                   leaseSegments,
                   terminalSegments,
                   readySegmentCursor,
                   leaseSegmentCursor,
                   terminalSegmentCursor,
                   claimSegmentCursor,
                   laneState,
                   readyTombstones>>

Stutter == /\ UNCHANGED vars

Next ==
    \/ \E j \in Jobs : EnqueueReady(j)
    \/ \E j \in Jobs : EnqueueDeferred(j)
    \/ \E j \in Jobs : PromoteDeferred(j)
    \/ AdvanceClaimCursor
    \/ \E w \in Workers, j \in Jobs : Claim(w, j)
    \/ \E j \in Jobs : MaterializeAttemptState(j)
    \/ \E j \in Jobs : Heartbeat(j)
    \/ \E j \in Jobs : LoseHeartbeat(j)
    \/ \E j \in Jobs : ProgressFlush(j)
    \/ \E w \in Workers, j \in Jobs : ParkToWaiting(w, j)
    \/ \E j \in Jobs : ResumeWaitingToRunning(j)
    \/ \E j \in Jobs : TimeoutWaitingToReady(j)
    \/ \E j \in Jobs : TimeoutWaitingToDlq(j)
    \/ \E w \in Workers, j \in Jobs : FastComplete(w, j)
    \/ \E w \in Workers, j \in Jobs : StatefulComplete(w, j)
    \/ \E w \in Workers, j \in Jobs : FailToDlq(w, j)
    \/ \E w \in Workers, j \in Jobs : RetryToDeferred(w, j)
    \/ \E j \in Jobs : RescueToReady(j)
    \/ \E j \in Jobs, r \in RunLeaseValues : RescueStaleReceipt(j, r)
    \/ \E j \in Jobs : CancelWaitingToTerminal(j)
    \/ \E j \in Jobs : CancelRunningToTerminal(j)
    \/ \E j \in Jobs : CancelReceiptOnlyToTerminal(j)
    \/ \E j \in Jobs : CancelReadyToTerminal(j)
    \/ \E j \in Jobs : ReprioritizeReady(j)
    \/ \E j \in Jobs : MoveQueueReady(j)
    \/ \E w \in Workers, j \in Jobs : StaleCompleteRejected(w, j)
    \/ \E j \in Jobs : MoveFailedToDlq(j)
    \/ \E j \in Jobs : RetryFromDlq(j)
    \/ \E j \in Jobs : PurgeDlq(j)
    \/ RotateReadySegments
    \/ RotateLeaseSegments
    \/ RotateTerminalSegments
    \/ RotateClaimSegments
    \/ \E seg \in ReadySegments : PruneReadySegment(seg)
    \/ \E seg \in LeaseSegments : PruneLeaseSegment(seg)
    \/ \E seg \in TerminalSegments : PruneTerminalSegment(seg)
    \/ \E seg \in ClaimSegments : PruneClaimSegment(seg)
    \/ Stutter

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ Jobs # {}
    /\ Workers # {}
    /\ ReadySegmentCount >= 1
    /\ LeaseSegmentCount >= 1
    /\ TerminalSegmentCount >= 1
    /\ ClaimSegmentCount >= 1
    /\ MaxRunLease >= 1
    /\ MaxAppendSeq >= Cardinality(Jobs)
    /\ readyEntries \subseteq Jobs
    /\ deferredEntries \subseteq Jobs
    /\ waitingLeases \subseteq Jobs
    /\ terminalEntries \subseteq Jobs
    /\ dlqEntries \subseteq Jobs
    /\ activeLeases \subseteq Jobs
    /\ leaseOwner \in [Jobs -> Workers \cup {NoWorker}]
    /\ runLease \in [Jobs -> 0..MaxRunLease]
    /\ taskLease \in [Workers -> [Jobs -> 0..MaxRunLease]]
    /\ attemptState \subseteq Jobs
    /\ heartbeatFresh \subseteq Jobs
    /\ progressTouched \subseteq Jobs
    /\ laneSeq \in [Jobs -> 0..MaxAppendSeq]
    /\ readySegmentOf \in [Jobs -> ReadySegments \cup {NoReadySegment}]
    /\ leaseSegmentOf \in [Jobs -> LeaseSegments \cup {NoLeaseSegment}]
    /\ terminalSegmentOf \in [Jobs -> TerminalSegments \cup {NoTerminalSegment}]
    /\ claimSegmentOf \in [ClaimKeys -> ClaimSegments \cup {NoClaimSegment}]
    /\ claimOpen \subseteq ClaimKeys
    /\ claimClosed \subseteq ClaimKeys
    /\ readySegments \in [ReadySegments -> SegmentStates]
    /\ leaseSegments \in [LeaseSegments -> SegmentStates]
    /\ terminalSegments \in [TerminalSegments -> SegmentStates]
    /\ claimSegments \in [ClaimSegments -> SegmentStates]
    /\ readySegmentCursor \in ReadySegments
    /\ leaseSegmentCursor \in LeaseSegments
    /\ terminalSegmentCursor \in TerminalSegments
    /\ claimSegmentCursor \in ClaimSegments
    /\ laneState.appendSeq \in 1..(MaxAppendSeq + 1)
    /\ laneState.claimSeq \in 1..(MaxAppendSeq + 1)
    /\ laneState.readyCount \in 0..Cardinality(Jobs)
    /\ laneState.leasedCount \in 0..Cardinality(Jobs)
    /\ readyTombstones \subseteq [job: Jobs,
                                  segment: ReadySegments \cup {NoReadySegment},
                                  laneSeq: 0..MaxAppendSeq]

OneOpenReadySegment == Cardinality({s \in ReadySegments : readySegments[s] = "open"}) = 1
OneOpenLeaseSegment == Cardinality({s \in LeaseSegments : leaseSegments[s] = "open"}) = 1
OneOpenTerminalSegment == Cardinality({s \in TerminalSegments : terminalSegments[s] = "open"}) = 1
OneOpenClaimSegment == Cardinality({s \in ClaimSegments : claimSegments[s] = "open"}) = 1

ReadyCursorIsOpen == readySegments[readySegmentCursor] = "open"
LeaseCursorIsOpen == leaseSegments[leaseSegmentCursor] = "open"
TerminalCursorIsOpen == terminalSegments[terminalSegmentCursor] = "open"
ClaimCursorIsOpen == claimSegments[claimSegmentCursor] = "open"

DeferredHasNoLiveRuntime ==
    /\ deferredEntries \cap activeLeases = {}
    /\ deferredEntries \cap attemptState = {}
    /\ deferredEntries \cap waitingLeases = {}

WaitingIsLeaseState == waitingLeases \subseteq activeLeases
WaitingRequiresAttemptState == waitingLeases \subseteq attemptState

ActiveLeasesSubsetReadyEntries == activeLeases \subseteq readyEntries
AttemptStateRequiresLiveLease == attemptState \subseteq activeLeases

\* Heartbeat freshness is now tracked at the lease level, matching the
\* Rust implementation. Short jobs (no attempt_state) can still publish
\* heartbeats and be rescued.
FreshHeartbeatRequiresLease == heartbeatFresh \subseteq (activeLeases \ waitingLeases)

ProgressRequiresAttemptState == progressTouched \subseteq attemptState

TerminalHasNoLiveRuntime ==
    \A j \in Jobs : j \in terminalEntries =>
        /\ j \notin activeLeases
        /\ j \notin attemptState
        /\ j \notin waitingLeases
        /\ j \notin ReceiptClaimJobs
        /\ j \notin deferredEntries

TerminalHasRetainedReadyBody ==
    \A j \in Jobs : j \in terminalEntries =>
        /\ j \in readyEntries
        /\ laneSeq[j] # NoLaneSeq
        /\ readySegmentOf[j] # NoReadySegment

DlqHasNoLiveRuntime ==
    \A j \in Jobs : j \in dlqEntries =>
        /\ j \notin activeLeases
        /\ j \notin attemptState
        /\ j \notin waitingLeases
        /\ j \notin CurrentReady
        /\ j \notin deferredEntries

DlqAndTerminalDisjoint == dlqEntries \cap terminalEntries = {}

ReadyEntriesHaveLaneSeq ==
    \A j \in CurrentReady : laneSeq[j] # NoLaneSeq

DeferredEntriesClearLaneSeq ==
    \A j \in deferredEntries : j \in readyEntries \/ laneSeq[j] = NoLaneSeq
DlqEntriesClearLaneSeq ==
    \A j \in dlqEntries : j \in readyEntries \/ laneSeq[j] = NoLaneSeq

ReadyLaneSeqUnique ==
    \A j1, j2 \in CurrentReady : j1 # j2 => laneSeq[j1] # laneSeq[j2]

ClaimCursorBounded == laneState.claimSeq <= laneState.appendSeq

CurrentReadyAtOrAheadOfCursor ==
    \A j \in CurrentReady : laneSeq[j] >= laneState.claimSeq

PrunedReadySegmentsAreEmpty ==
    \A j \in Jobs : readySegmentOf[j] # NoReadySegment => readySegments[readySegmentOf[j]] # "pruned"



PrunedLeaseSegmentsAreEmpty ==
    \A j \in Jobs : leaseSegmentOf[j] # NoLeaseSegment => leaseSegments[leaseSegmentOf[j]] # "pruned"


PrunedTerminalSegmentsAreEmpty ==
    \A j \in Jobs : terminalSegmentOf[j] # NoTerminalSegment => terminalSegments[terminalSegmentOf[j]] # "pruned"

PrunedClaimSegmentsAreEmpty ==
    \A k \in ClaimKeys : claimSegmentOf[k] # NoClaimSegment => claimSegments[claimSegmentOf[k]] # "pruned"

\* ADR-023 core safety: no open receipt is ever physically dropped. If a
\* claim is open (claimOpen), its partition is both non-zero and in an
\* un-pruned state (open or sealed, never pruned). This is what makes the
\* rotation-reclaim path safe: a prune can only fire once every claim in
\* the partition has closed (or been force-closed by RescueStaleReceipt).
NoLostClaim ==
    \A k \in claimOpen :
        /\ claimSegmentOf[k] # NoClaimSegment
        /\ claimSegments[claimSegmentOf[k]] # "pruned"

\* A claim is in at most one of the open / closed states. Moving between
\* them is a single atomic action (Claim opens, close transitions close,
\* prune clears); nothing leaves a (j, run_lease) key in both.
ClaimOpenAndClosedDisjoint == claimOpen \cap claimClosed = {}

\* An open receipt corresponds to a bound claim segment. An untracked
\* claim (segmentOf = 0) cannot be open.
OpenClaimHasSegment ==
    \A k \in ClaimKeys : k \in claimOpen => claimSegmentOf[k] # NoClaimSegment

ClosedClaimHasSegment ==
    \A k \in ClaimKeys : k \in claimClosed => claimSegmentOf[k] # NoClaimSegment

LaneStateConsistent ==
    /\ laneState.readyCount = Cardinality(CurrentReady)
    /\ laneState.leasedCount = Cardinality(activeLeases)

TaskLeaseBounded ==
    \A w \in Workers, j \in Jobs : taskLease[w][j] <= runLease[j]

=============================================================================

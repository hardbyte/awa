---- MODULE AwaSegmentedStorage ----
EXTENDS TLC, Naturals, FiniteSets

\* Focused storage model for the segmented 0.6+ runtime layout.
\*
\* It checks the relationships behind:
\* - ready_entries
\* - deferred_entries
\* - waiting_entries
\* - active_leases
\* - attempt_state
\* - terminal_entries
\* - dlq_entries
\* - lane_state
\* - ready / deferred / waiting / lease / dlq / terminal segment families
\* - claim segment family (ADR-023): lease_claims + lease_claim_closures
\*   partitioned BY LIST (claim_slot), reclaimed by rotation + TRUNCATE
\*   instead of the earlier open_receipt_claims INSERT+DELETE frontier
\*
\* Heartbeat freshness is tracked on active_leases (not attempt_state) to
\* match the Rust implementation, where a claimed short job carries a
\* heartbeat timestamp on its lease row without ever materialising an
\* attempt_state record. This lets short-job rescue be reachable in the
\* model.
\*
\* DLQ modelling: both the executor-initiated FailToDlq path (terminal
\* failure with DLQ enabled) and the admin-initiated MoveFailedToDlq path
\* (awa dlq move) land rows into a separate dlq_entries family with its
\* own rotating segments. RetryFromDlq re-appends a fresh runnable entry
\* with runLease reset to zero, matching the Rust contract.
\*
\* Terminal family modelling: terminal_entries is its own rotating
\* segmented family, matching ADR-019's "retention via partition
\* rotation, not row-by-row cleanup" intent. Successful completions and
\* cancel-from-waiting land into the current open terminal segment.
\* MoveFailedToDlq removes from the terminal family and clears the
\* segment pointer before re-appending to DLQ.
\*
\* Claim family modelling (ADR-023): every Claim action opens an
\* append-only receipt in the current claim segment, and every
\* attempt-ending transition (FastComplete / StatefulComplete / FailToDlq
\* / RetryToDeferred / RescueToReady / CancelWaitingToTerminal /
\* TimeoutWaitingToDlq / TimeoutWaitingToReady / ResumeWaitingToReady)
\* writes a closure row in that same segment. ParkToWaiting does NOT
\* close the receipt — the attempt is still alive in callback wait, so
\* its receipt stays open. PruneClaimSegment requires every claim in the
\* sealed partition to be closed before TRUNCATE fires, and rescue can
\* force-close stragglers via RescueStaleReceipt. This replaces the
\* earlier open_receipt_claims INSERT+DELETE frontier: the entire
\* receipt plane is reclaimed by partition rotation, which is what makes
\* the 0.6 vacuum-aware story complete.

CONSTANTS Jobs,
          Workers,
          ReadySegmentCount,
          DeferredSegmentCount,
          WaitingSegmentCount,
          LeaseSegmentCount,
          DlqSegmentCount,
          TerminalSegmentCount,
          ClaimSegmentCount,
          MaxRunLease,
          MaxAppendSeq

ReadySegments == 1..ReadySegmentCount
DeferredSegments == 1..DeferredSegmentCount
WaitingSegments == 1..WaitingSegmentCount
LeaseSegments == 1..LeaseSegmentCount
DlqSegments == 1..DlqSegmentCount
TerminalSegments == 1..TerminalSegmentCount
ClaimSegments == 1..ClaimSegmentCount
SegmentStates == {"open", "sealed", "pruned"}

NoWorker == "none"
NoReadySegment == 0
NoDeferredSegment == 0
NoWaitingSegment == 0
NoLeaseSegment == 0
NoDlqSegment == 0
NoTerminalSegment == 0
NoClaimSegment == 0
NoLaneSeq == 0

VARIABLES readyEntries,
          deferredEntries,
          waitingEntries,
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
          deferredSegmentOf,
          waitingSegmentOf,
          leaseSegmentOf,
          dlqSegmentOf,
          terminalSegmentOf,
          claimSegmentOf,
          claimOpen,
          claimClosed,
          readySegments,
          deferredSegments,
          waitingSegments,
          leaseSegments,
          dlqSegments,
          terminalSegments,
          claimSegments,
          readySegmentCursor,
          deferredSegmentCursor,
          waitingSegmentCursor,
          leaseSegmentCursor,
          dlqSegmentCursor,
          terminalSegmentCursor,
          claimSegmentCursor,
          laneState

vars == <<readyEntries,
          deferredEntries,
          waitingEntries,
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
          deferredSegmentOf,
          waitingSegmentOf,
          leaseSegmentOf,
          dlqSegmentOf,
          terminalSegmentOf,
          claimSegmentOf,
          claimOpen,
          claimClosed,
          readySegments,
          deferredSegments,
          waitingSegments,
          leaseSegments,
          dlqSegments,
          terminalSegments,
          claimSegments,
          readySegmentCursor,
          deferredSegmentCursor,
          waitingSegmentCursor,
          leaseSegmentCursor,
          dlqSegmentCursor,
          terminalSegmentCursor,
          claimSegmentCursor,
          laneState>>

NextReadySegment(s) == IF s = ReadySegmentCount THEN 1 ELSE s + 1
NextDeferredSegment(s) == IF s = DeferredSegmentCount THEN 1 ELSE s + 1
NextWaitingSegment(s) == IF s = WaitingSegmentCount THEN 1 ELSE s + 1
NextLeaseSegment(s) == IF s = LeaseSegmentCount THEN 1 ELSE s + 1
NextDlqSegment(s) == IF s = DlqSegmentCount THEN 1 ELSE s + 1
NextTerminalSegment(s) == IF s = TerminalSegmentCount THEN 1 ELSE s + 1
NextClaimSegment(s) == IF s = ClaimSegmentCount THEN 1 ELSE s + 1

CurrentReady == ((((readyEntries \ terminalEntries) \ activeLeases) \ deferredEntries) \ waitingEntries) \ dlqEntries

JobsInReadySegment(seg) == {j \in Jobs : readySegmentOf[j] = seg}
JobsInDeferredSegment(seg) == {j \in Jobs : deferredSegmentOf[j] = seg}
JobsInWaitingSegment(seg) == {j \in Jobs : waitingSegmentOf[j] = seg}
JobsInDlqSegment(seg) == {j \in Jobs : dlqSegmentOf[j] = seg}
JobsInTerminalSegment(seg) == {j \in Jobs : terminalSegmentOf[j] = seg}
JobsInClaimSegment(seg) == {j \in Jobs : claimSegmentOf[j] = seg}

InitSegments(Range) == [s \in Range |-> IF s = 1 THEN "open" ELSE "pruned"]

Init ==
    /\ readyEntries = {}
    /\ deferredEntries = {}
    /\ waitingEntries = {}
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
    /\ deferredSegmentOf = [j \in Jobs |-> NoDeferredSegment]
    /\ waitingSegmentOf = [j \in Jobs |-> NoWaitingSegment]
    /\ leaseSegmentOf = [j \in Jobs |-> NoLeaseSegment]
    /\ dlqSegmentOf = [j \in Jobs |-> NoDlqSegment]
    /\ terminalSegmentOf = [j \in Jobs |-> NoTerminalSegment]
    /\ claimSegmentOf = [j \in Jobs |-> NoClaimSegment]
    /\ claimOpen = {}
    /\ claimClosed = {}
    /\ readySegments = InitSegments(ReadySegments)
    /\ deferredSegments = InitSegments(DeferredSegments)
    /\ waitingSegments = InitSegments(WaitingSegments)
    /\ leaseSegments = InitSegments(LeaseSegments)
    /\ dlqSegments = InitSegments(DlqSegments)
    /\ terminalSegments = InitSegments(TerminalSegments)
    /\ claimSegments = InitSegments(ClaimSegments)
    /\ readySegmentCursor = 1
    /\ deferredSegmentCursor = 1
    /\ waitingSegmentCursor = 1
    /\ leaseSegmentCursor = 1
    /\ dlqSegmentCursor = 1
    /\ terminalSegmentCursor = 1
    /\ claimSegmentCursor = 1
    /\ laneState = [appendSeq |-> 1, claimSeq |-> 1, readyCount |-> 0, leasedCount |-> 0]

\* Segment-state variables for every family that are never touched by a
\* data-level action. Every data action unchanges this bundle.
UnchangedSegmentState ==
    UNCHANGED <<readySegments,
                deferredSegments,
                waitingSegments,
                leaseSegments,
                dlqSegments,
                terminalSegments,
                claimSegments,
                readySegmentCursor,
                deferredSegmentCursor,
                waitingSegmentCursor,
                leaseSegmentCursor,
                dlqSegmentCursor,
                terminalSegmentCursor,
                claimSegmentCursor>>

\* Receipt-plane data variables (claim-ring). Every action that doesn't
\* open or close a claim receipt unchanges this bundle. Claim() opens a
\* receipt; attempt-ending transitions close one.
UnchangedClaimData ==
    UNCHANGED <<claimSegmentOf, claimOpen, claimClosed>>

EnqueueReady(j) ==
    /\ j \in Jobs
    /\ runLease[j] = 0
    /\ j \notin readyEntries
    /\ j \notin deferredEntries
    /\ j \notin waitingEntries
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
                   waitingEntries,
                   terminalEntries,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   progressTouched,
                   deferredSegmentOf,
                   waitingSegmentOf,
                   leaseSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf>>
    /\ UnchangedSegmentState
    /\ UnchangedClaimData

EnqueueDeferred(j) ==
    /\ j \in Jobs
    /\ runLease[j] = 0
    /\ j \notin readyEntries
    /\ j \notin deferredEntries
    /\ j \notin waitingEntries
    /\ j \notin terminalEntries
    /\ j \notin dlqEntries
    /\ deferredSegments[deferredSegmentCursor] = "open"
    /\ deferredEntries' = deferredEntries \cup {j}
    /\ deferredSegmentOf' = [deferredSegmentOf EXCEPT ![j] = deferredSegmentCursor]
    /\ UNCHANGED <<readyEntries,
                   waitingEntries,
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
                   waitingSegmentOf,
                   leaseSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf,
                   laneState>>
    /\ UnchangedSegmentState
    /\ UnchangedClaimData

PromoteDeferred(j) ==
    /\ j \in deferredEntries
    /\ j \notin terminalEntries
    /\ laneState.appendSeq <= MaxAppendSeq
    /\ readySegments[readySegmentCursor] = "open"
    /\ deferredEntries' = deferredEntries \ {j}
    /\ readyEntries' = readyEntries \cup {j}
    /\ deferredSegmentOf' = [deferredSegmentOf EXCEPT ![j] = NoDeferredSegment]
    /\ readySegmentOf' = [readySegmentOf EXCEPT ![j] = readySegmentCursor]
    /\ laneSeq' = [laneSeq EXCEPT ![j] = laneState.appendSeq]
    /\ laneState' = [laneState EXCEPT
                        !.appendSeq = laneState.appendSeq + 1,
                        !.readyCount = laneState.readyCount + 1]
    /\ UNCHANGED <<waitingEntries,
                   terminalEntries,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   progressTouched,
                   waitingSegmentOf,
                   leaseSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf>>
    /\ UnchangedSegmentState
    /\ UnchangedClaimData

AdvanceClaimCursor ==
    /\ laneState.claimSeq < laneState.appendSeq
    /\ ~ (\E j \in CurrentReady : laneSeq[j] = laneState.claimSeq)
    /\ laneState' = [laneState EXCEPT !.claimSeq = laneState.claimSeq + 1]
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingEntries,
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
                   deferredSegmentOf,
                   waitingSegmentOf,
                   leaseSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf>>
    /\ UnchangedSegmentState
    /\ UnchangedClaimData

\* Claim now also appends a claim receipt into the current claim segment.
\* The receipt lives in `claim_slot = claimSegmentCursor`; the
\* corresponding closure row (written by an attempt-ending transition)
\* targets the same slot, which is why `claimSegmentOf[j]` is preserved
\* across close until the whole partition is pruned.
Claim(w, j) ==
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
    /\ claimSegmentOf' = [claimSegmentOf EXCEPT ![j] = claimSegmentCursor]
    /\ claimOpen' = claimOpen \cup {j}
    \* Starting a new attempt cycle wipes the closure bookkeeping for
    \* `j` left over from the previous attempt. The physical closure row
    \* in the old partition is still counted by that partition's
    \* claimSegmentOf mapping until prune fires.
    /\ claimClosed' = claimClosed \ {j}
    /\ laneState' = [laneState EXCEPT
                        !.claimSeq = laneState.claimSeq + 1,
                        !.readyCount = laneState.readyCount - 1,
                        !.leasedCount = laneState.leasedCount + 1]
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingEntries,
                   terminalEntries,
                   dlqEntries,
                   attemptState,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   deferredSegmentOf,
                   waitingSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf>>
    /\ UnchangedSegmentState

MaterializeAttemptState(j) ==
    /\ j \in activeLeases
    /\ j \notin attemptState
    /\ attemptState' = attemptState \cup {j}
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingEntries,
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
                   deferredSegmentOf,
                   waitingSegmentOf,
                   leaseSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf,
                   laneState>>
    /\ UnchangedSegmentState
    /\ UnchangedClaimData

Heartbeat(j) ==
    /\ j \in activeLeases
    /\ heartbeatFresh' = heartbeatFresh \cup {j}
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingEntries,
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
                   deferredSegmentOf,
                   waitingSegmentOf,
                   leaseSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf,
                   laneState>>
    /\ UnchangedSegmentState
    /\ UnchangedClaimData

LoseHeartbeat(j) ==
    /\ j \in activeLeases
    /\ j \in heartbeatFresh
    /\ heartbeatFresh' = heartbeatFresh \ {j}
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingEntries,
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
                   deferredSegmentOf,
                   waitingSegmentOf,
                   leaseSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf,
                   laneState>>
    /\ UnchangedSegmentState
    /\ UnchangedClaimData

ProgressFlush(j) ==
    /\ j \in attemptState
    /\ progressTouched' = progressTouched \cup {j}
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingEntries,
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
                   deferredSegmentOf,
                   waitingSegmentOf,
                   leaseSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf,
                   laneState>>
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
    /\ waitingSegments[waitingSegmentCursor] = "open"
    /\ activeLeases' = activeLeases \ {j}
    /\ waitingEntries' = waitingEntries \cup {j}
    /\ readyEntries' = readyEntries \ {j}
    /\ leaseOwner' = [leaseOwner EXCEPT ![j] = NoWorker]
    /\ taskLease' = [taskLease EXCEPT ![w][j] = 0]
    /\ heartbeatFresh' = heartbeatFresh \ {j}
    /\ laneSeq' = [laneSeq EXCEPT ![j] = NoLaneSeq]
    /\ readySegmentOf' = [readySegmentOf EXCEPT ![j] = NoReadySegment]
    /\ waitingSegmentOf' = [waitingSegmentOf EXCEPT ![j] = waitingSegmentCursor]
    /\ leaseSegmentOf' = [leaseSegmentOf EXCEPT ![j] = NoLeaseSegment]
    /\ laneState' = [laneState EXCEPT !.leasedCount = laneState.leasedCount - 1]
    /\ UNCHANGED <<deferredEntries,
                   terminalEntries,
                   dlqEntries,
                   runLease,
                   attemptState,
                   progressTouched,
                   deferredSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf>>
    /\ UnchangedSegmentState
    /\ UnchangedClaimData

\* Callback completion: waiting -> ready, and the old run_lease's receipt
\* closes (a fresh Claim will open a new receipt in whatever the current
\* claim-cursor value is at that time).
ResumeWaitingToReady(j) ==
    /\ j \in waitingEntries
    /\ j \in attemptState
    /\ laneState.appendSeq <= MaxAppendSeq
    /\ readySegments[readySegmentCursor] = "open"
    /\ waitingEntries' = waitingEntries \ {j}
    /\ readyEntries' = readyEntries \cup {j}
    /\ attemptState' = attemptState \ {j}
    /\ progressTouched' = progressTouched \ {j}
    /\ waitingSegmentOf' = [waitingSegmentOf EXCEPT ![j] = NoWaitingSegment]
    /\ readySegmentOf' = [readySegmentOf EXCEPT ![j] = readySegmentCursor]
    /\ laneSeq' = [laneSeq EXCEPT ![j] = laneState.appendSeq]
    /\ claimOpen' = claimOpen \ {j}
    /\ claimClosed' = claimClosed \cup {j}
    /\ laneState' = [laneState EXCEPT
                        !.appendSeq = laneState.appendSeq + 1,
                        !.readyCount = laneState.readyCount + 1]
    /\ UNCHANGED <<deferredEntries,
                   terminalEntries,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   heartbeatFresh,
                   deferredSegmentOf,
                   leaseSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf,
                   claimSegmentOf>>
    /\ UnchangedSegmentState

\* Callback timeout with attempts remaining: waiting -> ready and close
\* the old receipt (the retry will open a new one on its next Claim).
TimeoutWaitingToReady(j) ==
    /\ j \in waitingEntries
    /\ j \in attemptState
    /\ laneState.appendSeq <= MaxAppendSeq
    /\ readySegments[readySegmentCursor] = "open"
    /\ waitingEntries' = waitingEntries \ {j}
    /\ readyEntries' = readyEntries \cup {j}
    /\ attemptState' = attemptState \ {j}
    /\ progressTouched' = progressTouched \ {j}
    /\ waitingSegmentOf' = [waitingSegmentOf EXCEPT ![j] = NoWaitingSegment]
    /\ readySegmentOf' = [readySegmentOf EXCEPT ![j] = readySegmentCursor]
    /\ laneSeq' = [laneSeq EXCEPT ![j] = laneState.appendSeq]
    /\ claimOpen' = claimOpen \ {j}
    /\ claimClosed' = claimClosed \cup {j}
    /\ laneState' = [laneState EXCEPT
                        !.appendSeq = laneState.appendSeq + 1,
                        !.readyCount = laneState.readyCount + 1]
    /\ UNCHANGED <<deferredEntries,
                   terminalEntries,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   heartbeatFresh,
                   deferredSegmentOf,
                   leaseSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf,
                   claimSegmentOf>>
    /\ UnchangedSegmentState

\* Callback-timeout rescue on exhausted attempts lands a waiting entry
\* directly into the DLQ rather than re-enqueuing for another attempt.
TimeoutWaitingToDlq(j) ==
    /\ j \in waitingEntries
    /\ j \in attemptState
    /\ dlqSegments[dlqSegmentCursor] = "open"
    /\ waitingEntries' = waitingEntries \ {j}
    /\ dlqEntries' = dlqEntries \cup {j}
    /\ attemptState' = attemptState \ {j}
    /\ progressTouched' = progressTouched \ {j}
    /\ waitingSegmentOf' = [waitingSegmentOf EXCEPT ![j] = NoWaitingSegment]
    /\ dlqSegmentOf' = [dlqSegmentOf EXCEPT ![j] = dlqSegmentCursor]
    /\ claimOpen' = claimOpen \ {j}
    /\ claimClosed' = claimClosed \cup {j}
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   terminalEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   heartbeatFresh,
                   laneSeq,
                   readySegmentOf,
                   deferredSegmentOf,
                   leaseSegmentOf,
                   terminalSegmentOf,
                   claimSegmentOf,
                   laneState>>
    /\ UnchangedSegmentState

FastComplete(w, j) ==
    /\ w \in Workers
    /\ j \in activeLeases
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
    /\ readyEntries' = readyEntries \ {j}
    /\ laneSeq' = [laneSeq EXCEPT ![j] = NoLaneSeq]
    /\ readySegmentOf' = [readySegmentOf EXCEPT ![j] = NoReadySegment]
    /\ leaseSegmentOf' = [leaseSegmentOf EXCEPT ![j] = NoLeaseSegment]
    /\ claimOpen' = claimOpen \ {j}
    /\ claimClosed' = claimClosed \cup {j}
    /\ laneState' = [laneState EXCEPT !.leasedCount = laneState.leasedCount - 1]
    /\ UNCHANGED <<deferredEntries,
                   waitingEntries,
                   dlqEntries,
                   runLease,
                   attemptState,
                   progressTouched,
                   deferredSegmentOf,
                   waitingSegmentOf,
                   dlqSegmentOf,
                   claimSegmentOf>>
    /\ UnchangedSegmentState

StatefulComplete(w, j) ==
    /\ w \in Workers
    /\ j \in activeLeases
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
    /\ readyEntries' = readyEntries \ {j}
    /\ laneSeq' = [laneSeq EXCEPT ![j] = NoLaneSeq]
    /\ readySegmentOf' = [readySegmentOf EXCEPT ![j] = NoReadySegment]
    /\ leaseSegmentOf' = [leaseSegmentOf EXCEPT ![j] = NoLeaseSegment]
    /\ claimOpen' = claimOpen \ {j}
    /\ claimClosed' = claimClosed \cup {j}
    /\ laneState' = [laneState EXCEPT !.leasedCount = laneState.leasedCount - 1]
    /\ UNCHANGED <<deferredEntries,
                   waitingEntries,
                   dlqEntries,
                   runLease,
                   deferredSegmentOf,
                   waitingSegmentOf,
                   dlqSegmentOf,
                   claimSegmentOf>>
    /\ UnchangedSegmentState

\* Executor-side terminal failure with DLQ enabled. The attempt's lease
\* and attempt_state are cleared and the job is appended to the DLQ
\* instead of terminal_entries.
FailToDlq(w, j) ==
    /\ w \in Workers
    /\ j \in activeLeases
    /\ leaseOwner[j] = w
    /\ taskLease[w][j] = runLease[j]
    /\ dlqSegments[dlqSegmentCursor] = "open"
    /\ activeLeases' = activeLeases \ {j}
    /\ readyEntries' = readyEntries \ {j}
    /\ dlqEntries' = dlqEntries \cup {j}
    /\ leaseOwner' = [leaseOwner EXCEPT ![j] = NoWorker]
    /\ taskLease' = [taskLease EXCEPT ![w][j] = 0]
    /\ attemptState' = attemptState \ {j}
    /\ heartbeatFresh' = heartbeatFresh \ {j}
    /\ progressTouched' = progressTouched \ {j}
    /\ laneSeq' = [laneSeq EXCEPT ![j] = NoLaneSeq]
    /\ readySegmentOf' = [readySegmentOf EXCEPT ![j] = NoReadySegment]
    /\ leaseSegmentOf' = [leaseSegmentOf EXCEPT ![j] = NoLeaseSegment]
    /\ dlqSegmentOf' = [dlqSegmentOf EXCEPT ![j] = dlqSegmentCursor]
    /\ claimOpen' = claimOpen \ {j}
    /\ claimClosed' = claimClosed \cup {j}
    /\ laneState' = [laneState EXCEPT !.leasedCount = laneState.leasedCount - 1]
    /\ UNCHANGED <<deferredEntries,
                   waitingEntries,
                   terminalEntries,
                   runLease,
                   deferredSegmentOf,
                   waitingSegmentOf,
                   terminalSegmentOf,
                   claimSegmentOf>>
    /\ UnchangedSegmentState

RetryToDeferred(w, j) ==
    /\ w \in Workers
    /\ j \in activeLeases
    /\ leaseOwner[j] = w
    /\ taskLease[w][j] = runLease[j]
    /\ deferredSegments[deferredSegmentCursor] = "open"
    /\ activeLeases' = activeLeases \ {j}
    /\ deferredEntries' = deferredEntries \cup {j}
    /\ readyEntries' = readyEntries \ {j}
    /\ leaseOwner' = [leaseOwner EXCEPT ![j] = NoWorker]
    /\ taskLease' = [taskLease EXCEPT ![w][j] = 0]
    /\ attemptState' = attemptState \ {j}
    /\ heartbeatFresh' = heartbeatFresh \ {j}
    /\ progressTouched' = progressTouched \ {j}
    /\ laneSeq' = [laneSeq EXCEPT ![j] = NoLaneSeq]
    /\ readySegmentOf' = [readySegmentOf EXCEPT ![j] = NoReadySegment]
    /\ deferredSegmentOf' = [deferredSegmentOf EXCEPT ![j] = deferredSegmentCursor]
    /\ leaseSegmentOf' = [leaseSegmentOf EXCEPT ![j] = NoLeaseSegment]
    /\ claimOpen' = claimOpen \ {j}
    /\ claimClosed' = claimClosed \cup {j}
    /\ laneState' = [laneState EXCEPT !.leasedCount = laneState.leasedCount - 1]
    /\ UNCHANGED <<waitingEntries,
                   terminalEntries,
                   dlqEntries,
                   runLease,
                   waitingSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf,
                   claimSegmentOf>>
    /\ UnchangedSegmentState

\* Heartbeat or deadline rescue. No longer requires attempt_state, so
\* short jobs that claimed and immediately lost their heartbeat (e.g.
\* worker crash before the first heartbeat publish) are reachable here.
\* Rescue closes the old run_lease's receipt; the next Claim will open a
\* fresh receipt in the current claim segment.
RescueToReady(j) ==
    /\ j \in activeLeases
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
    /\ claimOpen' = claimOpen \ {j}
    /\ claimClosed' = claimClosed \cup {j}
    /\ laneState' = [laneState EXCEPT
                        !.appendSeq = laneState.appendSeq + 1,
                        !.readyCount = laneState.readyCount + 1,
                        !.leasedCount = laneState.leasedCount - 1]
    /\ UNCHANGED <<deferredEntries,
                   waitingEntries,
                   terminalEntries,
                   dlqEntries,
                   runLease,
                   taskLease,
                   heartbeatFresh,
                   deferredSegmentOf,
                   waitingSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf,
                   claimSegmentOf>>
    /\ UnchangedSegmentState

\* ADR-023 Tier-A rescue: force-close a straggling receipt whose attempt
\* has already been taken off the ready / leased / waiting lifecycle (so
\* there is no live runtime to rescue-to-ready from). The receipt itself
\* is still open, which would otherwise block prune on its partition.
\* This models the rescue-before-truncate precondition that
\* `prune_oldest_claims` takes before calling TRUNCATE.
RescueStaleReceipt(j) ==
    /\ j \in claimOpen
    /\ j \notin activeLeases
    /\ j \notin waitingEntries
    /\ claimOpen' = claimOpen \ {j}
    /\ claimClosed' = claimClosed \cup {j}
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingEntries,
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
                   deferredSegmentOf,
                   waitingSegmentOf,
                   leaseSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf,
                   claimSegmentOf,
                   laneState>>
    /\ UnchangedSegmentState

CancelWaitingToTerminal(j) ==
    /\ j \in waitingEntries
    /\ terminalSegments[terminalSegmentCursor] = "open"
    /\ terminalEntries' = terminalEntries \cup {j}
    /\ terminalSegmentOf' = [terminalSegmentOf EXCEPT ![j] = terminalSegmentCursor]
    /\ waitingEntries' = waitingEntries \ {j}
    /\ attemptState' = attemptState \ {j}
    /\ heartbeatFresh' = heartbeatFresh \ {j}
    /\ progressTouched' = progressTouched \ {j}
    /\ waitingSegmentOf' = [waitingSegmentOf EXCEPT ![j] = NoWaitingSegment]
    /\ claimOpen' = claimOpen \ {j}
    /\ claimClosed' = claimClosed \cup {j}
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   laneSeq,
                   readySegmentOf,
                   deferredSegmentOf,
                   leaseSegmentOf,
                   dlqSegmentOf,
                   claimSegmentOf,
                   laneState>>
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
    /\ readyEntries' = readyEntries \ {j}
    /\ laneSeq' = [laneSeq EXCEPT ![j] = NoLaneSeq]
    /\ readySegmentOf' = [readySegmentOf EXCEPT ![j] = NoReadySegment]
    /\ leaseSegmentOf' = [leaseSegmentOf EXCEPT ![j] = NoLeaseSegment]
    /\ claimOpen' = claimOpen \ {j}
    /\ claimClosed' = claimClosed \cup {j}
    /\ laneState' = [laneState EXCEPT !.leasedCount = laneState.leasedCount - 1]
    /\ UNCHANGED <<deferredEntries,
                   waitingEntries,
                   dlqEntries,
                   runLease,
                   deferredSegmentOf,
                   waitingSegmentOf,
                   dlqSegmentOf,
                   claimSegmentOf>>
    /\ UnchangedSegmentState

\* Admin cancel of a job whose attempt is running on the receipt-only
\* short path (open claim, no `leases` row materialized). Mirrors the
\* receipt-only branch in `cancel_job_tx`: SELECT FROM lease_claims
\* FOR UPDATE, INSERT done row, INSERT closure, defensive DELETE FROM
\* leases (Wave 2d sweeps any concurrent materialization), pg_notify.
\* Distinct from CancelRunningToTerminal because no lease ever existed,
\* so activeLeases is unchanged and laneState's leasedCount stays put.
CancelReceiptOnlyToTerminal(j) ==
    /\ j \in claimOpen
    /\ j \notin activeLeases
    /\ j \notin waitingEntries
    /\ terminalSegments[terminalSegmentCursor] = "open"
    /\ terminalEntries' = terminalEntries \cup {j}
    /\ terminalSegmentOf' = [terminalSegmentOf EXCEPT ![j] = terminalSegmentCursor]
    /\ readyEntries' = readyEntries \ {j}
    /\ laneSeq' = [laneSeq EXCEPT ![j] = NoLaneSeq]
    /\ readySegmentOf' = [readySegmentOf EXCEPT ![j] = NoReadySegment]
    \* Same rationale as CancelRunningToTerminal: zero every worker's
    \* taskLease snapshot since admin cancel has no worker context.
    /\ taskLease' = [w \in Workers |-> [taskLease[w] EXCEPT ![j] = 0]]
    /\ claimOpen' = claimOpen \ {j}
    /\ claimClosed' = claimClosed \cup {j}
    /\ UNCHANGED <<deferredEntries,
                   waitingEntries,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   attemptState,
                   heartbeatFresh,
                   progressTouched,
                   deferredSegmentOf,
                   waitingSegmentOf,
                   leaseSegmentOf,
                   dlqSegmentOf,
                   claimSegmentOf,
                   laneState>>
    /\ UnchangedSegmentState

StaleCompleteRejected(w, j) ==
    /\ w \in Workers
    /\ j \in Jobs
    /\ taskLease[w][j] # 0
    /\ j \notin activeLeases \/ taskLease[w][j] # runLease[j]
    /\ taskLease' = [taskLease EXCEPT ![w][j] = 0]
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingEntries,
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
                   deferredSegmentOf,
                   waitingSegmentOf,
                   leaseSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf,
                   laneState>>
    /\ UnchangedSegmentState
    /\ UnchangedClaimData

\* Admin-initiated move of a terminal row into the DLQ. Models the
\* `awa dlq move` command path. Clears the terminalSegmentOf pointer so
\* the source terminal segment can eventually be pruned.
MoveFailedToDlq(j) ==
    /\ j \in terminalEntries
    /\ j \notin dlqEntries
    /\ dlqSegments[dlqSegmentCursor] = "open"
    /\ terminalEntries' = terminalEntries \ {j}
    /\ terminalSegmentOf' = [terminalSegmentOf EXCEPT ![j] = NoTerminalSegment]
    /\ dlqEntries' = dlqEntries \cup {j}
    /\ dlqSegmentOf' = [dlqSegmentOf EXCEPT ![j] = dlqSegmentCursor]
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   deferredSegmentOf,
                   waitingSegmentOf,
                   leaseSegmentOf,
                   laneState>>
    /\ UnchangedSegmentState
    /\ UnchangedClaimData

\* Retry a DLQ row back into live queue storage. Resets run_lease to 0
\* to match the Rust contract (`retry_from_dlq` starts a fresh attempt).
RetryFromDlq(j) ==
    /\ j \in dlqEntries
    /\ laneState.appendSeq <= MaxAppendSeq
    /\ readySegments[readySegmentCursor] = "open"
    /\ dlqEntries' = dlqEntries \ {j}
    /\ readyEntries' = readyEntries \cup {j}
    /\ runLease' = [runLease EXCEPT ![j] = 0]
    /\ laneSeq' = [laneSeq EXCEPT ![j] = laneState.appendSeq]
    /\ dlqSegmentOf' = [dlqSegmentOf EXCEPT ![j] = NoDlqSegment]
    /\ readySegmentOf' = [readySegmentOf EXCEPT ![j] = readySegmentCursor]
    /\ laneState' = [laneState EXCEPT
                        !.appendSeq = laneState.appendSeq + 1,
                        !.readyCount = laneState.readyCount + 1]
    /\ UNCHANGED <<deferredEntries,
                   waitingEntries,
                   terminalEntries,
                   activeLeases,
                   leaseOwner,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   progressTouched,
                   deferredSegmentOf,
                   waitingSegmentOf,
                   leaseSegmentOf,
                   terminalSegmentOf>>
    /\ UnchangedSegmentState
    /\ UnchangedClaimData

\* Admin purge of a DLQ row. Leaves dlqSegmentOf pointing at the old
\* segment until PruneDlqSegment reclaims it, mirroring the Rust
\* behaviour where the row is deleted but its segment partition only
\* rotates out once all live rows have drained.
PurgeDlq(j) ==
    /\ j \in dlqEntries
    /\ dlqEntries' = dlqEntries \ {j}
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingEntries,
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
                   deferredSegmentOf,
                   waitingSegmentOf,
                   leaseSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf,
                   laneState>>
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
                   waitingEntries,
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
                   deferredSegmentOf,
                   waitingSegmentOf,
                   leaseSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf,
                   deferredSegments,
                   waitingSegments,
                   leaseSegments,
                   dlqSegments,
                   terminalSegments,
                   claimSegments,
                   deferredSegmentCursor,
                   waitingSegmentCursor,
                   leaseSegmentCursor,
                   dlqSegmentCursor,
                   terminalSegmentCursor,
                   claimSegmentCursor,
                   laneState>>
    /\ UnchangedClaimData

RotateDeferredSegments ==
    LET next == NextDeferredSegment(deferredSegmentCursor) IN
    /\ deferredSegments[deferredSegmentCursor] = "open"
    /\ deferredSegments[next] = "pruned"
    /\ deferredSegments' = [deferredSegments EXCEPT
                              ![deferredSegmentCursor] = "sealed",
                              ![next] = "open"]
    /\ deferredSegmentCursor' = next
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingEntries,
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
                   deferredSegmentOf,
                   waitingSegmentOf,
                   leaseSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf,
                   readySegments,
                   waitingSegments,
                   leaseSegments,
                   dlqSegments,
                   terminalSegments,
                   claimSegments,
                   readySegmentCursor,
                   waitingSegmentCursor,
                   leaseSegmentCursor,
                   dlqSegmentCursor,
                   terminalSegmentCursor,
                   claimSegmentCursor,
                   laneState>>
    /\ UnchangedClaimData

RotateWaitingSegments ==
    LET next == NextWaitingSegment(waitingSegmentCursor) IN
    /\ waitingSegments[waitingSegmentCursor] = "open"
    /\ waitingSegments[next] = "pruned"
    /\ waitingSegments' = [waitingSegments EXCEPT
                             ![waitingSegmentCursor] = "sealed",
                             ![next] = "open"]
    /\ waitingSegmentCursor' = next
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingEntries,
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
                   deferredSegmentOf,
                   waitingSegmentOf,
                   leaseSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf,
                   readySegments,
                   deferredSegments,
                   leaseSegments,
                   dlqSegments,
                   terminalSegments,
                   claimSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   leaseSegmentCursor,
                   dlqSegmentCursor,
                   terminalSegmentCursor,
                   claimSegmentCursor,
                   laneState>>
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
                   waitingEntries,
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
                   deferredSegmentOf,
                   waitingSegmentOf,
                   leaseSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf,
                   readySegments,
                   deferredSegments,
                   waitingSegments,
                   dlqSegments,
                   terminalSegments,
                   claimSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   waitingSegmentCursor,
                   dlqSegmentCursor,
                   terminalSegmentCursor,
                   claimSegmentCursor,
                   laneState>>
    /\ UnchangedClaimData

RotateDlqSegments ==
    LET next == NextDlqSegment(dlqSegmentCursor) IN
    /\ dlqSegments[dlqSegmentCursor] = "open"
    /\ dlqSegments[next] = "pruned"
    /\ dlqSegments' = [dlqSegments EXCEPT
                         ![dlqSegmentCursor] = "sealed",
                         ![next] = "open"]
    /\ dlqSegmentCursor' = next
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingEntries,
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
                   deferredSegmentOf,
                   waitingSegmentOf,
                   leaseSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf,
                   readySegments,
                   deferredSegments,
                   waitingSegments,
                   leaseSegments,
                   terminalSegments,
                   claimSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   waitingSegmentCursor,
                   leaseSegmentCursor,
                   terminalSegmentCursor,
                   claimSegmentCursor,
                   laneState>>
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
                   waitingEntries,
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
                   deferredSegmentOf,
                   waitingSegmentOf,
                   leaseSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf,
                   readySegments,
                   deferredSegments,
                   waitingSegments,
                   leaseSegments,
                   dlqSegments,
                   claimSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   waitingSegmentCursor,
                   leaseSegmentCursor,
                   dlqSegmentCursor,
                   claimSegmentCursor,
                   laneState>>
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
                   waitingEntries,
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
                   deferredSegmentOf,
                   waitingSegmentOf,
                   leaseSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf,
                   readySegments,
                   deferredSegments,
                   waitingSegments,
                   leaseSegments,
                   dlqSegments,
                   terminalSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   waitingSegmentCursor,
                   leaseSegmentCursor,
                   dlqSegmentCursor,
                   terminalSegmentCursor,
                   laneState>>
    /\ UnchangedClaimData

PruneReadySegment(seg) ==
    /\ seg \in ReadySegments
    /\ readySegments[seg] = "sealed"
    /\ \A j \in Jobs : readySegmentOf[j] = seg => j \notin CurrentReady /\ j \notin activeLeases
    /\ readyEntries' = readyEntries \ JobsInReadySegment(seg)
    /\ laneSeq' = [j \in Jobs |-> IF readySegmentOf[j] = seg THEN NoLaneSeq ELSE laneSeq[j]]
    /\ readySegmentOf' = [j \in Jobs |-> IF readySegmentOf[j] = seg THEN NoReadySegment ELSE readySegmentOf[j]]
    /\ readySegments' = [readySegments EXCEPT ![seg] = "pruned"]
    /\ UNCHANGED <<deferredEntries,
                   waitingEntries,
                   terminalEntries,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   progressTouched,
                   deferredSegmentOf,
                   waitingSegmentOf,
                   leaseSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf,
                   deferredSegments,
                   waitingSegments,
                   leaseSegments,
                   dlqSegments,
                   terminalSegments,
                   claimSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   waitingSegmentCursor,
                   leaseSegmentCursor,
                   dlqSegmentCursor,
                   terminalSegmentCursor,
                   claimSegmentCursor,
                   laneState>>
    /\ UnchangedClaimData

PruneDeferredSegment(seg) ==
    /\ seg \in DeferredSegments
    /\ deferredSegments[seg] = "sealed"
    /\ \A j \in Jobs : deferredSegmentOf[j] = seg => j \notin deferredEntries
    /\ deferredSegmentOf' = [j \in Jobs |-> IF deferredSegmentOf[j] = seg THEN NoDeferredSegment ELSE deferredSegmentOf[j]]
    /\ deferredSegments' = [deferredSegments EXCEPT ![seg] = "pruned"]
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingEntries,
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
                   waitingSegmentOf,
                   leaseSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf,
                   readySegments,
                   waitingSegments,
                   leaseSegments,
                   dlqSegments,
                   terminalSegments,
                   claimSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   waitingSegmentCursor,
                   leaseSegmentCursor,
                   dlqSegmentCursor,
                   terminalSegmentCursor,
                   claimSegmentCursor,
                   laneState>>
    /\ UnchangedClaimData

PruneWaitingSegment(seg) ==
    /\ seg \in WaitingSegments
    /\ waitingSegments[seg] = "sealed"
    /\ \A j \in Jobs : waitingSegmentOf[j] = seg => j \notin waitingEntries
    /\ waitingSegmentOf' = [j \in Jobs |-> IF waitingSegmentOf[j] = seg THEN NoWaitingSegment ELSE waitingSegmentOf[j]]
    /\ waitingSegments' = [waitingSegments EXCEPT ![seg] = "pruned"]
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingEntries,
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
                   deferredSegmentOf,
                   leaseSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf,
                   readySegments,
                   deferredSegments,
                   leaseSegments,
                   dlqSegments,
                   terminalSegments,
                   claimSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   waitingSegmentCursor,
                   leaseSegmentCursor,
                   dlqSegmentCursor,
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
                   waitingEntries,
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
                   deferredSegmentOf,
                   waitingSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf,
                   readySegments,
                   deferredSegments,
                   waitingSegments,
                   dlqSegments,
                   terminalSegments,
                   claimSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   waitingSegmentCursor,
                   leaseSegmentCursor,
                   dlqSegmentCursor,
                   terminalSegmentCursor,
                   claimSegmentCursor,
                   laneState>>
    /\ UnchangedClaimData

PruneDlqSegment(seg) ==
    /\ seg \in DlqSegments
    /\ dlqSegments[seg] = "sealed"
    /\ \A j \in Jobs : dlqSegmentOf[j] = seg => j \notin dlqEntries
    /\ dlqSegmentOf' = [j \in Jobs |-> IF dlqSegmentOf[j] = seg THEN NoDlqSegment ELSE dlqSegmentOf[j]]
    /\ dlqSegments' = [dlqSegments EXCEPT ![seg] = "pruned"]
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingEntries,
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
                   deferredSegmentOf,
                   waitingSegmentOf,
                   leaseSegmentOf,
                   terminalSegmentOf,
                   readySegments,
                   deferredSegments,
                   waitingSegments,
                   leaseSegments,
                   terminalSegments,
                   claimSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   waitingSegmentCursor,
                   leaseSegmentCursor,
                   dlqSegmentCursor,
                   terminalSegmentCursor,
                   claimSegmentCursor,
                   laneState>>
    /\ UnchangedClaimData

PruneTerminalSegment(seg) ==
    /\ seg \in TerminalSegments
    /\ terminalSegments[seg] = "sealed"
    /\ \A j \in Jobs : terminalSegmentOf[j] = seg => j \notin terminalEntries
    /\ terminalSegmentOf' = [j \in Jobs |-> IF terminalSegmentOf[j] = seg THEN NoTerminalSegment ELSE terminalSegmentOf[j]]
    /\ terminalSegments' = [terminalSegments EXCEPT ![seg] = "pruned"]
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingEntries,
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
                   deferredSegmentOf,
                   waitingSegmentOf,
                   leaseSegmentOf,
                   dlqSegmentOf,
                   readySegments,
                   deferredSegments,
                   waitingSegments,
                   leaseSegments,
                   dlqSegments,
                   claimSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   waitingSegmentCursor,
                   leaseSegmentCursor,
                   dlqSegmentCursor,
                   terminalSegmentCursor,
                   claimSegmentCursor,
                   laneState>>
    /\ UnchangedClaimData

\* ADR-023 prune of the claim ring. The precondition
\* `\A j : claimSegmentOf[j] = seg => j \notin claimOpen` captures
\* rescue-before-truncate: every claim in the partition must be closed
\* (either by a terminal transition or by RescueStaleReceipt) before
\* `prune_oldest_claims` can TRUNCATE the lease_claims / lease_claim_closures
\* children for that slot.
PruneClaimSegment(seg) ==
    /\ seg \in ClaimSegments
    /\ claimSegments[seg] = "sealed"
    /\ seg # claimSegmentCursor
    /\ \A j \in Jobs : claimSegmentOf[j] = seg => j \notin claimOpen
    /\ claimSegmentOf' = [j \in Jobs |-> IF claimSegmentOf[j] = seg THEN NoClaimSegment ELSE claimSegmentOf[j]]
    /\ claimClosed' = claimClosed \ JobsInClaimSegment(seg)
    /\ claimSegments' = [claimSegments EXCEPT ![seg] = "pruned"]
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   waitingEntries,
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
                   deferredSegmentOf,
                   waitingSegmentOf,
                   leaseSegmentOf,
                   dlqSegmentOf,
                   terminalSegmentOf,
                   claimOpen,
                   readySegments,
                   deferredSegments,
                   waitingSegments,
                   leaseSegments,
                   dlqSegments,
                   terminalSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   waitingSegmentCursor,
                   leaseSegmentCursor,
                   dlqSegmentCursor,
                   terminalSegmentCursor,
                   claimSegmentCursor,
                   laneState>>

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
    \/ \E j \in Jobs : ResumeWaitingToReady(j)
    \/ \E j \in Jobs : TimeoutWaitingToReady(j)
    \/ \E j \in Jobs : TimeoutWaitingToDlq(j)
    \/ \E w \in Workers, j \in Jobs : FastComplete(w, j)
    \/ \E w \in Workers, j \in Jobs : StatefulComplete(w, j)
    \/ \E w \in Workers, j \in Jobs : FailToDlq(w, j)
    \/ \E w \in Workers, j \in Jobs : RetryToDeferred(w, j)
    \/ \E j \in Jobs : RescueToReady(j)
    \/ \E j \in Jobs : RescueStaleReceipt(j)
    \/ \E j \in Jobs : CancelWaitingToTerminal(j)
    \/ \E j \in Jobs : CancelRunningToTerminal(j)
    \/ \E j \in Jobs : CancelReceiptOnlyToTerminal(j)
    \/ \E w \in Workers, j \in Jobs : StaleCompleteRejected(w, j)
    \/ \E j \in Jobs : MoveFailedToDlq(j)
    \/ \E j \in Jobs : RetryFromDlq(j)
    \/ \E j \in Jobs : PurgeDlq(j)
    \/ RotateReadySegments
    \/ RotateDeferredSegments
    \/ RotateWaitingSegments
    \/ RotateLeaseSegments
    \/ RotateDlqSegments
    \/ RotateTerminalSegments
    \/ RotateClaimSegments
    \/ \E seg \in ReadySegments : PruneReadySegment(seg)
    \/ \E seg \in DeferredSegments : PruneDeferredSegment(seg)
    \/ \E seg \in WaitingSegments : PruneWaitingSegment(seg)
    \/ \E seg \in LeaseSegments : PruneLeaseSegment(seg)
    \/ \E seg \in DlqSegments : PruneDlqSegment(seg)
    \/ \E seg \in TerminalSegments : PruneTerminalSegment(seg)
    \/ \E seg \in ClaimSegments : PruneClaimSegment(seg)
    \/ Stutter

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ Jobs # {}
    /\ Workers # {}
    /\ ReadySegmentCount >= 1
    /\ DeferredSegmentCount >= 1
    /\ WaitingSegmentCount >= 1
    /\ LeaseSegmentCount >= 1
    /\ DlqSegmentCount >= 1
    /\ TerminalSegmentCount >= 1
    /\ ClaimSegmentCount >= 1
    /\ MaxRunLease >= 1
    /\ MaxAppendSeq >= Cardinality(Jobs)
    /\ readyEntries \subseteq Jobs
    /\ deferredEntries \subseteq Jobs
    /\ waitingEntries \subseteq Jobs
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
    /\ deferredSegmentOf \in [Jobs -> DeferredSegments \cup {NoDeferredSegment}]
    /\ waitingSegmentOf \in [Jobs -> WaitingSegments \cup {NoWaitingSegment}]
    /\ leaseSegmentOf \in [Jobs -> LeaseSegments \cup {NoLeaseSegment}]
    /\ dlqSegmentOf \in [Jobs -> DlqSegments \cup {NoDlqSegment}]
    /\ terminalSegmentOf \in [Jobs -> TerminalSegments \cup {NoTerminalSegment}]
    /\ claimSegmentOf \in [Jobs -> ClaimSegments \cup {NoClaimSegment}]
    /\ claimOpen \subseteq Jobs
    /\ claimClosed \subseteq Jobs
    /\ readySegments \in [ReadySegments -> SegmentStates]
    /\ deferredSegments \in [DeferredSegments -> SegmentStates]
    /\ waitingSegments \in [WaitingSegments -> SegmentStates]
    /\ leaseSegments \in [LeaseSegments -> SegmentStates]
    /\ dlqSegments \in [DlqSegments -> SegmentStates]
    /\ terminalSegments \in [TerminalSegments -> SegmentStates]
    /\ claimSegments \in [ClaimSegments -> SegmentStates]
    /\ readySegmentCursor \in ReadySegments
    /\ deferredSegmentCursor \in DeferredSegments
    /\ waitingSegmentCursor \in WaitingSegments
    /\ leaseSegmentCursor \in LeaseSegments
    /\ dlqSegmentCursor \in DlqSegments
    /\ terminalSegmentCursor \in TerminalSegments
    /\ claimSegmentCursor \in ClaimSegments
    /\ laneState.appendSeq \in 1..(MaxAppendSeq + 1)
    /\ laneState.claimSeq \in 1..(MaxAppendSeq + 1)
    /\ laneState.readyCount \in 0..Cardinality(Jobs)
    /\ laneState.leasedCount \in 0..Cardinality(Jobs)

OneOpenReadySegment == Cardinality({s \in ReadySegments : readySegments[s] = "open"}) = 1
OneOpenDeferredSegment == Cardinality({s \in DeferredSegments : deferredSegments[s] = "open"}) = 1
OneOpenWaitingSegment == Cardinality({s \in WaitingSegments : waitingSegments[s] = "open"}) = 1
OneOpenLeaseSegment == Cardinality({s \in LeaseSegments : leaseSegments[s] = "open"}) = 1
OneOpenDlqSegment == Cardinality({s \in DlqSegments : dlqSegments[s] = "open"}) = 1
OneOpenTerminalSegment == Cardinality({s \in TerminalSegments : terminalSegments[s] = "open"}) = 1
OneOpenClaimSegment == Cardinality({s \in ClaimSegments : claimSegments[s] = "open"}) = 1

ReadyCursorIsOpen == readySegments[readySegmentCursor] = "open"
DeferredCursorIsOpen == deferredSegments[deferredSegmentCursor] = "open"
WaitingCursorIsOpen == waitingSegments[waitingSegmentCursor] = "open"
LeaseCursorIsOpen == leaseSegments[leaseSegmentCursor] = "open"
DlqCursorIsOpen == dlqSegments[dlqSegmentCursor] = "open"
TerminalCursorIsOpen == terminalSegments[terminalSegmentCursor] = "open"
ClaimCursorIsOpen == claimSegments[claimSegmentCursor] = "open"

DeferredHasNoLiveRuntime ==
    /\ deferredEntries \cap activeLeases = {}
    /\ deferredEntries \cap attemptState = {}
    /\ deferredEntries \cap waitingEntries = {}

WaitingHasNoLiveLease == waitingEntries \cap activeLeases = {}
WaitingRequiresAttemptState == waitingEntries \subseteq attemptState

ActiveLeasesSubsetReadyEntries == activeLeases \subseteq readyEntries
AttemptStateRequiresLeaseOrWaiting == attemptState \subseteq (activeLeases \cup waitingEntries)

\* Heartbeat freshness is now tracked at the lease level, matching the
\* Rust implementation. Short jobs (no attempt_state) can still publish
\* heartbeats and be rescued.
FreshHeartbeatRequiresLease == heartbeatFresh \subseteq activeLeases

ProgressRequiresAttemptState == progressTouched \subseteq attemptState

TerminalHasNoLiveRuntime ==
    \A j \in Jobs : j \in terminalEntries =>
        /\ j \notin activeLeases
        /\ j \notin attemptState
        /\ j \notin waitingEntries
        /\ j \notin readyEntries
        /\ j \notin deferredEntries

DlqHasNoLiveRuntime ==
    \A j \in Jobs : j \in dlqEntries =>
        /\ j \notin activeLeases
        /\ j \notin attemptState
        /\ j \notin waitingEntries
        /\ j \notin readyEntries
        /\ j \notin deferredEntries

DlqAndTerminalDisjoint == dlqEntries \cap terminalEntries = {}

ReadyEntriesHaveLaneSeq ==
    \A j \in CurrentReady : laneSeq[j] # NoLaneSeq

DeferredEntriesClearLaneSeq == \A j \in deferredEntries : laneSeq[j] = NoLaneSeq
WaitingEntriesClearLaneSeq == \A j \in waitingEntries : laneSeq[j] = NoLaneSeq
DlqEntriesClearLaneSeq == \A j \in dlqEntries : laneSeq[j] = NoLaneSeq

ReadyLaneSeqUnique ==
    \A j1, j2 \in CurrentReady : j1 # j2 => laneSeq[j1] # laneSeq[j2]

ClaimCursorBounded == laneState.claimSeq <= laneState.appendSeq

CurrentReadyAtOrAheadOfCursor ==
    \A j \in CurrentReady : laneSeq[j] >= laneState.claimSeq

PrunedReadySegmentsAreEmpty ==
    \A j \in Jobs : readySegmentOf[j] # NoReadySegment => readySegments[readySegmentOf[j]] # "pruned"

PrunedDeferredSegmentsAreEmpty ==
    \A j \in Jobs : deferredSegmentOf[j] # NoDeferredSegment => deferredSegments[deferredSegmentOf[j]] # "pruned"

PrunedWaitingSegmentsAreEmpty ==
    \A j \in Jobs : waitingSegmentOf[j] # NoWaitingSegment => waitingSegments[waitingSegmentOf[j]] # "pruned"

PrunedLeaseSegmentsAreEmpty ==
    \A j \in Jobs : leaseSegmentOf[j] # NoLeaseSegment => leaseSegments[leaseSegmentOf[j]] # "pruned"

PrunedDlqSegmentsAreEmpty ==
    \A j \in Jobs : dlqSegmentOf[j] # NoDlqSegment => dlqSegments[dlqSegmentOf[j]] # "pruned"

PrunedTerminalSegmentsAreEmpty ==
    \A j \in Jobs : terminalSegmentOf[j] # NoTerminalSegment => terminalSegments[terminalSegmentOf[j]] # "pruned"

PrunedClaimSegmentsAreEmpty ==
    \A j \in Jobs : claimSegmentOf[j] # NoClaimSegment => claimSegments[claimSegmentOf[j]] # "pruned"

\* ADR-023 core safety: no open receipt is ever physically dropped. If a
\* claim is open (claimOpen), its partition is both non-zero and in an
\* un-pruned state (open or sealed, never pruned). This is what makes the
\* rotation-reclaim path safe: a prune can only fire once every claim in
\* the partition has closed (or been force-closed by RescueStaleReceipt).
NoLostClaim ==
    \A j \in claimOpen :
        /\ claimSegmentOf[j] # NoClaimSegment
        /\ claimSegments[claimSegmentOf[j]] # "pruned"

\* A claim is in at most one of the open / closed states. Moving between
\* them is a single atomic action (Claim opens, close transitions close,
\* prune clears); nothing leaves j in both.
ClaimOpenAndClosedDisjoint == claimOpen \cap claimClosed = {}

\* An open receipt corresponds to a bound claim segment. An untracked
\* claim (segmentOf = 0) cannot be open.
OpenClaimHasSegment ==
    \A j \in Jobs : j \in claimOpen => claimSegmentOf[j] # NoClaimSegment

ClosedClaimHasSegment ==
    \A j \in Jobs : j \in claimClosed => claimSegmentOf[j] # NoClaimSegment

LaneStateConsistent ==
    /\ laneState.readyCount = Cardinality(CurrentReady)
    /\ laneState.leasedCount = Cardinality(activeLeases)

TaskLeaseBounded ==
    \A w \in Workers, j \in Jobs : taskLease[w][j] <= runLease[j]

=============================================================================

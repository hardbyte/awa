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
\* - ready / deferred / waiting / lease / dlq segment families
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

CONSTANTS Jobs,
          Workers,
          ReadySegmentCount,
          DeferredSegmentCount,
          WaitingSegmentCount,
          LeaseSegmentCount,
          DlqSegmentCount,
          MaxRunLease,
          MaxAppendSeq

ReadySegments == 1..ReadySegmentCount
DeferredSegments == 1..DeferredSegmentCount
WaitingSegments == 1..WaitingSegmentCount
LeaseSegments == 1..LeaseSegmentCount
DlqSegments == 1..DlqSegmentCount
SegmentStates == {"open", "sealed", "pruned"}

NoWorker == "none"
NoReadySegment == 0
NoDeferredSegment == 0
NoWaitingSegment == 0
NoLeaseSegment == 0
NoDlqSegment == 0
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
          readySegments,
          deferredSegments,
          waitingSegments,
          leaseSegments,
          dlqSegments,
          readySegmentCursor,
          deferredSegmentCursor,
          waitingSegmentCursor,
          leaseSegmentCursor,
          dlqSegmentCursor,
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
          readySegments,
          deferredSegments,
          waitingSegments,
          leaseSegments,
          dlqSegments,
          readySegmentCursor,
          deferredSegmentCursor,
          waitingSegmentCursor,
          leaseSegmentCursor,
          dlqSegmentCursor,
          laneState>>

NextReadySegment(s) == IF s = ReadySegmentCount THEN 1 ELSE s + 1
NextDeferredSegment(s) == IF s = DeferredSegmentCount THEN 1 ELSE s + 1
NextWaitingSegment(s) == IF s = WaitingSegmentCount THEN 1 ELSE s + 1
NextLeaseSegment(s) == IF s = LeaseSegmentCount THEN 1 ELSE s + 1
NextDlqSegment(s) == IF s = DlqSegmentCount THEN 1 ELSE s + 1

CurrentReady == ((((readyEntries \ terminalEntries) \ activeLeases) \ deferredEntries) \ waitingEntries) \ dlqEntries

JobsInReadySegment(seg) == {j \in Jobs : readySegmentOf[j] = seg}
JobsInDeferredSegment(seg) == {j \in Jobs : deferredSegmentOf[j] = seg}
JobsInWaitingSegment(seg) == {j \in Jobs : waitingSegmentOf[j] = seg}
JobsInDlqSegment(seg) == {j \in Jobs : dlqSegmentOf[j] = seg}

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
    /\ readySegments = InitSegments(ReadySegments)
    /\ deferredSegments = InitSegments(DeferredSegments)
    /\ waitingSegments = InitSegments(WaitingSegments)
    /\ leaseSegments = InitSegments(LeaseSegments)
    /\ dlqSegments = InitSegments(DlqSegments)
    /\ readySegmentCursor = 1
    /\ deferredSegmentCursor = 1
    /\ waitingSegmentCursor = 1
    /\ leaseSegmentCursor = 1
    /\ dlqSegmentCursor = 1
    /\ laneState = [appendSeq |-> 1, claimSeq |-> 1, readyCount |-> 0, leasedCount |-> 0]

UnchangedSegmentState ==
    UNCHANGED <<readySegments,
                deferredSegments,
                waitingSegments,
                leaseSegments,
                dlqSegments,
                readySegmentCursor,
                deferredSegmentCursor,
                waitingSegmentCursor,
                leaseSegmentCursor,
                dlqSegmentCursor>>

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
                   dlqSegmentOf>>
    /\ UnchangedSegmentState

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
                   laneState>>
    /\ UnchangedSegmentState

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
                   dlqSegmentOf>>
    /\ UnchangedSegmentState

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
                   dlqSegmentOf>>
    /\ UnchangedSegmentState

Claim(w, j) ==
    /\ w \in Workers
    /\ j \in CurrentReady
    /\ laneSeq[j] = laneState.claimSeq
    /\ runLease[j] < MaxRunLease
    /\ leaseSegments[leaseSegmentCursor] = "open"
    /\ activeLeases' = activeLeases \cup {j}
    /\ leaseOwner' = [leaseOwner EXCEPT ![j] = w]
    /\ runLease' = [runLease EXCEPT ![j] = runLease[j] + 1]
    /\ taskLease' = [taskLease EXCEPT ![w][j] = runLease[j] + 1]
    /\ leaseSegmentOf' = [leaseSegmentOf EXCEPT ![j] = leaseSegmentCursor]
    /\ heartbeatFresh' = heartbeatFresh \cup {j}
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
                   dlqSegmentOf>>
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
                   laneState>>
    /\ UnchangedSegmentState

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
                   laneState>>
    /\ UnchangedSegmentState

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
                   laneState>>
    /\ UnchangedSegmentState

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
                   laneState>>
    /\ UnchangedSegmentState

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
                   dlqSegmentOf>>
    /\ UnchangedSegmentState

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
                   dlqSegmentOf>>
    /\ UnchangedSegmentState

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
                   dlqSegmentOf>>
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
                   laneState>>
    /\ UnchangedSegmentState

FastComplete(w, j) ==
    /\ w \in Workers
    /\ j \in activeLeases
    /\ leaseOwner[j] = w
    /\ taskLease[w][j] = runLease[j]
    /\ j \notin attemptState
    /\ terminalEntries' = terminalEntries \cup {j}
    /\ activeLeases' = activeLeases \ {j}
    /\ leaseOwner' = [leaseOwner EXCEPT ![j] = NoWorker]
    /\ taskLease' = [taskLease EXCEPT ![w][j] = 0]
    /\ heartbeatFresh' = heartbeatFresh \ {j}
    /\ readyEntries' = readyEntries \ {j}
    /\ laneSeq' = [laneSeq EXCEPT ![j] = NoLaneSeq]
    /\ readySegmentOf' = [readySegmentOf EXCEPT ![j] = NoReadySegment]
    /\ leaseSegmentOf' = [leaseSegmentOf EXCEPT ![j] = NoLeaseSegment]
    /\ laneState' = [laneState EXCEPT !.leasedCount = laneState.leasedCount - 1]
    /\ UNCHANGED <<deferredEntries,
                   waitingEntries,
                   dlqEntries,
                   runLease,
                   attemptState,
                   progressTouched,
                   deferredSegmentOf,
                   waitingSegmentOf,
                   dlqSegmentOf>>
    /\ UnchangedSegmentState

StatefulComplete(w, j) ==
    /\ w \in Workers
    /\ j \in activeLeases
    /\ leaseOwner[j] = w
    /\ taskLease[w][j] = runLease[j]
    /\ j \in attemptState
    /\ terminalEntries' = terminalEntries \cup {j}
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
    /\ laneState' = [laneState EXCEPT !.leasedCount = laneState.leasedCount - 1]
    /\ UNCHANGED <<deferredEntries,
                   waitingEntries,
                   dlqEntries,
                   runLease,
                   deferredSegmentOf,
                   waitingSegmentOf,
                   dlqSegmentOf>>
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
    /\ laneState' = [laneState EXCEPT !.leasedCount = laneState.leasedCount - 1]
    /\ UNCHANGED <<deferredEntries,
                   waitingEntries,
                   terminalEntries,
                   runLease,
                   deferredSegmentOf,
                   waitingSegmentOf>>
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
    /\ laneState' = [laneState EXCEPT !.leasedCount = laneState.leasedCount - 1]
    /\ UNCHANGED <<waitingEntries,
                   terminalEntries,
                   dlqEntries,
                   runLease,
                   waitingSegmentOf,
                   dlqSegmentOf>>
    /\ UnchangedSegmentState

\* Heartbeat or deadline rescue. No longer requires attempt_state, so
\* short jobs that claimed and immediately lost their heartbeat (e.g.
\* worker crash before the first heartbeat publish) are reachable here.
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
                   dlqSegmentOf>>
    /\ UnchangedSegmentState

CancelWaitingToTerminal(j) ==
    /\ j \in waitingEntries
    /\ terminalEntries' = terminalEntries \cup {j}
    /\ waitingEntries' = waitingEntries \ {j}
    /\ attemptState' = attemptState \ {j}
    /\ heartbeatFresh' = heartbeatFresh \ {j}
    /\ progressTouched' = progressTouched \ {j}
    /\ waitingSegmentOf' = [waitingSegmentOf EXCEPT ![j] = NoWaitingSegment]
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
                   laneState>>
    /\ UnchangedSegmentState

\* Admin-initiated move of a terminal row into the DLQ. Models the
\* `awa dlq move` command path.
MoveFailedToDlq(j) ==
    /\ j \in terminalEntries
    /\ j \notin dlqEntries
    /\ dlqSegments[dlqSegmentCursor] = "open"
    /\ terminalEntries' = terminalEntries \ {j}
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
                   leaseSegmentOf>>
    /\ UnchangedSegmentState

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
                   laneState>>
    /\ UnchangedSegmentState

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
                   deferredSegments,
                   waitingSegments,
                   leaseSegments,
                   dlqSegments,
                   deferredSegmentCursor,
                   waitingSegmentCursor,
                   leaseSegmentCursor,
                   dlqSegmentCursor,
                   laneState>>

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
                   readySegments,
                   waitingSegments,
                   leaseSegments,
                   dlqSegments,
                   readySegmentCursor,
                   waitingSegmentCursor,
                   leaseSegmentCursor,
                   dlqSegmentCursor,
                   laneState>>

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
                   readySegments,
                   deferredSegments,
                   leaseSegments,
                   dlqSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   leaseSegmentCursor,
                   dlqSegmentCursor,
                   laneState>>

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
                   readySegments,
                   deferredSegments,
                   waitingSegments,
                   dlqSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   waitingSegmentCursor,
                   dlqSegmentCursor,
                   laneState>>

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
                   readySegments,
                   deferredSegments,
                   waitingSegments,
                   leaseSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   waitingSegmentCursor,
                   leaseSegmentCursor,
                   laneState>>

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
                   deferredSegments,
                   waitingSegments,
                   leaseSegments,
                   dlqSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   waitingSegmentCursor,
                   leaseSegmentCursor,
                   dlqSegmentCursor,
                   laneState>>

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
                   readySegments,
                   waitingSegments,
                   leaseSegments,
                   dlqSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   waitingSegmentCursor,
                   leaseSegmentCursor,
                   dlqSegmentCursor,
                   laneState>>

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
                   readySegments,
                   deferredSegments,
                   leaseSegments,
                   dlqSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   waitingSegmentCursor,
                   leaseSegmentCursor,
                   dlqSegmentCursor,
                   laneState>>

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
                   readySegments,
                   deferredSegments,
                   waitingSegments,
                   dlqSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   waitingSegmentCursor,
                   leaseSegmentCursor,
                   dlqSegmentCursor,
                   laneState>>

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
                   readySegments,
                   deferredSegments,
                   waitingSegments,
                   leaseSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   waitingSegmentCursor,
                   leaseSegmentCursor,
                   dlqSegmentCursor,
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
    \/ \E j \in Jobs : CancelWaitingToTerminal(j)
    \/ \E w \in Workers, j \in Jobs : StaleCompleteRejected(w, j)
    \/ \E j \in Jobs : MoveFailedToDlq(j)
    \/ \E j \in Jobs : RetryFromDlq(j)
    \/ \E j \in Jobs : PurgeDlq(j)
    \/ RotateReadySegments
    \/ RotateDeferredSegments
    \/ RotateWaitingSegments
    \/ RotateLeaseSegments
    \/ RotateDlqSegments
    \/ \E seg \in ReadySegments : PruneReadySegment(seg)
    \/ \E seg \in DeferredSegments : PruneDeferredSegment(seg)
    \/ \E seg \in WaitingSegments : PruneWaitingSegment(seg)
    \/ \E seg \in LeaseSegments : PruneLeaseSegment(seg)
    \/ \E seg \in DlqSegments : PruneDlqSegment(seg)
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
    /\ readySegments \in [ReadySegments -> SegmentStates]
    /\ deferredSegments \in [DeferredSegments -> SegmentStates]
    /\ waitingSegments \in [WaitingSegments -> SegmentStates]
    /\ leaseSegments \in [LeaseSegments -> SegmentStates]
    /\ dlqSegments \in [DlqSegments -> SegmentStates]
    /\ readySegmentCursor \in ReadySegments
    /\ deferredSegmentCursor \in DeferredSegments
    /\ waitingSegmentCursor \in WaitingSegments
    /\ leaseSegmentCursor \in LeaseSegments
    /\ dlqSegmentCursor \in DlqSegments
    /\ laneState.appendSeq \in 1..(MaxAppendSeq + 1)
    /\ laneState.claimSeq \in 1..(MaxAppendSeq + 1)
    /\ laneState.readyCount \in 0..Cardinality(Jobs)
    /\ laneState.leasedCount \in 0..Cardinality(Jobs)

OneOpenReadySegment == Cardinality({s \in ReadySegments : readySegments[s] = "open"}) = 1
OneOpenDeferredSegment == Cardinality({s \in DeferredSegments : deferredSegments[s] = "open"}) = 1
OneOpenWaitingSegment == Cardinality({s \in WaitingSegments : waitingSegments[s] = "open"}) = 1
OneOpenLeaseSegment == Cardinality({s \in LeaseSegments : leaseSegments[s] = "open"}) = 1
OneOpenDlqSegment == Cardinality({s \in DlqSegments : dlqSegments[s] = "open"}) = 1

ReadyCursorIsOpen == readySegments[readySegmentCursor] = "open"
DeferredCursorIsOpen == deferredSegments[deferredSegmentCursor] = "open"
WaitingCursorIsOpen == waitingSegments[waitingSegmentCursor] = "open"
LeaseCursorIsOpen == leaseSegments[leaseSegmentCursor] = "open"
DlqCursorIsOpen == dlqSegments[dlqSegmentCursor] = "open"

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

LaneStateConsistent ==
    /\ laneState.readyCount = Cardinality(CurrentReady)
    /\ laneState.leasedCount = Cardinality(activeLeases)

TaskLeaseBounded ==
    \A w \in Workers, j \in Jobs : taskLease[w][j] <= runLease[j]

=============================================================================

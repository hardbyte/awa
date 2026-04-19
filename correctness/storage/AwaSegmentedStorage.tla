---- MODULE AwaSegmentedStorage ----
EXTENDS TLC, Naturals, FiniteSets

\* Focused storage model for the segmented 0.6+ runtime layout.
\*
\* It checks the relationships behind:
\* - ready_entries
\* - deferred_entries
\* - active_leases
\* - attempt_state
\* - terminal_entries
\* - lane_state
\* - ready_segments / ready_segment_cursor
\* - deferred_segments / deferred_segment_cursor
\* - lease_segments / lease_segment_cursor

Jobs == {"j1", "j2"}
Workers == {"w1"}

ReadySegments == 1..2
DeferredSegments == 1..2
LeaseSegments == 1..2
SegmentStates == {"open", "sealed", "pruned"}

NoWorker == "none"
NoReadySegment == 0
NoDeferredSegment == 0
NoLeaseSegment == 0
NoLaneSeq == 0

MaxRunLease == 2
MaxAppendSeq == 4

VARIABLES readyEntries,
          deferredEntries,
          terminalEntries,
          activeLeases,
          leaseOwner,
          runLease,
          taskLease,
          attemptState,
          heartbeatFresh,
          waitingExternal,
          progressTouched,
          laneSeq,
          readySegmentOf,
          deferredSegmentOf,
          leaseSegmentOf,
          readySegments,
          deferredSegments,
          leaseSegments,
          readySegmentCursor,
          deferredSegmentCursor,
          leaseSegmentCursor,
          laneState

vars == <<readyEntries,
          deferredEntries,
          terminalEntries,
          activeLeases,
          leaseOwner,
          runLease,
          taskLease,
          attemptState,
          heartbeatFresh,
          waitingExternal,
          progressTouched,
          laneSeq,
          readySegmentOf,
          deferredSegmentOf,
          leaseSegmentOf,
          readySegments,
          deferredSegments,
          leaseSegments,
          readySegmentCursor,
          deferredSegmentCursor,
          leaseSegmentCursor,
          laneState>>

NextReadySegment(s) == IF s = 2 THEN 1 ELSE s + 1
NextDeferredSegment(s) == IF s = 2 THEN 1 ELSE s + 1
NextLeaseSegment(s) == IF s = 2 THEN 1 ELSE s + 1

CurrentReady == (readyEntries \ terminalEntries) \ activeLeases
JobsInReadySegment(seg) == {j \in Jobs : readySegmentOf[j] = seg}
JobsInDeferredSegment(seg) == {j \in Jobs : deferredSegmentOf[j] = seg}

Init ==
    /\ readyEntries = {}
    /\ deferredEntries = {}
    /\ terminalEntries = {}
    /\ activeLeases = {}
    /\ leaseOwner = [j \in Jobs |-> NoWorker]
    /\ runLease = [j \in Jobs |-> 0]
    /\ taskLease = [w \in Workers |-> [j \in Jobs |-> 0]]
    /\ attemptState = {}
    /\ heartbeatFresh = {}
    /\ waitingExternal = {}
    /\ progressTouched = {}
    /\ laneSeq = [j \in Jobs |-> NoLaneSeq]
    /\ readySegmentOf = [j \in Jobs |-> NoReadySegment]
    /\ deferredSegmentOf = [j \in Jobs |-> NoDeferredSegment]
    /\ leaseSegmentOf = [j \in Jobs |-> NoLeaseSegment]
    /\ readySegments = [s \in ReadySegments |-> IF s = 1 THEN "open" ELSE "pruned"]
    /\ deferredSegments = [s \in DeferredSegments |-> IF s = 1 THEN "open" ELSE "pruned"]
    /\ leaseSegments = [s \in LeaseSegments |-> IF s = 1 THEN "open" ELSE "pruned"]
    /\ readySegmentCursor = 1
    /\ deferredSegmentCursor = 1
    /\ leaseSegmentCursor = 1
    /\ laneState = [appendSeq |-> 1, claimSeq |-> 1, readyCount |-> 0, leasedCount |-> 0]

EnqueueReady(j) ==
    /\ j \in Jobs
    /\ runLease[j] = 0
    /\ j \notin readyEntries
    /\ j \notin deferredEntries
    /\ j \notin terminalEntries
    /\ laneState.appendSeq <= MaxAppendSeq
    /\ readySegments[readySegmentCursor] = "open"
    /\ readyEntries' = readyEntries \cup {j}
    /\ laneSeq' = [laneSeq EXCEPT ![j] = laneState.appendSeq]
    /\ readySegmentOf' = [readySegmentOf EXCEPT ![j] = readySegmentCursor]
    /\ laneState' = [laneState EXCEPT
                        !.appendSeq = laneState.appendSeq + 1,
                        !.readyCount = laneState.readyCount + 1]
    /\ UNCHANGED <<deferredEntries,
                   terminalEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   waitingExternal,
                   progressTouched,
                   deferredSegmentOf,
                   leaseSegmentOf,
                   readySegments,
                   deferredSegments,
                   leaseSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   leaseSegmentCursor>>

EnqueueDeferred(j) ==
    /\ j \in Jobs
    /\ runLease[j] = 0
    /\ j \notin readyEntries
    /\ j \notin deferredEntries
    /\ j \notin terminalEntries
    /\ deferredSegments[deferredSegmentCursor] = "open"
    /\ deferredEntries' = deferredEntries \cup {j}
    /\ deferredSegmentOf' = [deferredSegmentOf EXCEPT ![j] = deferredSegmentCursor]
    /\ UNCHANGED <<readyEntries,
                   terminalEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   waitingExternal,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   leaseSegmentOf,
                   readySegments,
                   deferredSegments,
                   leaseSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   leaseSegmentCursor,
                   laneState>>

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
    /\ UNCHANGED <<terminalEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   waitingExternal,
                   progressTouched,
                   leaseSegmentOf,
                   readySegments,
                   deferredSegments,
                   leaseSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   leaseSegmentCursor>>

AdvanceClaimCursor ==
    /\ laneState.claimSeq < laneState.appendSeq
    /\ ~ (\E j \in CurrentReady : laneSeq[j] = laneState.claimSeq)
    /\ laneState' = [laneState EXCEPT !.claimSeq = laneState.claimSeq + 1]
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   terminalEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   waitingExternal,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   deferredSegmentOf,
                   leaseSegmentOf,
                   readySegments,
                   deferredSegments,
                   leaseSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   leaseSegmentCursor>>

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
    /\ laneState' = [laneState EXCEPT
                        !.claimSeq = laneState.claimSeq + 1,
                        !.readyCount = laneState.readyCount - 1,
                        !.leasedCount = laneState.leasedCount + 1]
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   terminalEntries,
                   attemptState,
                   heartbeatFresh,
                   waitingExternal,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   deferredSegmentOf,
                   readySegments,
                   deferredSegments,
                   leaseSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   leaseSegmentCursor>>

MaterializeAttemptState(j) ==
    /\ j \in activeLeases
    /\ j \notin attemptState
    /\ attemptState' = attemptState \cup {j}
    /\ heartbeatFresh' = heartbeatFresh \cup {j}
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   terminalEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   waitingExternal,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   deferredSegmentOf,
                   leaseSegmentOf,
                   readySegments,
                   deferredSegments,
                   leaseSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   leaseSegmentCursor,
                   laneState>>

Heartbeat(j) ==
    /\ j \in attemptState
    /\ heartbeatFresh' = heartbeatFresh \cup {j}
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   terminalEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   waitingExternal,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   deferredSegmentOf,
                   leaseSegmentOf,
                   readySegments,
                   deferredSegments,
                   leaseSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   leaseSegmentCursor,
                   laneState>>

LoseHeartbeat(j) ==
    /\ j \in attemptState
    /\ j \in heartbeatFresh
    /\ heartbeatFresh' = heartbeatFresh \ {j}
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   terminalEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   waitingExternal,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   deferredSegmentOf,
                   leaseSegmentOf,
                   readySegments,
                   deferredSegments,
                   leaseSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   leaseSegmentCursor,
                   laneState>>

ProgressFlush(j) ==
    /\ j \in attemptState
    /\ progressTouched' = progressTouched \cup {j}
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   terminalEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   waitingExternal,
                   laneSeq,
                   readySegmentOf,
                   deferredSegmentOf,
                   leaseSegmentOf,
                   readySegments,
                   deferredSegments,
                   leaseSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   leaseSegmentCursor,
                   laneState>>

RegisterWait(j) ==
    /\ j \in attemptState
    /\ waitingExternal' = waitingExternal \cup {j}
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
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
                   leaseSegmentOf,
                   readySegments,
                   deferredSegments,
                   leaseSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   leaseSegmentCursor,
                   laneState>>

ResolveWait(j) ==
    /\ j \in waitingExternal
    /\ waitingExternal' = waitingExternal \ {j}
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
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
                   leaseSegmentOf,
                   readySegments,
                   deferredSegments,
                   leaseSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   leaseSegmentCursor,
                   laneState>>

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
    /\ leaseSegmentOf' = [leaseSegmentOf EXCEPT ![j] = NoLeaseSegment]
    /\ laneState' = [laneState EXCEPT !.leasedCount = laneState.leasedCount - 1]
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   runLease,
                   attemptState,
                   heartbeatFresh,
                   waitingExternal,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   deferredSegmentOf,
                   readySegments,
                   deferredSegments,
                   leaseSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   leaseSegmentCursor>>

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
    /\ waitingExternal' = waitingExternal \ {j}
    /\ progressTouched' = progressTouched \ {j}
    /\ leaseSegmentOf' = [leaseSegmentOf EXCEPT ![j] = NoLeaseSegment]
    /\ laneState' = [laneState EXCEPT !.leasedCount = laneState.leasedCount - 1]
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   runLease,
                   laneSeq,
                   readySegmentOf,
                   deferredSegmentOf,
                   readySegments,
                   deferredSegments,
                   leaseSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   leaseSegmentCursor>>

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
    /\ waitingExternal' = waitingExternal \ {j}
    /\ progressTouched' = progressTouched \ {j}
    /\ laneSeq' = [laneSeq EXCEPT ![j] = NoLaneSeq]
    /\ readySegmentOf' = [readySegmentOf EXCEPT ![j] = NoReadySegment]
    /\ deferredSegmentOf' = [deferredSegmentOf EXCEPT ![j] = deferredSegmentCursor]
    /\ leaseSegmentOf' = [leaseSegmentOf EXCEPT ![j] = NoLeaseSegment]
    /\ laneState' = [laneState EXCEPT !.leasedCount = laneState.leasedCount - 1]
    /\ UNCHANGED <<terminalEntries,
                   runLease,
                   readySegments,
                   deferredSegments,
                   leaseSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   leaseSegmentCursor>>

RescueToReady(j) ==
    /\ j \in activeLeases
    /\ j \in attemptState
    /\ j \notin heartbeatFresh
    /\ laneState.appendSeq <= MaxAppendSeq
    /\ readySegments[readySegmentCursor] = "open"
    /\ activeLeases' = activeLeases \ {j}
    /\ readyEntries' = readyEntries \cup {j}
    /\ leaseOwner' = [leaseOwner EXCEPT ![j] = NoWorker]
    /\ attemptState' = attemptState \ {j}
    /\ waitingExternal' = waitingExternal \ {j}
    /\ progressTouched' = progressTouched \ {j}
    /\ laneSeq' = [laneSeq EXCEPT ![j] = laneState.appendSeq]
    /\ readySegmentOf' = [readySegmentOf EXCEPT ![j] = readySegmentCursor]
    /\ leaseSegmentOf' = [leaseSegmentOf EXCEPT ![j] = NoLeaseSegment]
    /\ laneState' = [laneState EXCEPT
                        !.appendSeq = laneState.appendSeq + 1,
                        !.readyCount = laneState.readyCount + 1,
                        !.leasedCount = laneState.leasedCount - 1]
    /\ UNCHANGED <<deferredEntries,
                   terminalEntries,
                   runLease,
                   taskLease,
                   heartbeatFresh,
                   deferredSegmentOf,
                   readySegments,
                   deferredSegments,
                   leaseSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   leaseSegmentCursor>>

StaleCompleteRejected(w, j) ==
    /\ w \in Workers
    /\ j \in Jobs
    /\ taskLease[w][j] # 0
    /\ j \notin activeLeases \/ taskLease[w][j] # runLease[j]
    /\ taskLease' = [taskLease EXCEPT ![w][j] = 0]
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   terminalEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   attemptState,
                   heartbeatFresh,
                   waitingExternal,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   deferredSegmentOf,
                   leaseSegmentOf,
                   readySegments,
                   deferredSegments,
                   leaseSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   leaseSegmentCursor,
                   laneState>>

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
                   terminalEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   waitingExternal,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   deferredSegmentOf,
                   leaseSegmentOf,
                   deferredSegments,
                   leaseSegments,
                   deferredSegmentCursor,
                   leaseSegmentCursor,
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
                   terminalEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   waitingExternal,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   deferredSegmentOf,
                   leaseSegmentOf,
                   readySegments,
                   leaseSegments,
                   readySegmentCursor,
                   leaseSegmentCursor,
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
                   terminalEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   waitingExternal,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   deferredSegmentOf,
                   leaseSegmentOf,
                   readySegments,
                   deferredSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   laneState>>

PruneReadySegment(seg) ==
    /\ seg \in ReadySegments
    /\ readySegments[seg] = "sealed"
    /\ \A j \in Jobs : readySegmentOf[j] = seg => j \in terminalEntries
    /\ readyEntries' = readyEntries \ JobsInReadySegment(seg)
    /\ laneSeq' = [j \in Jobs |-> IF readySegmentOf[j] = seg THEN NoLaneSeq ELSE laneSeq[j]]
    /\ readySegmentOf' = [j \in Jobs |-> IF readySegmentOf[j] = seg THEN NoReadySegment ELSE readySegmentOf[j]]
    /\ readySegments' = [readySegments EXCEPT ![seg] = "pruned"]
    /\ UNCHANGED <<deferredEntries,
                   terminalEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   waitingExternal,
                   progressTouched,
                   deferredSegmentOf,
                   leaseSegmentOf,
                   deferredSegments,
                   leaseSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   leaseSegmentCursor,
                   laneState>>

PruneDeferredSegment(seg) ==
    /\ seg \in DeferredSegments
    /\ deferredSegments[seg] = "sealed"
    /\ \A j \in Jobs : deferredSegmentOf[j] = seg => j \notin deferredEntries
    /\ deferredSegmentOf' = [j \in Jobs |-> IF deferredSegmentOf[j] = seg THEN NoDeferredSegment ELSE deferredSegmentOf[j]]
    /\ deferredSegments' = [deferredSegments EXCEPT ![seg] = "pruned"]
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   terminalEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   waitingExternal,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   leaseSegmentOf,
                   readySegments,
                   leaseSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   leaseSegmentCursor,
                   laneState>>

PruneLeaseSegment(seg) ==
    /\ seg \in LeaseSegments
    /\ leaseSegments[seg] = "sealed"
    /\ \A j \in Jobs : leaseSegmentOf[j] = seg => j \notin activeLeases /\ j \notin attemptState
    /\ leaseSegmentOf' = [j \in Jobs |-> IF leaseSegmentOf[j] = seg THEN NoLeaseSegment ELSE leaseSegmentOf[j]]
    /\ leaseSegments' = [leaseSegments EXCEPT ![seg] = "pruned"]
    /\ UNCHANGED <<readyEntries,
                   deferredEntries,
                   terminalEntries,
                   activeLeases,
                   leaseOwner,
                   runLease,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   waitingExternal,
                   progressTouched,
                   laneSeq,
                   readySegmentOf,
                   deferredSegmentOf,
                   readySegments,
                   deferredSegments,
                   readySegmentCursor,
                   deferredSegmentCursor,
                   leaseSegmentCursor,
                   laneState>>

Stutter ==
    /\ UNCHANGED vars

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
    \/ \E j \in Jobs : RegisterWait(j)
    \/ \E j \in Jobs : ResolveWait(j)
    \/ \E w \in Workers, j \in Jobs : FastComplete(w, j)
    \/ \E w \in Workers, j \in Jobs : StatefulComplete(w, j)
    \/ \E w \in Workers, j \in Jobs : RetryToDeferred(w, j)
    \/ \E j \in Jobs : RescueToReady(j)
    \/ \E w \in Workers, j \in Jobs : StaleCompleteRejected(w, j)
    \/ RotateReadySegments
    \/ RotateDeferredSegments
    \/ RotateLeaseSegments
    \/ \E seg \in ReadySegments : PruneReadySegment(seg)
    \/ \E seg \in DeferredSegments : PruneDeferredSegment(seg)
    \/ \E seg \in LeaseSegments : PruneLeaseSegment(seg)
    \/ Stutter

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ readyEntries \subseteq Jobs
    /\ deferredEntries \subseteq Jobs
    /\ terminalEntries \subseteq Jobs
    /\ activeLeases \subseteq Jobs
    /\ leaseOwner \in [Jobs -> Workers \cup {NoWorker}]
    /\ runLease \in [Jobs -> 0..MaxRunLease]
    /\ taskLease \in [Workers -> [Jobs -> 0..MaxRunLease]]
    /\ attemptState \subseteq Jobs
    /\ heartbeatFresh \subseteq Jobs
    /\ waitingExternal \subseteq Jobs
    /\ progressTouched \subseteq Jobs
    /\ laneSeq \in [Jobs -> 0..MaxAppendSeq]
    /\ readySegmentOf \in [Jobs -> ReadySegments \cup {NoReadySegment}]
    /\ deferredSegmentOf \in [Jobs -> DeferredSegments \cup {NoDeferredSegment}]
    /\ leaseSegmentOf \in [Jobs -> LeaseSegments \cup {NoLeaseSegment}]
    /\ readySegments \in [ReadySegments -> SegmentStates]
    /\ deferredSegments \in [DeferredSegments -> SegmentStates]
    /\ leaseSegments \in [LeaseSegments -> SegmentStates]
    /\ readySegmentCursor \in ReadySegments
    /\ deferredSegmentCursor \in DeferredSegments
    /\ leaseSegmentCursor \in LeaseSegments
    /\ laneState.appendSeq \in 1..(MaxAppendSeq + 1)
    /\ laneState.claimSeq \in 1..(MaxAppendSeq + 1)
    /\ laneState.readyCount \in 0..Cardinality(Jobs)
    /\ laneState.leasedCount \in 0..Cardinality(Jobs)

OneOpenReadySegment == Cardinality({s \in ReadySegments : readySegments[s] = "open"}) = 1
OneOpenDeferredSegment == Cardinality({s \in DeferredSegments : deferredSegments[s] = "open"}) = 1
OneOpenLeaseSegment == Cardinality({s \in LeaseSegments : leaseSegments[s] = "open"}) = 1

ReadyCursorIsOpen == readySegments[readySegmentCursor] = "open"
DeferredCursorIsOpen == deferredSegments[deferredSegmentCursor] = "open"
LeaseCursorIsOpen == leaseSegments[leaseSegmentCursor] = "open"

ReadyAndDeferredDisjoint == readyEntries \cap deferredEntries = {}
DeferredHasNoLiveRuntime ==
    /\ deferredEntries \cap activeLeases = {}
    /\ deferredEntries \cap attemptState = {}
    /\ deferredEntries \cap terminalEntries = {}

ActiveLeasesSubsetReadyEntries == activeLeases \subseteq readyEntries
AttemptStateRequiresLease == attemptState \subseteq activeLeases
FreshHeartbeatRequiresAttemptState == heartbeatFresh \subseteq attemptState
WaitingRequiresAttemptState == waitingExternal \subseteq attemptState
ProgressRequiresAttemptState == progressTouched \subseteq attemptState

TerminalHasNoLiveRuntime ==
    \A j \in Jobs : j \in terminalEntries => j \notin activeLeases /\ j \notin attemptState

ReadyEntriesHaveLaneSeq ==
    \A j \in Jobs : j \in readyEntries => laneSeq[j] # NoLaneSeq

NonReadyEntriesClearLaneSeq ==
    \A j \in Jobs : j \notin readyEntries => laneSeq[j] = NoLaneSeq

ReadyLaneSeqUnique ==
    \A j1, j2 \in readyEntries : j1 # j2 => laneSeq[j1] # laneSeq[j2]

ClaimCursorBounded == laneState.claimSeq <= laneState.appendSeq

CurrentReadyAtOrAheadOfCursor ==
    \A j \in CurrentReady : laneSeq[j] >= laneState.claimSeq

PrunedReadySegmentsAreEmpty ==
    \A j \in Jobs : readySegmentOf[j] # NoReadySegment => readySegments[readySegmentOf[j]] # "pruned"

PrunedDeferredSegmentsAreEmpty ==
    \A j \in Jobs : deferredSegmentOf[j] # NoDeferredSegment => deferredSegments[deferredSegmentOf[j]] # "pruned"

PrunedLeaseSegmentsAreEmpty ==
    \A j \in Jobs : leaseSegmentOf[j] # NoLeaseSegment => leaseSegments[leaseSegmentOf[j]] # "pruned"

LaneStateConsistent ==
    /\ laneState.readyCount = Cardinality(CurrentReady)
    /\ laneState.leasedCount = Cardinality(activeLeases)

TaskLeaseBounded ==
    \A w \in Workers, j \in Jobs : taskLease[w][j] <= runLease[j]

=============================================================================

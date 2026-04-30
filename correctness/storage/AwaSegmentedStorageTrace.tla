---- MODULE AwaSegmentedStorageTrace ----
EXTENDS AwaSegmentedStorage, Sequences

\* Trace-validation harness for AwaSegmentedStorage.
\*
\* Given a concrete sequence of logical events transcribed from a real
\* queue-storage runtime test, this spec checks that each transition is
\* a valid firing of the corresponding base spec action. It is a
\* single-threaded replay: at each step, the only enabled action is the
\* one named by the trace event. If the base spec action's precondition
\* fails at the current state, TLC reports deadlock before reaching the
\* end of the trace.
\*
\* The first transcribed test is `test_queue_storage_runtime_snooze` in
\* awa/tests/queue_storage_runtime_test.rs. In spec terms the
\* lifecycle is:
\*   EnqueueReady(j1)
\*   Claim(w1, j1)
\*   RetryToDeferred(w1, j1)     \* SnoozeOnceWorker returns snooze
\*   PromoteDeferred(j1)         \* maintenance promote
\*   Claim(w1, j1)               \* second claim
\*   FastComplete(w1, j1)        \* second attempt returns Ok, short path
\*
\* The trace does NOT model timing, heartbeat updates, rotation, or
\* prune — those are maintenance-side noise the test tolerates but that
\* don't affect the lifecycle invariants. A richer trace could include
\* them; see MAPPING.md for how to extend.

VARIABLE traceIdx

\* Transcribed trace from test_queue_storage_runtime_snooze. Edit or
\* add sibling traces to run against a different test.
SnoozeTrace == <<
    [action |-> "EnqueueReady",    job |-> "j1"],
    [action |-> "Claim",           worker |-> "w1", job |-> "j1"],
    [action |-> "RetryToDeferred", worker |-> "w1", job |-> "j1"],
    [action |-> "PromoteDeferred", job |-> "j1"],
    [action |-> "Claim",           worker |-> "w1", job |-> "j1"],
    [action |-> "FastComplete",    worker |-> "w1", job |-> "j1"]
>>

\* Receipt-rescue trace. The base storage spec's Claim action materializes
\* a lease immediately. The real receipt plane has a short receipt-only
\* window before materialization, so this trace uses one trace-only seed
\* action after EnqueueReady to put the model into that receipt-only open
\* claim state, then validates that RescueStaleReceipt closes it.
ReceiptRescueTrace == <<
    [action |-> "EnqueueReady",             job |-> "j1"],
    [action |-> "SeedOpenReceiptOnlyClaim", worker |-> "w1", job |-> "j1"],
    \* SeedOpenReceiptOnlyClaim bumped runLease to 1, so the open receipt
    \* lives under (j1, 1). Rescue closes that exact key.
    [action |-> "RescueStaleReceipt",       job |-> "j1", run_lease |-> 1]
>>

\* Running admin-cancel trace: enqueue, claim a lease-backed running job,
\* admin-cancel it into terminal_entries, and close the matching receipt.
RunningCancelTrace == <<
    [action |-> "EnqueueReady",            job |-> "j1"],
    [action |-> "Claim",                   worker |-> "w1", job |-> "j1"],
    [action |-> "CancelRunningToTerminal", job |-> "j1"]
>>

\* DLQ retry trace: executor terminal failure lands in dlq_entries,
\* retry_from_dlq re-appends to ready_entries preserving the existing
\* run_lease counter (matching the Rust path: the DLQ row's run_lease
\* is reinserted as-is so the next claim's `run_lease + 1` continues
\* monotonically), and the revived job can be claimed and completed.
DlqRetryTrace == <<
    [action |-> "EnqueueReady", job |-> "j1"],
    [action |-> "Claim",        worker |-> "w1", job |-> "j1"],
    [action |-> "FailToDlq",    worker |-> "w1", job |-> "j1"],
    [action |-> "RetryFromDlq", job |-> "j1"],
    [action |-> "Claim",        worker |-> "w1", job |-> "j1"],
    [action |-> "FastComplete", worker |-> "w1", job |-> "j1"]
>>

\* Deliberately-broken trace: steps 3 and 4 are swapped so PromoteDeferred
\* fires before RetryToDeferred has moved the job into the deferred
\* family. TLC is expected to report deadlock at traceIdx = 2 when
\* PromoteDeferred's precondition (`j \in deferredEntries`) fails.
BrokenTrace == <<
    [action |-> "EnqueueReady",    job |-> "j1"],
    [action |-> "Claim",           worker |-> "w1", job |-> "j1"],
    [action |-> "PromoteDeferred", job |-> "j1"],
    [action |-> "RetryToDeferred", worker |-> "w1", job |-> "j1"],
    [action |-> "Claim",           worker |-> "w1", job |-> "j1"],
    [action |-> "FastComplete",    worker |-> "w1", job |-> "j1"]
>>

\* Trace-only scaffolding for the receipt-only short path. It represents
\* a claim receipt that has advanced the claim cursor and left an open
\* receipt, but has no materialized lease row. The main spec keeps this
\* window abstract; the trace harness needs it to replay receipt rescue.
SeedOpenReceiptOnlyClaim(w, j) ==
    LET newKey == <<j, runLease[j] + 1>>
    IN
    /\ w \in Workers
    /\ j \in CurrentReady
    /\ laneSeq[j] = laneState.claimSeq
    /\ runLease[j] < MaxRunLease
    /\ claimSegments[claimSegmentCursor] = "open"
    /\ readyEntries' = readyEntries \ {j}
    /\ runLease' = [runLease EXCEPT ![j] = runLease[j] + 1]
    /\ laneSeq' = [laneSeq EXCEPT ![j] = NoLaneSeq]
    /\ readySegmentOf' = [readySegmentOf EXCEPT ![j] = NoReadySegment]
    /\ claimSegmentOf' = [claimSegmentOf EXCEPT ![newKey] = claimSegmentCursor]
    /\ claimOpen' = claimOpen \cup {newKey}
    /\ UNCHANGED claimClosed
    /\ laneState' = [laneState EXCEPT
                        !.claimSeq = laneState.claimSeq + 1,
                        !.readyCount = laneState.readyCount - 1]
    /\ UNCHANGED <<deferredEntries,
                   waitingLeases,
                   terminalEntries,
                   dlqEntries,
                   activeLeases,
                   leaseOwner,
                   taskLease,
                   attemptState,
                   heartbeatFresh,
                   progressTouched,
                   leaseSegmentOf,
                   terminalSegmentOf>>
    /\ UnchangedSegmentState

\* Fire the base spec action matching trace[n]. Events that take a
\* (worker, job) pair read both ev.worker and ev.job. Events that take
\* only a job read ev.job.
TraceStep(trace, n) ==
    LET ev == trace[n] IN
    /\ traceIdx' = n
    /\ \/ (ev.action = "EnqueueReady" /\ EnqueueReady(ev.job))
       \/ (ev.action = "SeedOpenReceiptOnlyClaim" /\
              SeedOpenReceiptOnlyClaim(ev.worker, ev.job))
       \/ (ev.action = "Claim" /\ Claim(ev.worker, ev.job))
       \/ (ev.action = "RetryToDeferred" /\ RetryToDeferred(ev.worker, ev.job))
       \/ (ev.action = "PromoteDeferred" /\ PromoteDeferred(ev.job))
       \/ (ev.action = "FastComplete" /\ FastComplete(ev.worker, ev.job))
       \/ (ev.action = "StatefulComplete" /\ StatefulComplete(ev.worker, ev.job))
       \/ (ev.action = "MaterializeAttemptState" /\ MaterializeAttemptState(ev.job))
       \/ (ev.action = "Heartbeat" /\ Heartbeat(ev.job))
       \/ (ev.action = "LoseHeartbeat" /\ LoseHeartbeat(ev.job))
       \/ (ev.action = "ProgressFlush" /\ ProgressFlush(ev.job))
       \/ (ev.action = "ParkToWaiting" /\ ParkToWaiting(ev.worker, ev.job))
       \/ (ev.action = "ResumeWaitingToRunning" /\ ResumeWaitingToRunning(ev.job))
       \/ (ev.action = "TimeoutWaitingToReady" /\ TimeoutWaitingToReady(ev.job))
       \/ (ev.action = "TimeoutWaitingToDlq" /\ TimeoutWaitingToDlq(ev.job))
       \/ (ev.action = "FailToDlq" /\ FailToDlq(ev.worker, ev.job))
       \/ (ev.action = "RescueToReady" /\ RescueToReady(ev.job))
       \/ (ev.action = "RescueStaleReceipt" /\ RescueStaleReceipt(ev.job, ev.run_lease))
       \/ (ev.action = "CancelWaitingToTerminal" /\ CancelWaitingToTerminal(ev.job))
       \/ (ev.action = "CancelRunningToTerminal" /\ CancelRunningToTerminal(ev.job))
       \/ (ev.action = "CancelReceiptOnlyToTerminal" /\ CancelReceiptOnlyToTerminal(ev.job))
       \/ (ev.action = "MoveFailedToDlq" /\ MoveFailedToDlq(ev.job))
       \/ (ev.action = "RetryFromDlq" /\ RetryFromDlq(ev.job))
       \/ (ev.action = "PurgeDlq" /\ PurgeDlq(ev.job))

TraceInit ==
    /\ Init
    /\ traceIdx = 0

\* Advance through trace, stuttering once complete so TLC does not
\* report deadlock on valid traces. If any mid-trace step fails, no
\* disjunct is enabled and TLC reports deadlock at a state with
\* traceIdx < Len(trace) — that state's traceIdx names the failing step.
TraceNextFor(trace) ==
    \/ (traceIdx < Len(trace) /\ TraceStep(trace, traceIdx + 1))
    \/ (traceIdx = Len(trace) /\ UNCHANGED <<vars, traceIdx>>)

\* Per-trace specifications. The config file selects one via
\* `SPECIFICATION SpecSnooze`, etc.
SpecSnooze == TraceInit /\ [][TraceNextFor(SnoozeTrace)]_<<vars, traceIdx>>
SpecReceiptRescue == TraceInit /\ [][TraceNextFor(ReceiptRescueTrace)]_<<vars, traceIdx>>
SpecRunningCancel == TraceInit /\ [][TraceNextFor(RunningCancelTrace)]_<<vars, traceIdx>>
SpecDlqRetry == TraceInit /\ [][TraceNextFor(DlqRetryTrace)]_<<vars, traceIdx>>
SpecBroken == TraceInit /\ [][TraceNextFor(BrokenTrace)]_<<vars, traceIdx>>

\* Positive witness invariants: negated so "violation" means acceptance.
\* If the trace is fully consumed, traceIdx reaches Len(trace), making
\* the negated predicate false and tripping a TLC-reported violation.
\* That "violation" is the success signal. For the broken trace, TLC
\* reports actual deadlock (no enabled disjunct) BEFORE reaching the
\* end — the deadlock at traceIdx < Len proves the rejection.
SnoozeTraceIncomplete == traceIdx < Len(SnoozeTrace)
ReceiptRescueTraceIncomplete == traceIdx < Len(ReceiptRescueTrace)
RunningCancelTraceIncomplete == traceIdx < Len(RunningCancelTrace)
DlqRetryTraceIncomplete == traceIdx < Len(DlqRetryTrace)
BrokenTraceIncomplete == traceIdx < Len(BrokenTrace)

\* All other safety invariants (TypeOK, DlqHasNoLiveRuntime, etc.) are
\* inherited from AwaSegmentedStorage and must hold throughout any
\* trace the replay accepts.

=============================================================================

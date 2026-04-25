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
\* The transcribed test is `test_queue_storage_runtime_snooze` in
\* awa/tests/queue_storage_runtime_test.rs:574. In spec terms the
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

\* Fire the base spec action matching trace[n]. Events that take a
\* (worker, job) pair read both ev.worker and ev.job. Events that take
\* only a job read ev.job.
TraceStep(trace, n) ==
    LET ev == trace[n] IN
    /\ traceIdx' = n
    /\ \/ (ev.action = "EnqueueReady" /\ EnqueueReady(ev.job))
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
       \/ (ev.action = "ResumeWaitingToReady" /\ ResumeWaitingToReady(ev.job))
       \/ (ev.action = "TimeoutWaitingToReady" /\ TimeoutWaitingToReady(ev.job))
       \/ (ev.action = "TimeoutWaitingToDlq" /\ TimeoutWaitingToDlq(ev.job))
       \/ (ev.action = "FailToDlq" /\ FailToDlq(ev.worker, ev.job))
       \/ (ev.action = "RescueToReady" /\ RescueToReady(ev.job))
       \/ (ev.action = "RescueStaleReceipt" /\ RescueStaleReceipt(ev.job))
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
\* `SPECIFICATION SpecSnooze` or `SPECIFICATION SpecBroken`.
SpecSnooze == TraceInit /\ [][TraceNextFor(SnoozeTrace)]_<<vars, traceIdx>>
SpecBroken == TraceInit /\ [][TraceNextFor(BrokenTrace)]_<<vars, traceIdx>>

\* Positive witness invariants: negated so "violation" means acceptance.
\* If the trace is fully consumed, traceIdx reaches Len(trace), making
\* the negated predicate false and tripping a TLC-reported violation.
\* That "violation" is the success signal. For the broken trace, TLC
\* reports actual deadlock (no enabled disjunct) BEFORE reaching the
\* end — the deadlock at traceIdx < Len proves the rejection.
SnoozeTraceIncomplete == traceIdx < Len(SnoozeTrace)
BrokenTraceIncomplete == traceIdx < Len(BrokenTrace)

\* All other safety invariants (TypeOK, DlqHasNoLiveRuntime, etc.) are
\* inherited from AwaSegmentedStorage and must hold throughout any
\* trace the replay accepts.

=============================================================================

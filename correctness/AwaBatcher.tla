---- MODULE AwaBatcher ----
EXTENDS FiniteSets, Naturals
(*
    Model of the Awa completion batcher.

    Verifies that the batched completion path preserves the core invariants
    from AwaCore — lease-guarded finalization, no double-completion, no lost
    completions on shutdown — while adding the batcher as an intermediate
    actor between handler return and DB update.

    What this model covers:
      - Handler completes → result queued in batcher → flush to DB
      - Batcher flush failure → direct fallback completion
      - Concurrent rescue while completion is pending in batcher
      - Shutdown drain: batcher flushes all pending before exit
      - run_lease guard prevents stale completion from either path

    Deliberately simplified:
      - Single shard (real system has 8 independent shards)
      - No channel capacity limits (batcher channel is 4096 in reality)
      - Abstract time: flush triggers nondeterministically
*)

Jobs == {"j1", "j2"}
Workers == {"w1"}
MaxLease == 3

\* A completion request sitting in the batcher buffer
NoOwner == "no_owner"
NoPending == [job_id |-> "none", run_lease |-> 0]

\* Job DB states
JobStates == {"available", "running", "retryable", "completed", "failed"}

\* Handler-side completion state for each (worker, job)
\*   "idle"    — not executing
\*   "pending" — handler returned, completion queued in batcher
\*   "fallback"— batcher flush failed, about to try direct completion
\*   "done"    — completion acknowledged (success or stale)
HandlerPhases == {"idle", "pending", "fallback", "done"}

VARIABLES
    \* DB state
    jobState,       \* [Jobs -> JobStates]
    owner,          \* [Jobs -> Workers ∪ {NoOwner}]
    lease,          \* [Jobs -> 0..MaxLease]

    \* Per-worker per-job execution state
    taskLease,      \* [Workers -> [Jobs -> 0..MaxLease]]
    handlerPhase,   \* [Workers -> [Jobs -> HandlerPhases]]

    \* Batcher state (abstract: set of pending requests)
    batcherPending, \* SUBSET [job_id: Jobs, run_lease: 0..MaxLease]

    \* Lifecycle
    shutdownPhase,  \* {"running", "draining", "batcher_drain", "stopped"}

    \* Counters for safety checking
    dbCompletions   \* [Jobs -> Nat] — how many times each job was DB-completed

vars == <<jobState, owner, lease, taskLease, handlerPhase,
          batcherPending, shutdownPhase, dbCompletions>>

Init ==
    /\ jobState = [j \in Jobs |-> "available"]
    /\ owner = [j \in Jobs |-> NoOwner]
    /\ lease = [j \in Jobs |-> 0]
    /\ taskLease = [w \in Workers |-> [j \in Jobs |-> 0]]
    /\ handlerPhase = [w \in Workers |-> [j \in Jobs |-> "idle"]]
    /\ batcherPending = {}
    /\ shutdownPhase = "running"
    /\ dbCompletions = [j \in Jobs |-> 0]

(* ────────────────────────────────────────────────────────────────── *)
(* Dispatcher: claim a job                                           *)
(* ────────────────────────────────────────────────────────────────── *)
Claim(w, j) ==
    /\ shutdownPhase = "running"
    /\ jobState[j] = "available"
    /\ lease[j] < MaxLease
    /\ handlerPhase[w][j] = "idle"
    /\ jobState' = [jobState EXCEPT ![j] = "running"]
    /\ owner' = [owner EXCEPT ![j] = w]
    /\ lease' = [lease EXCEPT ![j] = @ + 1]
    /\ taskLease' = [taskLease EXCEPT ![w][j] = lease[j] + 1]
    /\ UNCHANGED <<handlerPhase, batcherPending, shutdownPhase, dbCompletions>>

(* ────────────────────────────────────────────────────────────────── *)
(* Handler completes: enqueue in batcher                             *)
(* ────────────────────────────────────────────────────────────────── *)
HandlerComplete(w, j) ==
    /\ jobState[j] = "running"
    /\ owner[j] = w
    /\ taskLease[w][j] > 0
    /\ handlerPhase[w][j] = "idle"
    /\ handlerPhase' = [handlerPhase EXCEPT ![w][j] = "pending"]
    /\ batcherPending' = batcherPending \cup
        {[job_id |-> j, run_lease |-> taskLease[w][j]]}
    /\ UNCHANGED <<jobState, owner, lease, taskLease, shutdownPhase, dbCompletions>>

(* ────────────────────────────────────────────────────────────────── *)
(* Batcher flush: apply a pending completion to the DB               *)
(*                                                                    *)
(* Models one request at a time for simplicity. Real system flushes   *)
(* batches of up to 512, but atomicity is per-SQL-transaction so the *)
(* per-request model captures the same safety properties.            *)
(* ────────────────────────────────────────────────────────────────── *)
BatcherFlushSuccess(req) ==
    /\ req \in batcherPending
    \* DB guard: state=running AND run_lease matches
    /\ jobState[req.job_id] = "running"
    /\ lease[req.job_id] = req.run_lease
    \* Apply: job completes
    /\ jobState' = [jobState EXCEPT ![req.job_id] = "completed"]
    /\ owner' = [owner EXCEPT ![req.job_id] = NoOwner]
    /\ dbCompletions' = [dbCompletions EXCEPT ![req.job_id] = @ + 1]
    /\ batcherPending' = batcherPending \ {req}
    \* Notify handler
    /\ \E w \in Workers :
        /\ handlerPhase[w][req.job_id] = "pending"
        /\ taskLease[w][req.job_id] = req.run_lease
        /\ handlerPhase' = [handlerPhase EXCEPT ![w][req.job_id] = "done"]
        /\ taskLease' = [taskLease EXCEPT ![w][req.job_id] = 0]
    /\ UNCHANGED <<lease, shutdownPhase>>

(* Batcher flush where the DB guard rejects (stale lease or wrong state) *)
BatcherFlushStale(req) ==
    /\ req \in batcherPending
    /\ (jobState[req.job_id] # "running" \/ lease[req.job_id] # req.run_lease)
    /\ batcherPending' = batcherPending \ {req}
    \* Notify handler: done (stale, no DB change)
    /\ \E w \in Workers :
        /\ handlerPhase[w][req.job_id] = "pending"
        /\ taskLease[w][req.job_id] = req.run_lease
        /\ handlerPhase' = [handlerPhase EXCEPT ![w][req.job_id] = "done"]
        /\ taskLease' = [taskLease EXCEPT ![w][req.job_id] = 0]
    /\ UNCHANGED <<jobState, owner, lease, shutdownPhase, dbCompletions>>

(* Batcher flush fails (e.g., DB connection error) — handler falls back *)
BatcherFlushFail(req) ==
    /\ req \in batcherPending
    /\ batcherPending' = batcherPending \ {req}
    \* Handler moves to fallback phase
    /\ \E w \in Workers :
        /\ handlerPhase[w][req.job_id] = "pending"
        /\ taskLease[w][req.job_id] = req.run_lease
        /\ handlerPhase' = [handlerPhase EXCEPT ![w][req.job_id] = "fallback"]
    /\ UNCHANGED <<jobState, owner, lease, taskLease, shutdownPhase, dbCompletions>>

(* ────────────────────────────────────────────────────────────────── *)
(* Direct completion fallback (after batcher failure)                *)
(* ────────────────────────────────────────────────────────────────── *)
DirectCompleteSuccess(w, j) ==
    /\ handlerPhase[w][j] = "fallback"
    /\ jobState[j] = "running"
    /\ lease[j] = taskLease[w][j]
    /\ jobState' = [jobState EXCEPT ![j] = "completed"]
    /\ owner' = [owner EXCEPT ![j] = NoOwner]
    /\ dbCompletions' = [dbCompletions EXCEPT ![j] = @ + 1]
    /\ handlerPhase' = [handlerPhase EXCEPT ![w][j] = "done"]
    /\ taskLease' = [taskLease EXCEPT ![w][j] = 0]
    /\ UNCHANGED <<lease, batcherPending, shutdownPhase>>

DirectCompleteStale(w, j) ==
    /\ handlerPhase[w][j] = "fallback"
    /\ (jobState[j] # "running" \/ lease[j] # taskLease[w][j])
    /\ handlerPhase' = [handlerPhase EXCEPT ![w][j] = "done"]
    /\ taskLease' = [taskLease EXCEPT ![w][j] = 0]
    /\ UNCHANGED <<jobState, owner, lease, batcherPending, shutdownPhase, dbCompletions>>

(* ────────────────────────────────────────────────────────────────── *)
(* Handler error: job fails terminally (no batcher involved)         *)
(* ────────────────────────────────────────────────────────────────── *)
HandlerFail(w, j) ==
    /\ jobState[j] = "running"
    /\ owner[j] = w
    /\ taskLease[w][j] > 0
    /\ handlerPhase[w][j] = "idle"
    /\ lease[j] = taskLease[w][j]
    /\ jobState' = [jobState EXCEPT ![j] = "failed"]
    /\ owner' = [owner EXCEPT ![j] = NoOwner]
    /\ taskLease' = [taskLease EXCEPT ![w][j] = 0]
    /\ UNCHANGED <<lease, handlerPhase, batcherPending, shutdownPhase, dbCompletions>>

(* ────────────────────────────────────────────────────────────────── *)
(* Maintenance: rescue + promote                                     *)
(* ────────────────────────────────────────────────────────────────── *)
Rescue(j) ==
    /\ jobState[j] = "running"
    /\ owner[j] \in Workers
    /\ jobState' = [jobState EXCEPT ![j] = "retryable"]
    /\ owner' = [owner EXCEPT ![j] = NoOwner]
    /\ UNCHANGED <<lease, taskLease, handlerPhase, batcherPending,
                   shutdownPhase, dbCompletions>>

Promote(j) ==
    /\ jobState[j] = "retryable"
    /\ jobState' = [jobState EXCEPT ![j] = "available"]
    /\ UNCHANGED <<owner, lease, taskLease, handlerPhase, batcherPending,
                   shutdownPhase, dbCompletions>>

(* ────────────────────────────────────────────────────────────────── *)
(* Cleanup: handler acknowledges stale task (after rescue)           *)
(* ────────────────────────────────────────────────────────────────── *)
HandlerCleanup(w, j) ==
    /\ handlerPhase[w][j] = "idle"
    /\ taskLease[w][j] > 0
    /\ (jobState[j] # "running" \/ owner[j] # w \/ lease[j] # taskLease[w][j])
    /\ taskLease' = [taskLease EXCEPT ![w][j] = 0]
    /\ UNCHANGED <<jobState, owner, lease, handlerPhase, batcherPending,
                   shutdownPhase, dbCompletions>>

(* ────────────────────────────────────────────────────────────────── *)
(* Shutdown sequencing                                                *)
(* ────────────────────────────────────────────────────────────────── *)
ShutdownBegin ==
    /\ shutdownPhase = "running"
    /\ shutdownPhase' = "draining"
    /\ UNCHANGED <<jobState, owner, lease, taskLease, handlerPhase,
                   batcherPending, dbCompletions>>

\* Batcher drain: all in-flight tasks have finished (no active taskLease),
\* and no handlers are mid-completion. Only then start draining the batcher.
BatcherDrainStart ==
    /\ shutdownPhase = "draining"
    /\ \A w \in Workers, j \in Jobs :
        /\ taskLease[w][j] = 0
        /\ handlerPhase[w][j] \in {"idle", "done"}
    /\ shutdownPhase' = "batcher_drain"
    /\ UNCHANGED <<jobState, owner, lease, taskLease, handlerPhase,
                   batcherPending, dbCompletions>>

FinishShutdown ==
    /\ shutdownPhase = "batcher_drain"
    /\ batcherPending = {}
    /\ shutdownPhase' = "stopped"
    /\ UNCHANGED <<jobState, owner, lease, taskLease, handlerPhase,
                   batcherPending, dbCompletions>>

(* ────────────────────────────────────────────────────────────────── *)
(* Spec                                                               *)
(* ────────────────────────────────────────────────────────────────── *)
Stutter == UNCHANGED vars

Next ==
    \/ \E w \in Workers, j \in Jobs : Claim(w, j)
    \/ \E w \in Workers, j \in Jobs : HandlerComplete(w, j)
    \/ \E req \in batcherPending : BatcherFlushSuccess(req)
    \/ \E req \in batcherPending : BatcherFlushStale(req)
    \/ \E req \in batcherPending : BatcherFlushFail(req)
    \/ \E w \in Workers, j \in Jobs : DirectCompleteSuccess(w, j)
    \/ \E w \in Workers, j \in Jobs : DirectCompleteStale(w, j)
    \/ \E w \in Workers, j \in Jobs : HandlerFail(w, j)
    \/ \E j \in Jobs : Rescue(j)
    \/ \E j \in Jobs : Promote(j)
    \/ \E w \in Workers, j \in Jobs : HandlerCleanup(w, j)
    \/ ShutdownBegin
    \/ BatcherDrainStart
    \/ FinishShutdown
    \/ Stutter

Spec == Init /\ [][Next]_vars

FairSpec == Spec
    /\ WF_vars(\E req \in batcherPending : BatcherFlushSuccess(req))
    /\ WF_vars(\E req \in batcherPending : BatcherFlushStale(req))
    /\ WF_vars(\E w \in Workers, j \in Jobs : DirectCompleteSuccess(w, j))
    /\ WF_vars(\E w \in Workers, j \in Jobs : DirectCompleteStale(w, j))
    /\ WF_vars(\E j \in Jobs : Promote(j))
    /\ WF_vars(BatcherDrainStart)
    /\ WF_vars(FinishShutdown)

(* ────────────────────────────────────────────────────────────────── *)
(* Safety invariants                                                  *)
(* ────────────────────────────────────────────────────────────────── *)

TypeOK ==
    /\ jobState \in [Jobs -> JobStates]
    /\ owner \in [Jobs -> Workers \cup {NoOwner}]
    /\ lease \in [Jobs -> 0..MaxLease]
    /\ taskLease \in [Workers -> [Jobs -> 0..MaxLease]]
    /\ handlerPhase \in [Workers -> [Jobs -> HandlerPhases]]
    /\ batcherPending \subseteq [job_id : Jobs, run_lease : 1..MaxLease]
    /\ shutdownPhase \in {"running", "draining", "batcher_drain", "stopped"}
    /\ dbCompletions \in [Jobs -> 0..MaxLease]

\* A job is DB-completed at most once per lifecycle
AtMostOneCompletion ==
    \A j \in Jobs : dbCompletions[j] <= 1

\* Running jobs have an owner
RunningOwned ==
    \A j \in Jobs : jobState[j] = "running" => owner[j] \in Workers

\* Non-running jobs have no owner
NonRunningUnowned ==
    \A j \in Jobs : jobState[j] # "running" => owner[j] = NoOwner

\* A pending batcher request implies the job was running at enqueue time
\* (it may have since been rescued, but the request references a valid lease)
PendingRequestHasValidLease ==
    \A req \in batcherPending : req.run_lease > 0

\* After shutdown completes, no pending completions remain
ShutdownDrainedBatcher ==
    shutdownPhase = "stopped" => batcherPending = {}

\* No handler is stuck in "pending" or "fallback" after shutdown
ShutdownHandlersDone ==
    shutdownPhase = "stopped" =>
        \A w \in Workers, j \in Jobs :
            handlerPhase[w][j] \in {"idle", "done"}

(* ────────────────────────────────────────────────────────────────── *)
(* Liveness: under fairness, pending completions eventually resolve  *)
(* ────────────────────────────────────────────────────────────────── *)
PendingEventuallyResolved ==
    \A w \in Workers, j \in Jobs :
        handlerPhase[w][j] = "pending" ~> handlerPhase[w][j] \in {"done", "idle"}

====

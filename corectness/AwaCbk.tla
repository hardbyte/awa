---- MODULE AwaCbk ----
EXTENDS FiniteSets, Naturals

(*
  External callback resolution model.

  Verifies the three-way race between:
    1. External system calling complete_external / fail_external
    2. Maintenance leader running rescue_expired_callbacks (timeout)
    3. Heartbeat rescue clearing stale callback after worker crash

  Maps to code:
    RegisterCallback  -> admin.rs: register_callback (UPDATE WHERE state='running' AND run_lease=$4)
    EnterWaiting      -> executor.rs: WaitForCallback path (UPDATE WHERE state='running' AND callback_id IS NOT NULL)
    ExternalComplete  -> admin.rs: complete_external (UPDATE WHERE callback_id=$1 AND state IN ('waiting_external','running'))
    ExternalFail      -> admin.rs: fail_external (same WHERE clause as complete)
    TimeoutRescue     -> maintenance.rs: rescue_expired_callbacks (UPDATE WHERE state='waiting_external' AND timeout < now())
    HeartbeatRescue   -> maintenance.rs: rescue_stale_heartbeats (UPDATE WHERE state='running' AND heartbeat stale, clears callback_id)

  Safety: at-most-once resolution — a callback never produces >1 state transition.
  Liveness: a timed-out waiting job is eventually rescued (under availability).
*)

Instances == {"i1", "i2"}
NoInstance == "none"
NoCb == "none"
CbId == "cb1"
MaxLease == 3

JobStates == {"available", "running", "waiting_external", "completed", "retryable", "failed"}
TerminalStates == {"completed", "retryable", "failed"}

VARIABLES
    jobState,          \* Current job state
    callbackId,        \* DB callback_id column: NoCb or CbId
    callbackTimedOut,  \* Has the callback timeout deadline passed?
    heartbeatFresh,    \* Is the running job's heartbeat current?
    owner,             \* Instance that claimed the job (or NoInstance)
    lease,             \* DB run_lease counter (incremented on each claim)
    taskLease,         \* Per-instance: the lease value when they started the task
    leader,            \* Maintenance leader (or NoInstance)
    resolved           \* Count of successful callback resolutions (safety target)

vars == <<jobState, callbackId, callbackTimedOut, heartbeatFresh,
          owner, lease, taskLease, leader, resolved>>

\* ─── Initial state ────────────────────────────────────────

Init ==
    /\ jobState = "available"
    /\ callbackId = NoCb
    /\ callbackTimedOut = FALSE
    /\ heartbeatFresh = FALSE
    /\ owner = NoInstance
    /\ lease = 0
    /\ taskLease = [i \in Instances |-> 0]
    /\ leader = NoInstance
    /\ resolved = 0

\* ─── Actions ──────────────────────────────────────────────

\* Dispatcher claims the job (available → running).
\* Models: claim query with FOR UPDATE SKIP LOCKED + state transition.
Claim(i) ==
    /\ jobState = "available"
    /\ lease < MaxLease   \* bounds state space (models max_attempts)
    /\ jobState' = "running"
    /\ owner' = i
    /\ lease' = lease + 1
    /\ heartbeatFresh' = TRUE
    /\ taskLease' = [taskLease EXCEPT ![i] = lease + 1]
    /\ UNCHANGED <<callbackId, callbackTimedOut, leader, resolved>>

\* Handler calls ctx.register_callback() — writes callback_id to DB.
\* Guard: state='running' AND run_lease matches (from admin.rs:303).
RegisterCallback(i) ==
    /\ jobState = "running"
    /\ owner = i
    /\ taskLease[i] = lease
    /\ callbackId = NoCb
    /\ callbackId' = CbId
    /\ callbackTimedOut' = FALSE
    /\ resolved' = 0   \* reset: new callback lifecycle
    /\ UNCHANGED <<jobState, heartbeatFresh, owner, lease, taskLease, leader>>

\* Handler returns WaitForCallback — transitions to waiting_external.
\* Guard: state='running' AND callback_id IS NOT NULL AND run_lease matches.
\* If a racing callback already completed the job, this is a no-op (modeled
\* by the precondition checking state='running').
EnterWaiting(i) ==
    /\ jobState = "running"
    /\ owner = i
    /\ taskLease[i] = lease
    /\ callbackId = CbId
    /\ jobState' = "waiting_external"
    /\ heartbeatFresh' = FALSE  \* heartbeat_at set to NULL
    /\ UNCHANGED <<callbackId, callbackTimedOut, owner, lease, taskLease, leader, resolved>>

\* External system calls complete_external(callback_id).
\* Guard: callback_id=$1 AND state IN ('waiting_external','running').
\* First-writer-wins: atomically sets callback_id=NULL.
ExternalComplete ==
    /\ callbackId = CbId
    /\ jobState \in {"waiting_external", "running"}
    /\ jobState' = "completed"
    /\ callbackId' = NoCb
    /\ callbackTimedOut' = FALSE
    /\ heartbeatFresh' = FALSE
    /\ owner' = NoInstance
    /\ resolved' = resolved + 1
    /\ UNCHANGED <<lease, taskLease, leader>>

\* External system calls fail_external(callback_id).
\* Same guard as complete_external.
ExternalFail ==
    /\ callbackId = CbId
    /\ jobState \in {"waiting_external", "running"}
    /\ jobState' = "failed"
    /\ callbackId' = NoCb
    /\ callbackTimedOut' = FALSE
    /\ heartbeatFresh' = FALSE
    /\ owner' = NoInstance
    /\ resolved' = resolved + 1
    /\ UNCHANGED <<lease, taskLease, leader>>

\* Callback timeout expires (clock advances past deadline).
TimeoutExpires ==
    /\ jobState = "waiting_external"
    /\ callbackId = CbId
    /\ ~callbackTimedOut
    /\ callbackTimedOut' = TRUE
    /\ UNCHANGED <<jobState, callbackId, heartbeatFresh, owner, lease, taskLease, leader, resolved>>

\* Maintenance rescues a timed-out callback.
\* Guard: state='waiting_external' AND callback_timeout_at < now().
\* Atomically clears callback_id (first-writer-wins vs external complete).
TimeoutRescue(i) ==
    /\ leader = i
    /\ jobState = "waiting_external"
    /\ callbackId = CbId
    /\ callbackTimedOut
    /\ jobState' = "retryable"    \* simplified; real code checks attempt >= max_attempts
    /\ callbackId' = NoCb
    /\ callbackTimedOut' = FALSE
    /\ heartbeatFresh' = FALSE
    /\ owner' = NoInstance
    /\ resolved' = resolved + 1
    /\ UNCHANGED <<lease, taskLease, leader>>

\* Heartbeat becomes stale (models worker crash or network partition).
HeartbeatStale ==
    /\ jobState = "running"
    /\ heartbeatFresh
    /\ heartbeatFresh' = FALSE
    /\ UNCHANGED <<jobState, callbackId, callbackTimedOut, owner, lease, taskLease, leader, resolved>>

\* Maintenance rescues a job with stale heartbeat (crash detection).
\* Guard: state='running' AND heartbeat stale.
\* Clears callback_id and all callback fields — prevents future external
\* resolution of a callback registered by the crashed worker.
HeartbeatRescue(i) ==
    /\ leader = i
    /\ jobState = "running"
    /\ owner \in Instances
    /\ ~heartbeatFresh
    /\ jobState' = "retryable"
    /\ callbackId' = NoCb           \* key: clears callback before timeout can fire
    /\ callbackTimedOut' = FALSE
    /\ heartbeatFresh' = FALSE
    /\ owner' = NoInstance
    /\ UNCHANGED <<lease, taskLease, leader, resolved>>

\* Promote retryable → available (maintenance leader promotes after backoff).
PromoteRetryable ==
    /\ jobState = "retryable"
    /\ jobState' = "available"
    /\ UNCHANGED <<callbackId, callbackTimedOut, heartbeatFresh, owner, lease, taskLease, leader, resolved>>

\* Leader election.
AcquireLeader(i) ==
    /\ leader = NoInstance
    /\ leader' = i
    /\ UNCHANGED <<jobState, callbackId, callbackTimedOut, heartbeatFresh, owner, lease, taskLease, resolved>>

LoseLeader(i) ==
    /\ leader = i
    /\ leader' = NoInstance
    /\ UNCHANGED <<jobState, callbackId, callbackTimedOut, heartbeatFresh, owner, lease, taskLease, resolved>>

\* ─── Specification ────────────────────────────────────────

Next ==
    \/ \E i \in Instances : Claim(i)
    \/ \E i \in Instances : RegisterCallback(i)
    \/ \E i \in Instances : EnterWaiting(i)
    \/ ExternalComplete
    \/ ExternalFail
    \/ TimeoutExpires
    \/ \E i \in Instances : TimeoutRescue(i)
    \/ HeartbeatStale
    \/ \E i \in Instances : HeartbeatRescue(i)
    \/ PromoteRetryable
    \/ \E i \in Instances : AcquireLeader(i)
    \/ \E i \in Instances : LoseLeader(i)

Spec == Init /\ [][Next]_vars

\* ─── Safety invariants ────────────────────────────────────

TypeOK ==
    /\ jobState \in JobStates
    /\ callbackId \in {NoCb, CbId}
    /\ callbackTimedOut \in BOOLEAN
    /\ heartbeatFresh \in BOOLEAN
    /\ owner \in Instances \cup {NoInstance}
    /\ lease \in 0..MaxLease
    /\ taskLease \in [Instances -> 0..MaxLease]
    /\ leader \in Instances \cup {NoInstance}
    /\ resolved \in 0..1

\* CRITICAL SAFETY: at most one resolution per callback lifecycle.
\* A "resolution" is any transition out of {running, waiting_external}
\* caused by a callback-related action (external complete/fail or timeout rescue).
\* Heartbeat rescue is NOT counted because it doesn't resolve the callback —
\* it clears it as part of crash recovery.
AtMostOnceResolution ==
    resolved <= 1

\* Terminal states have no callback.
TerminalClearsCallback ==
    jobState \in TerminalStates => callbackId = NoCb

\* waiting_external always has a callback.
WaitingHasCallback ==
    jobState = "waiting_external" => callbackId # NoCb

\* Non-running, non-waiting states have no owner.
IdleHasNoOwner ==
    jobState \in {"available", "retryable", "completed", "failed"} => owner = NoInstance

\* Running/waiting jobs have an owner.
ActiveHasOwner ==
    jobState \in {"running", "waiting_external"} => owner \in Instances

\* Callback timeout only possible in waiting_external.
TimeoutOnlyWhenWaiting ==
    callbackTimedOut => jobState = "waiting_external"

\* ─── Liveness (under availability) ────────────────────────

StableNext ==
    \/ \E i \in Instances : Claim(i)
    \/ \E i \in Instances : RegisterCallback(i)
    \/ \E i \in Instances : EnterWaiting(i)
    \/ ExternalComplete
    \/ ExternalFail
    \/ TimeoutExpires
    \/ \E i \in Instances : TimeoutRescue(i)
    \/ HeartbeatStale
    \/ \E i \in Instances : HeartbeatRescue(i)
    \/ PromoteRetryable
    \/ \E i \in Instances : AcquireLeader(i)
    \/ \E i \in Instances : LoseLeader(i)

StableSpec == Init /\ [][StableNext]_vars

FairSpec ==
    StableSpec
    /\ WF_vars(\E i \in Instances : AcquireLeader(i))
    /\ SF_vars(\E i \in Instances : TimeoutRescue(i))
    /\ WF_vars(TimeoutExpires)
    /\ WF_vars(PromoteRetryable)

\* A timed-out waiting job is eventually rescued or resolved.
TimedOutEventuallyLeaves ==
    (jobState = "waiting_external" /\ callbackTimedOut)
        ~> (jobState # "waiting_external")

====

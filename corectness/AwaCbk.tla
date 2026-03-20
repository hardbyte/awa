---- MODULE AwaCbk ----
EXTENDS FiniteSets, Naturals

(*
  External callback resolution model — with Postgres row-lock semantics.

  Verifies the three-way race between:
    1. External system calling complete_external / fail_external
    2. Maintenance leader running rescue_expired_callbacks (timeout)
    3. Heartbeat rescue clearing stale callback after worker crash

  Unlike a naive atomic-action model, this version models Postgres row-level
  locking to verify that the concurrency control actually prevents double
  resolution:

  - complete_external is a plain UPDATE: if the row is locked by another
    transaction, it BLOCKS until the lock is released, then re-evaluates
    the WHERE clause against the new row state.
  - rescue_expired_callbacks uses FOR UPDATE SKIP LOCKED: if the row is
    locked, it skips and returns 0 rows.
  - heartbeat rescue also uses FOR UPDATE SKIP LOCKED.

  The model splits each resolution path into lock-acquire + execute phases
  to expose the interleaving window that atomic-action models hide.
*)

Instances == {"i1", "i2"}
NoInstance == "none"
NoCb == "none"
CbId == "cb1"
MaxLease == 3

JobStates == {"available", "running", "waiting_external", "completed", "retryable", "failed"}
TerminalStates == {"completed", "retryable", "failed"}

\* Row lock states. NoLock means the row is unlocked.
\* Other values identify which operation holds the lock.
NoLock == "unlocked"
LockKinds == {"complete", "timeout_rescue", "heartbeat_rescue"}

VARIABLES
    jobState,          \* Current job state
    callbackId,        \* DB callback_id column: NoCb or CbId
    callbackTimedOut,  \* Has the callback timeout deadline passed?
    heartbeatFresh,    \* Is the running job's heartbeat current?
    owner,             \* Instance that claimed the job (or NoInstance)
    lease,             \* DB run_lease counter (incremented on each claim)
    taskLease,         \* Per-instance: the lease value when they started the task
    leader,            \* Maintenance leader (or NoInstance)
    resolved,          \* Count of successful callback resolutions (safety target)
    rowLock,           \* Who holds the Postgres row lock: NoLock or LockKinds
    completeBlocked    \* TRUE if complete_external is blocked waiting for a row lock

vars == <<jobState, callbackId, callbackTimedOut, heartbeatFresh,
          owner, lease, taskLease, leader, resolved,
          rowLock, completeBlocked>>

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
    /\ rowLock = NoLock
    /\ completeBlocked = FALSE

\* ─── Non-locking actions (unchanged from original) ────────

\* Dispatcher claims the job.
\* This is a CTE with FOR UPDATE SKIP LOCKED — requires row unlocked.
Claim(i) ==
    /\ jobState = "available"
    /\ lease < MaxLease
    /\ rowLock = NoLock
    /\ jobState' = "running"
    /\ owner' = i
    /\ lease' = lease + 1
    /\ heartbeatFresh' = TRUE
    /\ taskLease' = [taskLease EXCEPT ![i] = lease + 1]
    /\ UNCHANGED <<callbackId, callbackTimedOut, leader, resolved, rowLock, completeBlocked>>

\* Handler registers callback (UPDATE WHERE state='running' AND run_lease=$4).
\* Plain UPDATE — blocks on row lock, but if lock is held by rescue,
\* state will be 'retryable' after release and WHERE fails. Modeled as
\* requiring NoLock (the blocking + re-evaluation always fails).
RegisterCallback(i) ==
    /\ jobState = "running"
    /\ owner = i
    /\ taskLease[i] = lease
    /\ callbackId = NoCb
    /\ rowLock = NoLock
    /\ callbackId' = CbId
    /\ callbackTimedOut' = FALSE
    /\ resolved' = 0
    /\ UNCHANGED <<jobState, heartbeatFresh, owner, lease, taskLease, leader, rowLock, completeBlocked>>

\* Handler returns WaitForCallback (UPDATE WHERE state='running' AND callback_id IS NOT NULL).
\* Same blocking semantics as RegisterCallback.
EnterWaiting(i) ==
    /\ jobState = "running"
    /\ owner = i
    /\ taskLease[i] = lease
    /\ callbackId = CbId
    /\ rowLock = NoLock
    /\ jobState' = "waiting_external"
    /\ heartbeatFresh' = FALSE
    /\ UNCHANGED <<callbackId, callbackTimedOut, owner, lease, taskLease, leader, resolved, rowLock, completeBlocked>>

\* Timeout deadline passes.
TimeoutExpires ==
    /\ jobState = "waiting_external"
    /\ callbackId = CbId
    /\ ~callbackTimedOut
    /\ callbackTimedOut' = TRUE
    /\ UNCHANGED <<jobState, callbackId, heartbeatFresh, owner, lease, taskLease, leader, resolved, rowLock, completeBlocked>>

\* Heartbeat becomes stale.
HeartbeatStale ==
    /\ jobState = "running"
    /\ heartbeatFresh
    /\ heartbeatFresh' = FALSE
    /\ UNCHANGED <<jobState, callbackId, callbackTimedOut, owner, lease, taskLease, leader, resolved, rowLock, completeBlocked>>

\* Promote retryable → available.
PromoteRetryable ==
    /\ jobState = "retryable"
    /\ jobState' = "available"
    /\ UNCHANGED <<callbackId, callbackTimedOut, heartbeatFresh, owner, lease, taskLease, leader, resolved, rowLock, completeBlocked>>

\* Leader election.
AcquireLeader(i) ==
    /\ leader = NoInstance
    /\ leader' = i
    /\ UNCHANGED <<jobState, callbackId, callbackTimedOut, heartbeatFresh, owner, lease, taskLease, resolved, rowLock, completeBlocked>>

LoseLeader(i) ==
    /\ leader = i
    /\ leader' = NoInstance
    /\ UNCHANGED <<jobState, callbackId, callbackTimedOut, heartbeatFresh, owner, lease, taskLease, resolved, rowLock, completeBlocked>>

\* ─── complete_external: blocking UPDATE ───────────────────
\*
\* Plain UPDATE — no SKIP LOCKED. Postgres blocks if row is locked,
\* then re-evaluates WHERE after the lock is released.

\* Phase 1a: Try to lock. Row is free → acquire lock.
CompleteTryLock ==
    /\ callbackId = CbId
    /\ jobState \in {"waiting_external", "running"}
    /\ rowLock = NoLock
    /\ ~completeBlocked
    /\ rowLock' = "complete"
    /\ UNCHANGED <<jobState, callbackId, callbackTimedOut, heartbeatFresh, owner, lease, taskLease, leader, resolved, completeBlocked>>

\* Phase 1b: Row is locked by someone else → block (wait for lock).
CompleteBlock ==
    /\ callbackId = CbId
    /\ jobState \in {"waiting_external", "running"}
    /\ rowLock \in LockKinds
    /\ rowLock # "complete"
    /\ ~completeBlocked
    /\ completeBlocked' = TRUE
    /\ UNCHANGED <<jobState, callbackId, callbackTimedOut, heartbeatFresh, owner, lease, taskLease, leader, resolved, rowLock>>

\* Phase 1c: Lock released while complete is blocked → re-evaluate WHERE.
\* If preconditions still hold, acquire lock. (Postgres re-checks the row.)
CompleteReEvaluate ==
    /\ completeBlocked
    /\ rowLock = NoLock
    /\ IF callbackId = CbId /\ jobState \in {"waiting_external", "running"}
       THEN /\ rowLock' = "complete"        \* re-eval succeeded, acquire lock
            /\ completeBlocked' = FALSE
            /\ UNCHANGED <<jobState, callbackId, callbackTimedOut, heartbeatFresh, owner, lease, taskLease, leader, resolved>>
       ELSE /\ completeBlocked' = FALSE     \* re-eval failed (row changed), give up
            /\ UNCHANGED <<jobState, callbackId, callbackTimedOut, heartbeatFresh, owner, lease, taskLease, leader, resolved, rowLock>>

\* Phase 2: Execute the UPDATE (lock held).
CompleteExecute ==
    /\ rowLock = "complete"
    /\ jobState' = "completed"
    /\ callbackId' = NoCb
    /\ callbackTimedOut' = FALSE
    /\ heartbeatFresh' = FALSE
    /\ owner' = NoInstance
    /\ resolved' = resolved + 1
    /\ rowLock' = NoLock     \* release lock (commit)
    /\ UNCHANGED <<lease, taskLease, leader, completeBlocked>>

\* ─── rescue_expired_callbacks: SKIP LOCKED UPDATE ─────────
\*
\* Uses FOR UPDATE SKIP LOCKED in the inner SELECT.
\* If the row is locked, it returns 0 rows — the outer UPDATE does nothing.

\* Phase 1: Try to lock. Free → lock. Locked → skip (no-op).
TimeoutTryLock(i) ==
    /\ leader = i
    /\ jobState = "waiting_external"
    /\ callbackId = CbId
    /\ callbackTimedOut
    /\ rowLock = NoLock
    /\ rowLock' = "timeout_rescue"
    /\ UNCHANGED <<jobState, callbackId, callbackTimedOut, heartbeatFresh, owner, lease, taskLease, leader, resolved, completeBlocked>>

\* Phase 2: Execute (lock held).
TimeoutExecute ==
    /\ rowLock = "timeout_rescue"
    /\ jobState' = "retryable"
    /\ callbackId' = NoCb
    /\ callbackTimedOut' = FALSE
    /\ heartbeatFresh' = FALSE
    /\ owner' = NoInstance
    /\ resolved' = resolved + 1
    /\ rowLock' = NoLock     \* release lock (commit)
    /\ UNCHANGED <<lease, taskLease, leader, completeBlocked>>

\* ─── rescue_stale_heartbeats: SKIP LOCKED UPDATE ──────────

\* Phase 1: Try to lock. Free → lock. Locked → skip.
HeartbeatTryLock(i) ==
    /\ leader = i
    /\ jobState = "running"
    /\ owner \in Instances
    /\ ~heartbeatFresh
    /\ rowLock = NoLock
    /\ rowLock' = "heartbeat_rescue"
    /\ UNCHANGED <<jobState, callbackId, callbackTimedOut, heartbeatFresh, owner, lease, taskLease, leader, resolved, completeBlocked>>

\* Phase 2: Execute (lock held). Clears callback_id.
HeartbeatExecute ==
    /\ rowLock = "heartbeat_rescue"
    /\ jobState' = "retryable"
    /\ callbackId' = NoCb
    /\ callbackTimedOut' = FALSE
    /\ heartbeatFresh' = FALSE
    /\ owner' = NoInstance
    /\ rowLock' = NoLock     \* release lock (commit)
    /\ UNCHANGED <<lease, taskLease, leader, resolved, completeBlocked>>

\* ─── Specification ────────────────────────────────────────

Next ==
    \/ \E i \in Instances : Claim(i)
    \/ \E i \in Instances : RegisterCallback(i)
    \/ \E i \in Instances : EnterWaiting(i)
    \/ CompleteTryLock
    \/ CompleteBlock
    \/ CompleteReEvaluate
    \/ CompleteExecute
    \/ TimeoutExpires
    \/ \E i \in Instances : TimeoutTryLock(i)
    \/ TimeoutExecute
    \/ HeartbeatStale
    \/ \E i \in Instances : HeartbeatTryLock(i)
    \/ HeartbeatExecute
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
    /\ rowLock \in {NoLock} \cup LockKinds
    /\ completeBlocked \in BOOLEAN

\* CRITICAL SAFETY: at most one resolution per callback lifecycle.
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

\* Running/waiting jobs have an owner (unless mid-resolution with lock held).
ActiveHasOwner ==
    jobState \in {"running", "waiting_external"} /\ rowLock = NoLock => owner \in Instances

\* Lock holder consistency: if a lock is held, the row is in a state
\* that the lock holder can legitimately modify.
LockHolderConsistent ==
    /\ rowLock = "complete" =>
        (callbackId = CbId /\ jobState \in {"waiting_external", "running"})
    /\ rowLock = "timeout_rescue" =>
        (jobState = "waiting_external" /\ callbackId = CbId /\ callbackTimedOut)
    /\ rowLock = "heartbeat_rescue" =>
        (jobState = "running" /\ ~heartbeatFresh)

\* A blocked complete never co-exists with a complete lock.
\* (You can't be waiting for a lock you already hold.)
BlockedNotSelfLocked ==
    completeBlocked => rowLock # "complete"

\* ─── Liveness (under availability) ────────────────────────

FairSpec ==
    Spec
    /\ WF_vars(\E i \in Instances : AcquireLeader(i))
    /\ WF_vars(TimeoutExpires)
    /\ WF_vars(PromoteRetryable)
    /\ SF_vars(\E i \in Instances : TimeoutTryLock(i))
    /\ WF_vars(TimeoutExecute)
    /\ WF_vars(CompleteExecute)
    /\ WF_vars(HeartbeatExecute)
    /\ WF_vars(CompleteReEvaluate)

\* A timed-out waiting job is eventually rescued or resolved.
TimedOutEventuallyLeaves ==
    (jobState = "waiting_external" /\ callbackTimedOut)
        ~> (jobState # "waiting_external")

====

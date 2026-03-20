---- MODULE AwaCbk ----
EXTENDS FiniteSets, Naturals

(*
  External callback resolution model — with Postgres row-lock semantics.

  Verifies the three-way race between:
    1. External system calling complete_external / fail_external / resolve_callback
    2. Maintenance leader running rescue_expired_callbacks (timeout)
    3. Heartbeat rescue clearing stale callback after worker crash

  Unlike a naive atomic-action model, this version models Postgres row-level
  locking to verify that the concurrency control actually prevents double
  resolution:

  - complete_external and fail_external are plain UPDATEs: if the row is
    locked, they BLOCK until the lock is released, then re-evaluate the
    WHERE clause against the new row state.
  - resolve_callback does SELECT ... FOR UPDATE: if the row is locked, it
    BLOCKS until the lock is released, then re-checks callback_id + state
    before deciding Complete / Fail / Ignore.
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
BlockingOps == {"complete", "fail", "resolve"}

\* Row lock states. NoLock means the row is unlocked.
\* Other values identify which operation holds the lock.
NoLock == "unlocked"
LockKinds == BlockingOps \cup {"timeout_rescue", "heartbeat_rescue"}

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
    blockedOps         \* External operations currently blocked on the row lock

vars == <<jobState, callbackId, callbackTimedOut, heartbeatFresh,
          owner, lease, taskLease, leader, resolved, rowLock, blockedOps>>

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
    /\ blockedOps = {}

\* ─── Non-locking actions ──────────────────────────────────

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
    /\ UNCHANGED <<callbackId, callbackTimedOut, leader, resolved, rowLock, blockedOps>>

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
    /\ UNCHANGED <<jobState, heartbeatFresh, owner, lease, taskLease, leader, rowLock, blockedOps>>

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
    /\ UNCHANGED <<callbackId, callbackTimedOut, owner, lease, taskLease, leader, resolved, rowLock, blockedOps>>

\* Timeout deadline passes.
TimeoutExpires ==
    /\ jobState = "waiting_external"
    /\ callbackId = CbId
    /\ ~callbackTimedOut
    /\ callbackTimedOut' = TRUE
    /\ UNCHANGED <<jobState, callbackId, heartbeatFresh, owner, lease, taskLease, leader, resolved, rowLock, blockedOps>>

\* Heartbeat becomes stale.
HeartbeatStale ==
    /\ jobState = "running"
    /\ heartbeatFresh
    /\ heartbeatFresh' = FALSE
    /\ UNCHANGED <<jobState, callbackId, callbackTimedOut, owner, lease, taskLease, leader, resolved, rowLock, blockedOps>>

\* Promote retryable → available.
PromoteRetryable ==
    /\ jobState = "retryable"
    /\ jobState' = "available"
    /\ UNCHANGED <<callbackId, callbackTimedOut, heartbeatFresh, owner, lease, taskLease, leader, resolved, rowLock, blockedOps>>

\* Leader election.
AcquireLeader(i) ==
    /\ leader = NoInstance
    /\ leader' = i
    /\ UNCHANGED <<jobState, callbackId, callbackTimedOut, heartbeatFresh, owner, lease, taskLease, resolved, rowLock, blockedOps>>

LoseLeader(i) ==
    /\ leader = i
    /\ leader' = NoInstance
    /\ UNCHANGED <<jobState, callbackId, callbackTimedOut, heartbeatFresh, owner, lease, taskLease, resolved, rowLock, blockedOps>>

\* ─── External callback resolution: blocking UPDATE / FOR UPDATE ─────

BlockingPreconditions(op) ==
    /\ op \in BlockingOps
    /\ callbackId = CbId
    /\ IF op = "resolve"
          THEN jobState = "waiting_external"
          ELSE jobState \in {"waiting_external", "running"}

\* Phase 1a: Try to lock. Row is free -> acquire lock.
BlockingTryLock(op) ==
    /\ BlockingPreconditions(op)
    /\ rowLock = NoLock
    /\ op \notin blockedOps
    /\ rowLock' = op
    /\ UNCHANGED <<jobState, callbackId, callbackTimedOut, heartbeatFresh, owner, lease, taskLease, leader, resolved, blockedOps>>

\* Phase 1b: Row is locked by someone else -> block (wait for lock).
BlockingBlock(op) ==
    /\ BlockingPreconditions(op)
    /\ rowLock \in LockKinds
    /\ rowLock # op
    /\ op \notin blockedOps
    /\ blockedOps' = blockedOps \cup {op}
    /\ UNCHANGED <<jobState, callbackId, callbackTimedOut, heartbeatFresh, owner, lease, taskLease, leader, resolved, rowLock>>

\* Phase 1c: Lock released while an external op is blocked -> re-evaluate.
\* If preconditions still hold, acquire lock. Otherwise give up.
BlockingReEvaluate(op) ==
    /\ op \in blockedOps
    /\ rowLock = NoLock
    /\ IF BlockingPreconditions(op)
          THEN /\ rowLock' = op
               /\ blockedOps' = blockedOps \ {op}
               /\ UNCHANGED <<jobState, callbackId, callbackTimedOut, heartbeatFresh, owner, lease, taskLease, leader, resolved>>
          ELSE /\ blockedOps' = blockedOps \ {op}
               /\ UNCHANGED <<jobState, callbackId, callbackTimedOut, heartbeatFresh, owner, lease, taskLease, leader, resolved, rowLock>>

\* Phase 2: Execute UPDATE paths.
CompleteExecute ==
    /\ rowLock = "complete"
    /\ jobState' = "completed"
    /\ callbackId' = NoCb
    /\ callbackTimedOut' = FALSE
    /\ heartbeatFresh' = FALSE
    /\ owner' = NoInstance
    /\ resolved' = resolved + 1
    /\ rowLock' = NoLock
    /\ UNCHANGED <<lease, taskLease, leader, blockedOps>>

FailExecute ==
    /\ rowLock = "fail"
    /\ jobState' = "failed"
    /\ callbackId' = NoCb
    /\ callbackTimedOut' = FALSE
    /\ heartbeatFresh' = FALSE
    /\ owner' = NoInstance
    /\ resolved' = resolved + 1
    /\ rowLock' = NoLock
    /\ UNCHANGED <<lease, taskLease, leader, blockedOps>>

\* resolve_callback chooses between Complete / Fail / Ignore after acquiring
\* the FOR UPDATE lock. CEL evaluation is abstracted as nondeterministic choice.
ResolveCompleteExecute ==
    /\ rowLock = "resolve"
    /\ jobState = "waiting_external"
    /\ callbackId = CbId
    /\ jobState' = "completed"
    /\ callbackId' = NoCb
    /\ callbackTimedOut' = FALSE
    /\ heartbeatFresh' = FALSE
    /\ owner' = NoInstance
    /\ resolved' = resolved + 1
    /\ rowLock' = NoLock
    /\ UNCHANGED <<lease, taskLease, leader, blockedOps>>

ResolveFailExecute ==
    /\ rowLock = "resolve"
    /\ jobState = "waiting_external"
    /\ callbackId = CbId
    /\ jobState' = "failed"
    /\ callbackId' = NoCb
    /\ callbackTimedOut' = FALSE
    /\ heartbeatFresh' = FALSE
    /\ owner' = NoInstance
    /\ resolved' = resolved + 1
    /\ rowLock' = NoLock
    /\ UNCHANGED <<lease, taskLease, leader, blockedOps>>

ResolveIgnoreRelease ==
    /\ rowLock = "resolve"
    /\ jobState = "waiting_external"
    /\ callbackId = CbId
    /\ rowLock' = NoLock
    /\ UNCHANGED <<jobState, callbackId, callbackTimedOut, heartbeatFresh, owner, lease, taskLease, leader, resolved, blockedOps>>

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
    /\ UNCHANGED <<jobState, callbackId, callbackTimedOut, heartbeatFresh, owner, lease, taskLease, leader, resolved, blockedOps>>

\* Phase 2: Execute (lock held). Production can yield retryable or failed
\* depending on max_attempts; model both outcomes.
TimeoutRetryExecute ==
    /\ rowLock = "timeout_rescue"
    /\ jobState' = "retryable"
    /\ callbackId' = NoCb
    /\ callbackTimedOut' = FALSE
    /\ heartbeatFresh' = FALSE
    /\ owner' = NoInstance
    /\ resolved' = resolved + 1
    /\ rowLock' = NoLock
    /\ UNCHANGED <<lease, taskLease, leader, blockedOps>>

TimeoutFailExecute ==
    /\ rowLock = "timeout_rescue"
    /\ jobState' = "failed"
    /\ callbackId' = NoCb
    /\ callbackTimedOut' = FALSE
    /\ heartbeatFresh' = FALSE
    /\ owner' = NoInstance
    /\ resolved' = resolved + 1
    /\ rowLock' = NoLock
    /\ UNCHANGED <<lease, taskLease, leader, blockedOps>>

\* ─── rescue_stale_heartbeats: SKIP LOCKED UPDATE ──────────

\* Phase 1: Try to lock. Free → lock. Locked → skip.
HeartbeatTryLock(i) ==
    /\ leader = i
    /\ jobState = "running"
    /\ owner \in Instances
    /\ callbackId = CbId
    /\ ~heartbeatFresh
    /\ rowLock = NoLock
    /\ rowLock' = "heartbeat_rescue"
    /\ UNCHANGED <<jobState, callbackId, callbackTimedOut, heartbeatFresh, owner, lease, taskLease, leader, resolved, blockedOps>>

\* Phase 2: Execute (lock held). Clears callback_id.
HeartbeatExecute ==
    /\ rowLock = "heartbeat_rescue"
    /\ jobState' = "retryable"
    /\ callbackId' = NoCb
    /\ callbackTimedOut' = FALSE
    /\ heartbeatFresh' = FALSE
    /\ owner' = NoInstance
    /\ rowLock' = NoLock
    /\ resolved' = resolved + 1
    /\ UNCHANGED <<lease, taskLease, leader, blockedOps>>

\* ─── Specification ────────────────────────────────────────

Next ==
    \/ \E i \in Instances : Claim(i)
    \/ \E i \in Instances : RegisterCallback(i)
    \/ \E i \in Instances : EnterWaiting(i)
    \/ \E op \in BlockingOps : BlockingTryLock(op)
    \/ \E op \in BlockingOps : BlockingBlock(op)
    \/ \E op \in BlockingOps : BlockingReEvaluate(op)
    \/ CompleteExecute
    \/ FailExecute
    \/ ResolveCompleteExecute
    \/ ResolveFailExecute
    \/ ResolveIgnoreRelease
    \/ TimeoutExpires
    \/ \E i \in Instances : TimeoutTryLock(i)
    \/ TimeoutRetryExecute
    \/ TimeoutFailExecute
    \/ HeartbeatStale
    \/ \E i \in Instances : HeartbeatTryLock(i)
    \/ HeartbeatExecute
    \/ PromoteRetryable
    \/ \E i \in Instances : AcquireLeader(i)
    \/ \E i \in Instances : LoseLeader(i)

StableNext ==
    \/ \E i \in Instances : Claim(i)
    \/ \E i \in Instances : RegisterCallback(i)
    \/ \E i \in Instances : EnterWaiting(i)
    \/ \E op \in BlockingOps : BlockingTryLock(op)
    \/ \E op \in BlockingOps : BlockingBlock(op)
    \/ \E op \in BlockingOps : BlockingReEvaluate(op)
    \/ CompleteExecute
    \/ FailExecute
    \/ ResolveCompleteExecute
    \/ ResolveFailExecute
    \/ ResolveIgnoreRelease
    \/ TimeoutExpires
    \/ \E i \in Instances : TimeoutTryLock(i)
    \/ TimeoutRetryExecute
    \/ TimeoutFailExecute
    \/ HeartbeatStale
    \/ \E i \in Instances : HeartbeatTryLock(i)
    \/ HeartbeatExecute
    \/ PromoteRetryable
    \/ \E i \in Instances : AcquireLeader(i)

Spec == Init /\ [][Next]_vars
StableSpec == Init /\ [][StableNext]_vars

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
    /\ blockedOps \subseteq BlockingOps

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
    /\ rowLock = "fail" =>
        (callbackId = CbId /\ jobState \in {"waiting_external", "running"})
    /\ rowLock = "resolve" =>
        (jobState = "waiting_external" /\ callbackId = CbId)
    /\ rowLock = "timeout_rescue" =>
        (jobState = "waiting_external" /\ callbackId = CbId /\ callbackTimedOut)
    /\ rowLock = "heartbeat_rescue" =>
        (jobState = "running" /\ callbackId = CbId /\ ~heartbeatFresh)

\* A blocked operation never appears as the current lock holder.
BlockedNotSelfLocked ==
    rowLock = NoLock \/ rowLock \notin blockedOps

\* ─── Liveness (under availability) ────────────────────────

FairnessAssumptions ==
    /\ WF_vars(\E i \in Instances : AcquireLeader(i))
    /\ WF_vars(TimeoutExpires)
    /\ WF_vars(PromoteRetryable)
    /\ SF_vars(\E i \in Instances : TimeoutTryLock(i))
    /\ WF_vars(TimeoutRetryExecute)
    /\ WF_vars(TimeoutFailExecute)
    /\ WF_vars(CompleteExecute)
    /\ WF_vars(FailExecute)
    /\ WF_vars(ResolveCompleteExecute)
    /\ WF_vars(ResolveFailExecute)
    /\ WF_vars(ResolveIgnoreRelease)
    /\ WF_vars(HeartbeatExecute)
    /\ \A op \in BlockingOps : WF_vars(BlockingReEvaluate(op))

FairSpec ==
    Spec /\ FairnessAssumptions

\* Liveness is checked under a stable-cluster assumption: once a leader is
\* acquired, it is not lost. This matches the issue scope for timeout rescue.
StableFairSpec ==
    StableSpec /\ FairnessAssumptions

\* A timed-out waiting job is eventually rescued or resolved.
TimedOutEventuallyLeaves ==
    (jobState = "waiting_external" /\ callbackTimedOut)
        ~> (jobState # "waiting_external")

====

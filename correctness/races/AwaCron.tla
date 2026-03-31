---- MODULE AwaCron ----
EXTENDS FiniteSets, Naturals

(*
  Cron double-fire prevention model.

  Models the interaction between the maintenance leader's cron evaluation
  loop and the atomic_enqueue CTE. The key safety property: each fire time
  produces at most one enqueued job, even under leader failover where
  multiple instances may concurrently attempt the CAS.

  Maps to code:
    ReadCronState  -> maintenance.rs: evaluate_cron_schedules calls list_cron_jobs
    AtomicEnqueue  -> cron.rs: atomic_enqueue CTE (UPDATE...WHERE last_enqueued_at
                     IS NOT DISTINCT FROM $3, then INSERT...FROM mark)
    CASFail        -> CTE UPDATE matches 0 rows, INSERT produces nothing

  The read (list_cron_jobs) and write (atomic_enqueue) are separate DB
  operations. Between them, leadership can change and another instance
  can claim the same fire. The CAS prevents duplicate enqueues.
*)

Instances == {"A", "B"}

\* Abstract fire times as integers 1..MaxFire.
\* A fire is "due" when clock >= fire.
MaxFire == 2
FireTimes == 1..MaxFire

NoLeader == "none"

VARIABLES
    leader,         \* Advisory lock holder: Instances \cup {NoLeader}
    lastEnqueued,   \* DB column cron_jobs.last_enqueued_at (0 = never)
    snapshot,       \* Per-instance: cached lastEnqueued from list_cron_jobs read
    hasSnapshot,    \* Per-instance: whether a valid snapshot exists
    clock,          \* Abstract time; fires <= clock are due
    jobCount,       \* Per fire: count of jobs created (safety target)
    alive           \* Per-instance: whether the instance process is running

vars == <<leader, lastEnqueued, snapshot, hasSnapshot, clock, jobCount, alive>>

\* ─── Initial state ────────────────────────────────────────

Init ==
    /\ leader = NoLeader
    /\ lastEnqueued = 0
    /\ snapshot = [i \in Instances |-> 0]
    /\ hasSnapshot = [i \in Instances |-> FALSE]
    /\ clock = 0
    /\ jobCount = [f \in FireTimes |-> 0]
    /\ alive = [i \in Instances |-> TRUE]

\* ─── Actions ──────────────────────────────────────────────

\* Time advances, making fires due.
AdvanceClock ==
    /\ clock < MaxFire
    /\ clock' = clock + 1
    /\ UNCHANGED <<leader, lastEnqueued, snapshot, hasSnapshot, jobCount, alive>>

\* Acquire advisory lock (pg_try_advisory_lock succeeds).
AcquireLeader(i) ==
    /\ alive[i]
    /\ leader = NoLeader
    /\ leader' = i
    /\ UNCHANGED <<lastEnqueued, snapshot, hasSnapshot, clock, jobCount, alive>>

\* Lose advisory lock: connection dies, explicit release, or shutdown.
\* The instance may still have a cached snapshot from a prior read.
LoseLeader(i) ==
    /\ leader = i
    /\ leader' = NoLeader
    /\ UNCHANGED <<lastEnqueued, snapshot, hasSnapshot, clock, jobCount, alive>>

\* Instance crashes: loses leadership, loses snapshot (stack unwound).
Crash(i) ==
    /\ alive[i]
    /\ alive' = [alive EXCEPT ![i] = FALSE]
    /\ hasSnapshot' = [hasSnapshot EXCEPT ![i] = FALSE]
    /\ leader' = IF leader = i THEN NoLeader ELSE leader
    /\ UNCHANGED <<lastEnqueued, snapshot, clock, jobCount>>

\* Instance recovers (restarts).
Recover(i) ==
    /\ ~alive[i]
    /\ alive' = [alive EXCEPT ![i] = TRUE]
    /\ UNCHANGED <<leader, lastEnqueued, snapshot, hasSnapshot, clock, jobCount>>

\* Leader reads cron state from DB (list_cron_jobs).
\* Only the leader enters evaluate_cron_schedules.
ReadCronState(i) ==
    /\ alive[i]
    /\ leader = i
    /\ snapshot' = [snapshot EXCEPT ![i] = lastEnqueued]
    /\ hasSnapshot' = [hasSnapshot EXCEPT ![i] = TRUE]
    /\ UNCHANGED <<leader, lastEnqueued, clock, jobCount, alive>>

\* The latest due fire from this instance's perspective.
\* Maps to compute_fire_time: returns the largest fire <= clock that
\* is strictly after the cached last_enqueued_at (snapshot).
\* Returns 0 if no fire is due.
LatestDueFire(i) ==
    LET candidates == {f \in FireTimes : f <= clock /\ f > snapshot[i]}
    IN IF candidates = {} THEN 0
       ELSE CHOOSE f \in candidates : \A g \in candidates : f >= g

\* Atomic CAS + insert (the atomic_enqueue CTE).
\* CAS succeeds: DB lastEnqueued matches our snapshot.
\* Precondition does NOT require current leadership — models the window
\* where leadership was lost between ReadCronState and this action.
\* Only attempts the latest due fire (compute_fire_time semantics).
AtomicEnqueue(i) ==
    LET fire == LatestDueFire(i) IN
    /\ alive[i]
    /\ hasSnapshot[i]
    /\ fire > 0                          \* a fire is due
    /\ lastEnqueued = snapshot[i]        \* CAS: DB matches what we read
    /\ lastEnqueued' = fire
    /\ jobCount' = [jobCount EXCEPT ![fire] = @ + 1]
    /\ hasSnapshot' = [hasSnapshot EXCEPT ![i] = FALSE]
    /\ UNCHANGED <<leader, snapshot, clock, alive>>

\* CAS fails: another instance already updated last_enqueued_at.
\* The CTE UPDATE matches 0 rows, INSERT produces nothing.
CASFail(i) ==
    LET fire == LatestDueFire(i) IN
    /\ alive[i]
    /\ hasSnapshot[i]
    /\ fire > 0
    /\ lastEnqueued # snapshot[i]        \* CAS fails — DB moved
    /\ hasSnapshot' = [hasSnapshot EXCEPT ![i] = FALSE]
    /\ UNCHANGED <<leader, lastEnqueued, snapshot, clock, jobCount, alive>>

\* ─── Specification ────────────────────────────────────────

Next ==
    \/ AdvanceClock
    \/ \E i \in Instances : AcquireLeader(i)
    \/ \E i \in Instances : LoseLeader(i)
    \/ \E i \in Instances : Crash(i)
    \/ \E i \in Instances : Recover(i)
    \/ \E i \in Instances : ReadCronState(i)
    \/ \E i \in Instances : AtomicEnqueue(i)
    \/ \E i \in Instances : CASFail(i)

Spec == Init /\ [][Next]_vars

\* ─── Safety invariants ────────────────────────────────────

TypeOK ==
    /\ leader \in Instances \cup {NoLeader}
    /\ lastEnqueued \in 0..MaxFire
    /\ snapshot \in [Instances -> 0..MaxFire]
    /\ hasSnapshot \in [Instances -> BOOLEAN]
    /\ clock \in 0..MaxFire
    /\ jobCount \in [FireTimes -> 0..MaxFire]
    /\ alive \in [Instances -> BOOLEAN]

\* CRITICAL SAFETY: no fire time ever produces more than one job.
NoDuplicateFire ==
    \A f \in FireTimes : jobCount[f] <= 1

\* A cached snapshot is never ahead of the current DB value.
\* Holds because: snapshots are read from lastEnqueued, and lastEnqueued
\* only increases (AtomicEnqueue: fire > snapshot[i] = lastEnqueued).
SnapshotNeverAheadOfDB ==
    \A i \in Instances :
        hasSnapshot[i] => snapshot[i] <= lastEnqueued

\* Only alive instances with snapshots attempt CAS.
SnapshotRequiresAlive ==
    \A i \in Instances : hasSnapshot[i] => alive[i]

\* Leader must be alive.
LeaderAlive ==
    leader # NoLeader => alive[leader]

\* ─── Liveness (under fairness) ────────────────────────────

\* ─── Liveness (stable cluster — no crashes) ────────────────
\*
\* Liveness requires an external availability assumption: at least one
\* instance stays alive and eventually becomes leader. In a finite model
\* where TLC can crash the entire cluster, this cannot be checked without
\* restricting the spec. We check liveness under a "stable cluster" Next
\* that omits Crash/Recover (all instances stay alive).

StableNext ==
    \/ AdvanceClock
    \/ \E i \in Instances : AcquireLeader(i)
    \/ \E i \in Instances : LoseLeader(i)
    \/ \E i \in Instances : ReadCronState(i)
    \/ \E i \in Instances : AtomicEnqueue(i)
    \/ \E i \in Instances : CASFail(i)

StableSpec == Init /\ [][StableNext]_vars

FairSpec ==
    StableSpec
    /\ WF_vars(AdvanceClock)
    /\ WF_vars(\E i \in Instances : AcquireLeader(i))
    /\ SF_vars(\E i \in Instances : ReadCronState(i))
    /\ SF_vars(\E i \in Instances : AtomicEnqueue(i))
    /\ SF_vars(\E i \in Instances : CASFail(i))

\* The latest due fire is eventually enqueued.
\* Note: earlier fires may be skipped (no-backfill design) — the code's
\* compute_fire_time only returns the latest fire, so if fire 1 and 2
\* are both due, only fire 2 is enqueued.
\* Checked under FairSpec (stable cluster with no crashes).
LatestFireEventuallyEnqueued ==
    (clock = MaxFire) ~> (jobCount[MaxFire] = 1)

====

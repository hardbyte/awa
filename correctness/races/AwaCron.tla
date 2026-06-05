---- MODULE AwaCron ----
EXTENDS FiniteSets, Naturals

(*
  Cron double-fire prevention and pause-gated firing model.

  Models the interaction between the maintenance leader's cron evaluation
  loop and the atomic_enqueue CTE. Key safety properties:

    1. Each fire time produces at most one enqueued job, even under leader
       failover where multiple instances may concurrently attempt the CAS.
    2. While a schedule is paused, no fire time ever produces a job. The
       atomic_enqueue CTE's `paused_at IS NULL` guard means a stale snapshot
       carried across a pause still cannot enqueue.

  The cron evaluator can run either coalesced (enqueue only the latest
  missed fire) or catch-up (enqueue every missed fire in timestamp order).
  The safety argument is the same for both: every enqueue advances
  last_enqueued_at with a compare-and-swap against the evaluator's snapshot,
  and the same UPDATE also re-checks paused_at.

  Maps to code:
    ReadCronState  -> maintenance.rs: evaluate_cron_schedules calls
                      list_cron_jobs (paused rows are skipped up front in
                      Rust; the model exercises the harder case where the
                      paused flag flips between read and CAS)
    ChosenFire     -> maintenance.rs: compute_fire_times
    AtomicEnqueue  -> cron.rs: atomic_enqueue CTE
                      (UPDATE...WHERE last_enqueued_at IS NOT DISTINCT FROM $3
                       AND paused_at IS NULL, then INSERT...FROM mark)
    CASFail        -> CTE UPDATE matches 0 rows: either last_enqueued_at
                      moved (another leader won) OR paused_at became NOT NULL
                      (operator paused the schedule)
    Pause / Resume -> cron.rs: pause_cron_job / resume_cron_job

  The read (list_cron_jobs) and write (atomic_enqueue) are separate DB
  operations. Between them, leadership can change, another instance can
  claim the same fire, or an operator can pause the schedule. The CAS plus
  the paused-row guard make all three safe.
*)

Instances == {"A", "B"}

\* Abstract fire times as integers 1..MaxFire.
\* A fire is "due" when clock >= fire.
MaxFire == 2
FireTimes == 1..MaxFire

NoLeader == "none"
Policies == {"coalesce", "catch_up"}

VARIABLES
    leader,         \* Advisory lock holder: Instances \cup {NoLeader}
    policy,         \* Cron missed-fire policy for this schedule
    paused,         \* Schedule pause flag (cron_jobs.paused_at IS NOT NULL)
    lastEnqueued,   \* DB column cron_jobs.last_enqueued_at (0 = never)
    snapshot,       \* Per-instance: cached lastEnqueued from list_cron_jobs read
    hasSnapshot,    \* Per-instance: whether a valid snapshot exists
    clock,          \* Abstract time; fires <= clock are due
    jobCount,       \* Per fire: count of jobs created (safety target)
    alive           \* Per-instance: whether the instance process is running

vars == <<leader, policy, paused, lastEnqueued, snapshot, hasSnapshot, clock, jobCount, alive>>

\* ─── Initial state ────────────────────────────────────────

Init ==
    /\ leader = NoLeader
    /\ policy \in Policies
    /\ paused \in BOOLEAN
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
    /\ UNCHANGED <<leader, policy, paused, lastEnqueued, snapshot, hasSnapshot, jobCount, alive>>

\* Acquire advisory lock (pg_try_advisory_lock succeeds).
AcquireLeader(i) ==
    /\ alive[i]
    /\ leader = NoLeader
    /\ leader' = i
    /\ UNCHANGED <<policy, paused, lastEnqueued, snapshot, hasSnapshot, clock, jobCount, alive>>

\* Lose advisory lock: connection dies, explicit release, or shutdown.
\* The instance may still have a cached snapshot from a prior read.
LoseLeader(i) ==
    /\ leader = i
    /\ leader' = NoLeader
    /\ UNCHANGED <<policy, paused, lastEnqueued, snapshot, hasSnapshot, clock, jobCount, alive>>

\* Instance crashes: loses leadership, loses snapshot (stack unwound).
Crash(i) ==
    /\ alive[i]
    /\ alive' = [alive EXCEPT ![i] = FALSE]
    /\ hasSnapshot' = [hasSnapshot EXCEPT ![i] = FALSE]
    /\ leader' = IF leader = i THEN NoLeader ELSE leader
    /\ UNCHANGED <<policy, paused, lastEnqueued, snapshot, clock, jobCount>>

\* Instance recovers (restarts).
Recover(i) ==
    /\ ~alive[i]
    /\ alive' = [alive EXCEPT ![i] = TRUE]
    /\ UNCHANGED <<leader, policy, paused, lastEnqueued, snapshot, hasSnapshot, clock, jobCount>>

\* Operator pauses the schedule (pause_cron_job). Global DB state — does
\* not require leadership. May race with a leader holding a snapshot.
Pause ==
    /\ ~paused
    /\ paused' = TRUE
    /\ UNCHANGED <<leader, policy, lastEnqueued, snapshot, hasSnapshot, clock, jobCount, alive>>

\* Operator resumes the schedule (resume_cron_job). Snapshots that
\* survived through a pause/resume cycle are still valid: lastEnqueued
\* does not change while paused, so the CAS will still match.
Resume ==
    /\ paused
    /\ paused' = FALSE
    /\ UNCHANGED <<leader, policy, lastEnqueued, snapshot, hasSnapshot, clock, jobCount, alive>>

\* Leader reads cron state from DB (list_cron_jobs).
\* Only the leader enters evaluate_cron_schedules. The Rust code skips
\* paused rows before calling atomic_enqueue, but the model allows the
\* read to happen regardless of pause state to exercise the harder case
\* where pause flips between read and CAS.
ReadCronState(i) ==
    /\ alive[i]
    /\ leader = i
    /\ snapshot' = [snapshot EXCEPT ![i] = lastEnqueued]
    /\ hasSnapshot' = [hasSnapshot EXCEPT ![i] = TRUE]
    /\ UNCHANGED <<leader, policy, paused, lastEnqueued, clock, jobCount, alive>>

\* Due fires from this instance's perspective.
DueFires(i) == {f \in FireTimes : f <= clock /\ f > snapshot[i]}

\* Coalesced policy: enqueue the latest due fire only.
LatestDueFire(i) ==
    LET candidates == DueFires(i)
    IN IF candidates = {} THEN 0
       ELSE CHOOSE f \in candidates : \A g \in candidates : f >= g

\* Catch-up policy: enqueue due fires in timestamp order.
EarliestDueFire(i) ==
    LET candidates == DueFires(i)
    IN IF candidates = {} THEN 0
       ELSE CHOOSE f \in candidates : \A g \in candidates : f <= g

ChosenFire(i) ==
    IF policy = "coalesce"
    THEN LatestDueFire(i)
    ELSE EarliestDueFire(i)

MoreDueAfter(i, fire) ==
    \E f \in FireTimes : f <= clock /\ f > fire

\* Atomic CAS + insert (the atomic_enqueue CTE).
\* Both predicates must hold for the UPDATE to match a row:
\*   lastEnqueued = snapshot[i]    (CAS against the read)
\*   ~paused                        (paused_at IS NULL guard)
\* If either fails the UPDATE matches 0 rows and the INSERT produces
\* nothing — modeled by CASFail.
\* Precondition does NOT require current leadership — models the window
\* where leadership was lost between ReadCronState and this action.
AtomicEnqueue(i) ==
    LET fire == ChosenFire(i) IN
    /\ alive[i]
    /\ hasSnapshot[i]
    /\ fire > 0                          \* a fire is due
    /\ lastEnqueued = snapshot[i]        \* CAS: DB matches what we read
    /\ ~paused                           \* paused_at IS NULL guard
    /\ lastEnqueued' = fire
    /\ jobCount' = [jobCount EXCEPT ![fire] = @ + 1]
    /\ IF policy = "catch_up" /\ MoreDueAfter(i, fire)
       THEN
           /\ snapshot' = [snapshot EXCEPT ![i] = fire]
           /\ hasSnapshot' = hasSnapshot
       ELSE
           /\ snapshot' = snapshot
           /\ hasSnapshot' = [hasSnapshot EXCEPT ![i] = FALSE]
    /\ UNCHANGED <<leader, policy, paused, clock, alive>>

\* The CTE UPDATE matches 0 rows. Two physical causes:
\*   - another instance already updated last_enqueued_at (CAS mismatch)
\*   - the row was paused between read and CAS (paused_at IS NOT NULL)
\* In either case the INSERT produces nothing and the instance discards
\* its snapshot.
CASFail(i) ==
    LET fire == ChosenFire(i) IN
    /\ alive[i]
    /\ hasSnapshot[i]
    /\ fire > 0
    /\ (lastEnqueued # snapshot[i] \/ paused)
    /\ hasSnapshot' = [hasSnapshot EXCEPT ![i] = FALSE]
    /\ UNCHANGED <<leader, policy, paused, lastEnqueued, snapshot, clock, jobCount, alive>>

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
    \/ Pause
    \/ Resume

Spec == Init /\ [][Next]_vars

\* ─── Safety invariants ────────────────────────────────────

TypeOK ==
    /\ leader \in Instances \cup {NoLeader}
    /\ policy \in Policies
    /\ paused \in BOOLEAN
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
\* Pause / Resume do not modify lastEnqueued so they cannot break this.
SnapshotNeverAheadOfDB ==
    \A i \in Instances :
        hasSnapshot[i] => snapshot[i] <= lastEnqueued

\* Only alive instances with snapshots attempt CAS.
SnapshotRequiresAlive ==
    \A i \in Instances : hasSnapshot[i] => alive[i]

\* Leader must be alive.
LeaderAlive ==
    leader # NoLeader => alive[leader]

\* While paused, no new fires are enqueued. This is a temporal property
\* (action formula) rather than a state invariant — it asserts that any
\* step taken from a paused state leaves jobCount unchanged. Enforced by
\* AtomicEnqueue's ~paused precondition; checking it here means a future
\* refactor that drops that precondition fails the model loudly.
PausedBlocksEnqueue ==
    [][paused => UNCHANGED jobCount]_vars

\* ─── Liveness (under fairness) ────────────────────────────
\*
\* Liveness requires external assumptions:
\*   - at least one instance stays alive and eventually becomes leader, and
\*   - the operator stops adversarially pausing: a paused schedule will
\*     eventually be resumed, after which it stays active.
\*
\* In a finite model where TLC can crash the entire cluster or storm
\* Pause / Resume, neither liveness claim can be checked without
\* restricting the spec. StableNext omits Crash, Recover, and Pause but
\* keeps Resume so an initially-paused schedule can become active. Weak
\* fairness on Resume ensures that any pause eventually clears. Safety
\* (Spec, PausedBlocksEnqueue) is still checked across the full Next
\* that includes the omitted actions — the restriction only narrows
\* what liveness means, not what safety must hold under.

StableNext ==
    \/ AdvanceClock
    \/ \E i \in Instances : AcquireLeader(i)
    \/ \E i \in Instances : LoseLeader(i)
    \/ \E i \in Instances : ReadCronState(i)
    \/ \E i \in Instances : AtomicEnqueue(i)
    \/ \E i \in Instances : CASFail(i)
    \/ Resume

StableSpec == Init /\ [][StableNext]_vars

FairSpec ==
    StableSpec
    /\ WF_vars(AdvanceClock)
    /\ WF_vars(\E i \in Instances : AcquireLeader(i))
    /\ SF_vars(\E i \in Instances : ReadCronState(i))
    /\ SF_vars(\E i \in Instances : AtomicEnqueue(i))
    /\ SF_vars(\E i \in Instances : CASFail(i))
    /\ WF_vars(Resume)

\* Coalesced schedules eventually enqueue the latest due fire.
\* Checked under FairSpec (stable cluster, no adversarial pause/resume).
CoalescedLatestFireEventuallyEnqueued ==
    (policy = "coalesce" /\ clock = MaxFire) ~> (jobCount[MaxFire] = 1)

\* Catch-up schedules eventually enqueue every missed fire in order.
\* Checked under FairSpec (stable cluster, no adversarial pause/resume).
CatchUpFiresEventuallyEnqueued ==
    (policy = "catch_up" /\ clock = MaxFire) ~> (\A f \in FireTimes : jobCount[f] = 1)

====

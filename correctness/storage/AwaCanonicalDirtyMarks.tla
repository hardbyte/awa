---- MODULE AwaCanonicalDirtyMarks ----
EXTENDS TLC, Naturals, Sequences, FiniteSets

\* Lock-wait model for the canonical engine's admin dirty-key triggers.
\*
\* Every INSERT / UPDATE / DELETE on awa.jobs_hot or awa.scheduled_jobs fires a
\* statement-level trigger that records "queue q / kind k needs its cached
\* admin counts recomputed". The v006 design wrote that mark with
\*
\*     INSERT INTO awa.admin_dirty_queues (queue) ... ON CONFLICT (queue) DO NOTHING
\*
\* against a table keyed by queue. ON CONFLICT DO NOTHING is free only when
\* the conflicting row is committed; against a row another transaction has
\* inserted (or deleted) and not yet committed it blocks until that
\* transaction ends. The maintenance drain deletes marks every ~2s, so the
\* "row absent, first inserter uncommitted" window recurs constantly.
\*
\* That wait is a waits-for edge between transactions that share nothing but
\* a queue or kind name. This spec shows the class:
\*
\*   - two application transactions that enqueue through awa.jobs and also
\*     lock an application row, in opposite orders (`app_*` plans), and
\*   - two worker completions with follow-up enqueues (ADR-029) that touch two
\*     queues in opposite orders (`followup_*` plans).
\*
\* With WaitFree = FALSE (v006 semantics) the marks close a cycle and
\* NoDeadlock is violated — for the application pair, and for the exact
\* production pair (`IncidentTxs`): two re-schedules in one queue whose
\* DELETE-trigger and INSERT-trigger marks straddle a drain, so one takes
\* k1 before q1 while the other takes q1 before k1. With WaitFree = TRUE (v046: append-only mark
\* tables with no unique index, so the trigger INSERT is a plain heap insert)
\* no hot-path transaction ever waits on a mark, and the only waits left are
\* the application's own row locks, which cannot cycle through awa.
\*
\* Modelled: mark rows as absent / uncommitted-by-tx / committed, the drain
\* deleting a committed mark, exclusive row locks, blocking, commit releasing
\* waiters. Not modelled: MVCC beyond visibility of the mark row, the recount
\* itself, the deadlock detector's choice of victim (a cycle is flagged as a
\* safety violation instead).

CONSTANTS WaitFree, Txs

Keys == {"q1", "q2", "k1"}
Rows == {"r"}
None == "none"

\* The two plan families a config can pick from:
\*   AppTxs      — application transactions that enqueue and lock a row in
\*                 opposite orders, plus ADR-029 follow-up enqueues.
\*   IncidentTxs — the 2026-09-12 production shape: two Snooze re-schedules of
\*                 different jobs in the same queue q1 / kind k1. One CTE fires
\*                 the jobs_hot DELETE trigger and then the scheduled_jobs
\*                 INSERT trigger, each marking queue then kind, and a claim
\*                 UPDATE trigger marks only the queue (kind unchanged). With
\*                 the drain deleting q1 between a re-schedule's two triggers,
\*                 the two re-schedules acquire q1/k1 in opposite orders.
AppTxs == {"app_mark_then_row", "app_row_then_mark", "followup_q1_q2", "followup_q2_q1"}
IncidentTxs == {"reschedule_a", "reschedule_b", "claim_q1"}

Mark(k) == [kind |-> "mark", key |-> k]
Row(r) == [kind |-> "row", row |-> r]

\* Each plan is the ordered sequence of shared resources a transaction touches.
\*   app_mark_then_row : INSERT INTO awa.jobs (queue q1) ... ; UPDATE app_row r
\*   app_row_then_mark : UPDATE app_row r ; INSERT INTO awa.jobs (queue q1)
\*   followup_q1_q2    : re-schedule a q1 job, then follow-up enqueue into q2
\*   followup_q2_q1    : re-schedule a q2 job, then follow-up enqueue into q1
Plan(t) == CASE t = "app_mark_then_row" -> << Mark("q1"), Row("r") >>
             [] t = "app_row_then_mark" -> << Row("r"), Mark("q1") >>
             [] t = "followup_q1_q2"    -> << Mark("q1"), Mark("q2") >>
             [] t = "followup_q2_q1"    -> << Mark("q2"), Mark("q1") >>
             [] t = "reschedule_a"      -> << Mark("q1"), Mark("k1"), Mark("q1"), Mark("k1") >>
             [] t = "reschedule_b"      -> << Mark("q1"), Mark("k1"), Mark("q1"), Mark("k1") >>
             [] t = "claim_q1"          -> << Mark("q1") >>

VARIABLES
    pc,             \* next plan step per transaction
    waitingOn,      \* transaction each transaction is blocked on, or None
    waitKind,       \* what kind of resource it is blocked on
    done,           \* committed
    markOwner,      \* transaction holding an uncommitted mark row per key, or None
    markCommitted,  \* a committed mark row exists for the key
    rowHolder       \* transaction holding the application row lock, or None

vars == << pc, waitingOn, waitKind, done, markOwner, markCommitted, rowHolder >>

TypeOK ==
    /\ pc \in [Txs -> 1..5]
    /\ waitingOn \in [Txs -> Txs \cup {None}]
    /\ waitKind \in [Txs -> {"none", "mark", "row"}]
    /\ done \in [Txs -> BOOLEAN]
    /\ markOwner \in [Keys -> Txs \cup {None}]
    /\ markCommitted \in [Keys -> BOOLEAN]
    /\ rowHolder \in [Rows -> Txs \cup {None}]

Init ==
    /\ pc = [t \in Txs |-> 1]
    /\ waitingOn = [t \in Txs |-> None]
    /\ waitKind = [t \in Txs |-> "none"]
    /\ done = [t \in Txs |-> FALSE]
    /\ markOwner = [k \in Keys |-> None]
    /\ markCommitted \in [Keys -> BOOLEAN]
    /\ rowHolder = [r \in Rows |-> None]

Active(t) == ~done[t] /\ pc[t] <= Len(Plan(t))

Advance(t) == pc' = [pc EXCEPT ![t] = @ + 1]

Block(t, holder, kind) ==
    /\ waitingOn' = [waitingOn EXCEPT ![t] = holder]
    /\ waitKind' = [waitKind EXCEPT ![t] = kind]
    /\ UNCHANGED << pc, markOwner, markCommitted, rowHolder >>

\* The trigger's mark write.
MarkStep(t, k) ==
    IF WaitFree THEN
        \* v046: plain INSERT into an unconstrained table. Nothing to conflict
        \* with, nothing to wait for.
        /\ Advance(t)
        /\ UNCHANGED << waitingOn, waitKind, markOwner, markCommitted, rowHolder >>
    ELSE IF markCommitted[k] THEN
        \* v006: the keyed row exists and is visible; ON CONFLICT DO NOTHING
        \* is a read.
        /\ Advance(t)
        /\ UNCHANGED << waitingOn, waitKind, markOwner, markCommitted, rowHolder >>
    ELSE IF markOwner[k] = None THEN
        \* v006: row absent (never marked, or drained). This transaction
        \* inserts it and holds it uncommitted until commit.
        /\ markOwner' = [markOwner EXCEPT ![k] = t]
        /\ Advance(t)
        /\ UNCHANGED << waitingOn, waitKind, markCommitted, rowHolder >>
    ELSE IF markOwner[k] = t THEN
        /\ Advance(t)
        /\ UNCHANGED << waitingOn, waitKind, markOwner, markCommitted, rowHolder >>
    ELSE
        \* v006: another transaction's uncommitted row conflicts; Postgres
        \* waits for that transaction ("ShareLock on transaction").
        Block(t, markOwner[k], "mark")

\* An exclusive row lock owned by the application (UPDATE / SELECT FOR UPDATE).
RowStep(t, r) ==
    IF rowHolder[r] = None \/ rowHolder[r] = t THEN
        /\ rowHolder' = [rowHolder EXCEPT ![r] = t]
        /\ Advance(t)
        /\ UNCHANGED << waitingOn, waitKind, markOwner, markCommitted >>
    ELSE
        Block(t, rowHolder[r], "row")

Step(t) ==
    /\ Active(t)
    /\ waitingOn[t] = None
    /\ LET s == Plan(t)[pc[t]] IN
         IF s.kind = "mark" THEN MarkStep(t, s.key) ELSE RowStep(t, s.row)
    /\ UNCHANGED done

\* Commit publishes this transaction's mark rows, releases its row locks, and
\* wakes everything blocked on it (they retry their step).
Commit(t) ==
    /\ ~done[t]
    /\ pc[t] > Len(Plan(t))
    /\ done' = [done EXCEPT ![t] = TRUE]
    /\ markCommitted' = [k \in Keys |-> markCommitted[k] \/ markOwner[k] = t]
    /\ markOwner' = [k \in Keys |-> IF markOwner[k] = t THEN None ELSE markOwner[k]]
    /\ rowHolder' = [r \in Rows |-> IF rowHolder[r] = t THEN None ELSE rowHolder[r]]
    /\ waitingOn' = [u \in Txs |-> IF waitingOn[u] = t THEN None ELSE waitingOn[u]]
    /\ waitKind' = [u \in Txs |-> IF waitingOn[u] = t THEN "none" ELSE waitKind[u]]
    /\ UNCHANGED pc

\* The maintenance drain deletes a committed mark row (recompute_dirty_admin_metadata).
\* Modelled as instantaneous; the v006 drain additionally held the deleted row
\* uncommitted for the whole recompute, which only widens the window.
Drain(k) ==
    /\ markCommitted[k]
    /\ markCommitted' = [markCommitted EXCEPT ![k] = FALSE]
    /\ UNCHANGED << pc, waitingOn, waitKind, done, markOwner, rowHolder >>

Terminating ==
    /\ \A t \in Txs : done[t]
    /\ UNCHANGED vars

Next ==
    \/ \E t \in Txs : Step(t) \/ Commit(t)
    \/ \E k \in Keys : Drain(k)
    \/ Terminating

Spec == Init /\ [][Next]_vars

\* ── Invariants ─────────────────────────────────────────────────────────

RECURSIVE Follow(_, _)
Follow(t, n) == IF n = 0 \/ t = None THEN t ELSE Follow(waitingOn[t], n - 1)

InCycle(t) ==
    /\ waitingOn[t] # None
    /\ \E n \in 1..Cardinality(Txs) : Follow(t, n) = t

\* The waits-for graph is acyclic.
NoDeadlock == \A t \in Txs : ~InCycle(t)

\* No job-transition transaction is ever blocked by the dirty-key trigger.
\* This is the property v046 restores: the trigger cannot be a deadlock edge
\* if it never waits.
HotPathMarksNeverWait == \A t \in Txs : waitKind[t] # "mark"

====

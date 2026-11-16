---- MODULE AwaMaintenanceCursorAdvance ----
EXTENDS Naturals, FiniteSets, TLC

\* One ready lane `(queue, priority, enqueue_shard)` and the processes that
\* write it: an enqueuer, a claimer, a canceller/mover and the maintenance
\* leader advancing the lane's claim cursor over a spent prefix outside a
\* claim (#779). Positions are lane_seq values 1..N.
\*
\* state[p]: "free"      not reserved
\*           "inflight"  reserved by an uncommitted enqueue
\*           "gap"       reserved by an enqueue that rolled back
\*           "live"      committed ready row, claimable
\*           "claimed"   committed claim evidence for the row
\*           "tomb"      committed ready tombstone (cancel, move, reprioritise)
\*
\* A claim, a cancel and a move each hold the lane's queue_claim_heads row
\* until they commit; a cursor advance takes it for its own statement. The
\* sequence-backed cursor is non-transactional and set_sequence_next only
\* raises it. Enqueue holds the lane reservation lock until it commits, so
\* at most one reservation is in flight and enqueues commit in position
\* order. Retry and rescue re-enqueue a job at a new position at the tail;
\* that is a Reserve here, and the old position stays spent.
\*
\* Maintenance reads the lane in one statement under the head lock, taken
\* with SKIP LOCKED, and computes the cursor target as the first visible
\* unspent row, or one past the last visible row when every visible row from
\* the cursor is spent; it then raises the cursor to that target.
\*
\* MaintLocksHead  maintenance takes the head lock (production: TRUE).
\* SkipInvisible   broken: maintenance treats reserved-but-invisible
\*                 positions as spent, i.e. advances to the enqueue head.
\* DirtyTombstone  broken: maintenance treats a tombstone written by an
\*                 uncommitted cancel or move as committed.
CONSTANTS N, MaintLocksHead, SkipInvisible, DirtyTombstone

Positions == 1..N
Spent == {"claimed", "tomb"}
Visible == {"live", "claimed", "tomb"}
NoPos == 0

VARIABLES state, append, cursor, headLock, claiming, tombing, tombAdvance,
          pendingClaimAdvance, pendingCancelAdvance, maintPhase, maintTarget,
          claimCount
vars == <<state, append, cursor, headLock, claiming, tombing, tombAdvance,
          pendingClaimAdvance, pendingCancelAdvance, maintPhase, maintTarget,
          claimCount>>

Max(a, b) == IF a > b THEN a ELSE b

Init ==
    /\ state = [p \in Positions |-> "free"]
    /\ append = 1
    /\ cursor = 1
    /\ headLock = "none"
    /\ claiming = NoPos
    /\ tombing = NoPos
    /\ tombAdvance = FALSE
    /\ pendingClaimAdvance = {}
    /\ pendingCancelAdvance = {}
    /\ maintPhase = "idle"
    /\ maintTarget = 1
    /\ claimCount = [p \in Positions |-> 0]

\* ---- enqueue -------------------------------------------------------------

Reserve ==
    /\ append <= N
    /\ ~ \E p \in Positions : state[p] = "inflight"
    /\ state' = [state EXCEPT ![append] = "inflight"]
    /\ append' = append + 1
    /\ UNCHANGED <<cursor, headLock, claiming, tombing, tombAdvance,
                   pendingClaimAdvance, pendingCancelAdvance, maintPhase,
                   maintTarget, claimCount>>

EnqueueCommit(p) ==
    /\ state[p] = "inflight"
    /\ state' = [state EXCEPT ![p] = "live"]
    /\ UNCHANGED <<append, cursor, headLock, claiming, tombing, tombAdvance,
                   pendingClaimAdvance, pendingCancelAdvance, maintPhase,
                   maintTarget, claimCount>>

EnqueueAbort(p) ==
    /\ state[p] = "inflight"
    /\ state' = [state EXCEPT ![p] = "gap"]
    /\ UNCHANGED <<append, cursor, headLock, claiming, tombing, tombAdvance,
                   pendingClaimAdvance, pendingCancelAdvance, maintPhase,
                   maintTarget, claimCount>>

\* ---- claim ---------------------------------------------------------------

\* claim_ready_runtime locks the head with SKIP LOCKED and claims the first
\* visible untombstoned row at or after the cursor.
ClaimHead ==
    {p \in Positions :
        /\ p >= cursor
        /\ state[p] \in {"live", "claimed"}
        /\ \A q \in Positions : (q >= cursor /\ q < p) =>
                state[q] \notin {"live", "claimed"}}

ClaimBegin ==
    /\ headLock = "none"
    /\ \E p \in ClaimHead :
        /\ state[p] = "live"
        /\ headLock' = "claim"
        /\ claiming' = p
    /\ UNCHANGED <<state, append, cursor, tombing, tombAdvance,
                   pendingClaimAdvance, pendingCancelAdvance, maintPhase,
                   maintTarget, claimCount>>

ClaimCommit ==
    /\ headLock = "claim"
    /\ state' = [state EXCEPT ![claiming] = "claimed"]
    /\ claimCount' = [claimCount EXCEPT ![claiming] = @ + 1]
    /\ pendingClaimAdvance' = pendingClaimAdvance \cup {claiming}
    /\ headLock' = "none"
    /\ claiming' = NoPos
    /\ UNCHANGED <<append, cursor, tombing, tombAdvance, pendingCancelAdvance,
                   maintPhase, maintTarget>>

ClaimAbort ==
    /\ headLock = "claim"
    /\ headLock' = "none"
    /\ claiming' = NoPos
    /\ UNCHANGED <<state, append, cursor, tombing, tombAdvance,
                   pendingClaimAdvance, pendingCancelAdvance, maintPhase,
                   maintTarget, claimCount>>

\* When the head row is already spent, the claim function moves the cursor
\* over the contiguous spent prefix and returns without claiming.
ClaimRepair ==
    /\ headLock = "none"
    /\ cursor <= N
    /\ state[cursor] \in Spent
    /\ cursor' = cursor + 1
    /\ UNCHANGED <<state, append, headLock, claiming, tombing, tombAdvance,
                   pendingClaimAdvance, pendingCancelAdvance, maintPhase,
                   maintTarget, claimCount>>

\* Post-commit advance of a committed claim, evidence-guarded and monotone.
ClaimAdvance(p) ==
    /\ p \in pendingClaimAdvance
    /\ headLock = "none"
    /\ state[p] = "claimed"
    /\ cursor' = Max(cursor, p + 1)
    /\ pendingClaimAdvance' = pendingClaimAdvance \ {p}
    /\ UNCHANGED <<state, append, headLock, claiming, tombing, tombAdvance,
                   pendingCancelAdvance, maintPhase, maintTarget, claimCount>>

\* ---- cancel / move / set_priority ----------------------------------------

\* `withAdvance`: a cancel, which queues an only_if_current advance. Move
\* and set_priority tombstone without one.
TombBegin(p, withAdvance) ==
    /\ headLock = "none"
    /\ state[p] = "live"
    /\ headLock' = "tomb"
    /\ tombing' = p
    /\ tombAdvance' = withAdvance
    /\ UNCHANGED <<state, append, cursor, claiming, pendingClaimAdvance,
                   pendingCancelAdvance, maintPhase, maintTarget, claimCount>>

TombCommit ==
    /\ headLock = "tomb"
    /\ state' = [state EXCEPT ![tombing] = "tomb"]
    /\ pendingCancelAdvance' =
           IF tombAdvance THEN pendingCancelAdvance \cup {tombing}
           ELSE pendingCancelAdvance
    /\ headLock' = "none"
    /\ tombing' = NoPos
    /\ tombAdvance' = FALSE
    /\ UNCHANGED <<append, cursor, claiming, pendingClaimAdvance, maintPhase,
                   maintTarget, claimCount>>

TombAbort ==
    /\ headLock = "tomb"
    /\ headLock' = "none"
    /\ tombing' = NoPos
    /\ tombAdvance' = FALSE
    /\ UNCHANGED <<state, append, cursor, claiming, pendingClaimAdvance,
                   pendingCancelAdvance, maintPhase, maintTarget, claimCount>>

\* only_if_current: moves the cursor by one only when it is at the row.
CancelAdvance(p) ==
    /\ p \in pendingCancelAdvance
    /\ headLock = "none"
    /\ cursor' = IF cursor = p THEN p + 1 ELSE cursor
    /\ pendingCancelAdvance' = pendingCancelAdvance \ {p}
    /\ UNCHANGED <<state, append, headLock, claiming, tombing, tombAdvance,
                   pendingClaimAdvance, maintPhase, maintTarget, claimCount>>

\* ---- maintenance ---------------------------------------------------------

\* What the maintenance statement reads at position p.
Seen(p) ==
    IF DirtyTombstone /\ tombing = p THEN "tomb"
    ELSE IF SkipInvisible /\ state[p] \in {"inflight", "gap"} THEN "gap"
    ELSE state[p]

Window == {p \in Positions : p >= cursor /\ p < append}
SeenLive == {p \in Window : Seen(p) = "live"}
SeenVisible ==
    IF SkipInvisible THEN Window
    ELSE {p \in Window : Seen(p) \in Visible}
SetMin(S) == CHOOSE p \in S : \A q \in S : p <= q
SetMax(S) == CHOOSE p \in S : \A q \in S : p >= q

MaintTarget ==
    IF SeenLive # {} THEN SetMin(SeenLive)
    ELSE IF SeenVisible = {} THEN cursor
    ELSE SetMax(SeenVisible) + 1

MaintScan ==
    /\ maintPhase = "idle"
    /\ IF MaintLocksHead
       THEN /\ headLock = "none"
            /\ headLock' = "maint"
       ELSE UNCHANGED headLock
    /\ maintTarget' = MaintTarget
    /\ maintPhase' = "scanned"
    /\ UNCHANGED <<state, append, cursor, claiming, tombing, tombAdvance,
                   pendingClaimAdvance, pendingCancelAdvance, claimCount>>

MaintSet ==
    /\ maintPhase = "scanned"
    /\ cursor' = Max(cursor, maintTarget)
    /\ headLock' = IF MaintLocksHead THEN "none" ELSE headLock
    /\ maintPhase' = "idle"
    /\ UNCHANGED <<state, append, claiming, tombing, tombAdvance,
                   pendingClaimAdvance, pendingCancelAdvance, maintTarget,
                   claimCount>>

Next ==
    \/ Reserve
    \/ \E p \in Positions : EnqueueCommit(p) \/ EnqueueAbort(p)
    \/ ClaimBegin \/ ClaimCommit \/ ClaimAbort \/ ClaimRepair
    \/ \E p \in Positions : ClaimAdvance(p)
    \/ \E p \in Positions, adv \in BOOLEAN : TombBegin(p, adv)
    \/ TombCommit \/ TombAbort
    \/ \E p \in Positions : CancelAdvance(p)
    \/ MaintScan \/ MaintSet

Spec == Init /\ [][Next]_vars

\* ---- properties ----------------------------------------------------------

TypeOK ==
    /\ state \in [Positions -> {"free", "inflight", "gap", "live", "claimed", "tomb"}]
    /\ append \in 1..(N + 1)
    /\ cursor \in 1..(N + 1)
    /\ headLock \in {"none", "claim", "tomb", "maint"}
    /\ claiming \in Positions \cup {NoPos}
    /\ tombing \in Positions \cup {NoPos}
    /\ pendingClaimAdvance \subseteq Positions
    /\ pendingCancelAdvance \subseteq Positions
    /\ maintPhase \in {"idle", "scanned"}
    /\ maintTarget \in 1..(N + 1)

\* No row that is, or may yet become, claimable sits behind the cursor.
NoSkippedLive ==
    \A p \in Positions : p < cursor => state[p] \notin {"live", "inflight"}

\* A row is claimed at most once.
NoDoubleClaim == \A p \in Positions : claimCount[p] <= 1

\* The cursor never passes the enqueue head.
CursorBounded == cursor <= append

\* The cursor only moves forward, and spent or rolled-back positions never
\* become claimable again.
CursorMonotone == [][cursor' >= cursor]_vars
\* Witness, expected to be violated: maintenance does move the cursor over
\* a tombstoned row that no cancel advance moved it past.
MaintNeverPassesTombstone ==
    ~ (/\ maintPhase = "scanned"
       /\ cursor <= N
       /\ state[cursor] = "tomb"
       /\ cursor \notin pendingCancelAdvance
       /\ maintTarget > cursor)

SpentIsFinal ==
    [][\A p \in Positions :
        state[p] \in {"claimed", "tomb", "gap"} => state'[p] = state[p]]_vars
=============================================================================

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
\* until they commit; a post-commit cursor advance takes it for its own
\* statement. Released compatibility SQL (`awa.delete_job_compat`) writes
\* tombstones without the head, so safety must not rest on it: the
\* unlocked config checks that. The sequence-backed cursor is
\* non-transactional and set_sequence_next only raises it. Enqueue holds
\* the lane reservation lock until it commits, so at most one reservation
\* is in flight and enqueues commit in position order. Retry and rescue
\* re-enqueue a job at a new position at the tail; that is a Reserve here,
\* and the old position stays spent.
\*
\* Maintenance reads committed lane state in one statement, under the head
\* lock taken with SKIP LOCKED, over the window from the cursor to
\* min(first visible row + WindowSize, enqueue head), so a run of rolled-back
\* reservations of any length is crossed once a committed row lies beyond
\* it. Among the window's
\* visible untombstoned rows it checks at most CheckCap for claim evidence.
\* The target is the first checked row that is unclaimed; else one past the
\* last checked row when CheckCap rows were checked; else one past the last
\* visible row; else no move. It then raises the cursor to the target.
\*
\* MaintLocksHead   maintenance takes the head lock (production: TRUE).
\* SkipInvisible    broken: maintenance treats reserved-but-invisible
\*                  positions as spent, i.e. advances to the enqueue head.
\* InOrderEnqueue   enqueues on the lane commit in position order
\*                  (production: TRUE); FALSE drops the reservation lock.
\* AllowCursorLoss  a crash or failover loses committed cursor movement and
\*                  the cursor comes back lower; claims then pass the spent
\*                  rows on their evidence.
\* RepairCrossesGaps a claim whose head row is spent crosses rolled-back
\*                  reservations before it (production: TRUE); FALSE moves
\*                  the cursor only from a spent row at the cursor (#813).
CONSTANTS N, WindowSize, CheckCap, MaintLocksHead, SkipInvisible,
          InOrderEnqueue, AllowCursorLoss, RepairCrossesGaps

Positions == 1..N
Spent == {"claimed", "tomb"}
Visible == {"live", "claimed", "tomb"}
NoPos == 0

VARIABLES state, append, cursor, headLock, claiming, tombing, tombAdvance,
          pendingClaimAdvance, pendingCancelAdvance, maintPhase, maintTarget,
          claimCount, maintPassedTomb
vars == <<state, append, cursor, headLock, claiming, tombing, tombAdvance,
          pendingClaimAdvance, pendingCancelAdvance, maintPhase, maintTarget,
          claimCount, maintPassedTomb>>

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
    /\ maintPassedTomb = FALSE

\* ---- enqueue -------------------------------------------------------------

Reserve ==
    /\ append <= N
    /\ InOrderEnqueue => ~ \E p \in Positions : state[p] = "inflight"
    /\ state' = [state EXCEPT ![append] = "inflight"]
    /\ append' = append + 1
    /\ UNCHANGED <<cursor, headLock, claiming, tombing, tombAdvance,
                   pendingClaimAdvance, pendingCancelAdvance, maintPhase,
                   maintTarget, claimCount, maintPassedTomb>>

EnqueueCommit(p) ==
    /\ state[p] = "inflight"
    /\ state' = [state EXCEPT ![p] = "live"]
    /\ UNCHANGED <<append, cursor, headLock, claiming, tombing, tombAdvance,
                   pendingClaimAdvance, pendingCancelAdvance, maintPhase,
                   maintTarget, claimCount, maintPassedTomb>>

EnqueueAbort(p) ==
    /\ state[p] = "inflight"
    /\ state' = [state EXCEPT ![p] = "gap"]
    /\ UNCHANGED <<append, cursor, headLock, claiming, tombing, tombAdvance,
                   pendingClaimAdvance, pendingCancelAdvance, maintPhase,
                   maintTarget, claimCount, maintPassedTomb>>

\* ---- claim ---------------------------------------------------------------

\* claim_ready_runtime locks the head with SKIP LOCKED and reads the first
\* visible row at or after the cursor: it claims the row when it is live and
\* repairs the cursor when it is spent.
ClaimHead ==
    {p \in Positions :
        /\ p >= cursor
        /\ state[p] \in Visible
        /\ \A q \in Positions : (q >= cursor /\ q < p) =>
                state[q] \notin Visible}

ClaimBegin ==
    /\ headLock = "none"
    /\ \E p \in ClaimHead :
        /\ state[p] = "live"
        /\ headLock' = "claim"
        /\ claiming' = p
    /\ UNCHANGED <<state, append, cursor, tombing, tombAdvance,
                   pendingClaimAdvance, pendingCancelAdvance, maintPhase,
                   maintTarget, claimCount, maintPassedTomb>>

ClaimCommit ==
    /\ headLock = "claim"
    /\ state' = [state EXCEPT ![claiming] = "claimed"]
    /\ claimCount' = [claimCount EXCEPT ![claiming] = @ + 1]
    /\ pendingClaimAdvance' = pendingClaimAdvance \cup {claiming}
    /\ headLock' = "none"
    /\ claiming' = NoPos
    /\ UNCHANGED <<append, cursor, tombing, tombAdvance, pendingCancelAdvance,
                   maintPhase, maintTarget, maintPassedTomb>>

ClaimAbort ==
    /\ headLock = "claim"
    /\ headLock' = "none"
    /\ claiming' = NoPos
    /\ UNCHANGED <<state, append, cursor, tombing, tombAdvance,
                   pendingClaimAdvance, pendingCancelAdvance, maintPhase,
                   maintTarget, claimCount, maintPassedTomb>>

\* When the head row is already spent, the claim function moves the cursor
\* over it, and the rolled-back reservations before it, and returns without
\* claiming. One step per spent row stands for the leading spent run.
ClaimRepair ==
    /\ headLock = "none"
    /\ \E p \in ClaimHead :
        /\ state[p] \in Spent
        /\ RepairCrossesGaps \/ p = cursor
        /\ cursor' = p + 1
    /\ UNCHANGED <<state, append, headLock, claiming, tombing, tombAdvance,
                   pendingClaimAdvance, pendingCancelAdvance, maintPhase,
                   maintTarget, claimCount, maintPassedTomb>>

\* Post-commit advance of a committed claim, evidence-guarded and monotone.
ClaimAdvance(p) ==
    /\ p \in pendingClaimAdvance
    /\ headLock = "none"
    /\ state[p] = "claimed"
    /\ cursor' = Max(cursor, p + 1)
    /\ pendingClaimAdvance' = pendingClaimAdvance \ {p}
    /\ UNCHANGED <<state, append, headLock, claiming, tombing, tombAdvance,
                   pendingCancelAdvance, maintPhase, maintTarget, claimCount, maintPassedTomb>>

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
                   pendingCancelAdvance, maintPhase, maintTarget, claimCount, maintPassedTomb>>

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
                   maintTarget, claimCount, maintPassedTomb>>

TombAbort ==
    /\ headLock = "tomb"
    /\ headLock' = "none"
    /\ tombing' = NoPos
    /\ tombAdvance' = FALSE
    /\ UNCHANGED <<state, append, cursor, claiming, pendingClaimAdvance,
                   pendingCancelAdvance, maintPhase, maintTarget, claimCount, maintPassedTomb>>

\* only_if_current: moves the cursor by one only when it is at the row.
CancelAdvance(p) ==
    /\ p \in pendingCancelAdvance
    /\ headLock = "none"
    /\ cursor' = IF cursor = p THEN p + 1 ELSE cursor
    /\ pendingCancelAdvance' = pendingCancelAdvance \ {p}
    /\ UNCHANGED <<state, append, headLock, claiming, tombing, tombAdvance,
                   pendingClaimAdvance, maintPhase, maintTarget, claimCount, maintPassedTomb>>

\* ---- maintenance ---------------------------------------------------------

SetMin(S) == CHOOSE p \in S : \A q \in S : p <= q
SetMax(S) == CHOOSE p \in S : \A q \in S : p >= q
Min(a, b) == IF a < b THEN a ELSE b

\* Committed rows at or after the cursor, in the window's lane.
VisibleFromCursor ==
    {p \in Positions : p >= cursor /\ p < append /\ state[p] \in Visible}
\* The window starts at the first visible row, past any rolled-back
\* reservations, and is empty when there is none.
WindowEnd ==
    IF VisibleFromCursor = {} THEN cursor
    ELSE Min(SetMin(VisibleFromCursor) + WindowSize, append)
InWindow == {p \in Positions : p >= cursor /\ p < WindowEnd}
SeenVisible ==
    IF SkipInvisible THEN InWindow
    ELSE {p \in InWindow : state[p] \in Visible}
Untombstoned == {p \in SeenVisible : state[p] # "tomb"}
\* The first CheckCap untombstoned visible rows, in lane order.
Checked ==
    {p \in Untombstoned :
        Cardinality({q \in Untombstoned : q < p}) < CheckCap}
CheckedLive == {p \in Checked : state[p] = "live"}

MaintTarget ==
    IF CheckedLive # {} THEN SetMin(CheckedLive)
    ELSE IF Checked # {} /\ Cardinality(Checked) = CheckCap THEN SetMax(Checked) + 1
    ELSE IF SeenVisible # {} THEN SetMax(SeenVisible) + 1
    ELSE cursor

MaintScan ==
    /\ maintPhase = "idle"
    /\ IF MaintLocksHead
       THEN /\ headLock = "none"
            /\ headLock' = "maint"
       ELSE UNCHANGED headLock
    /\ maintTarget' = MaintTarget
    /\ maintPhase' = "scanned"
    /\ UNCHANGED <<state, append, cursor, claiming, tombing, tombAdvance,
                   pendingClaimAdvance, pendingCancelAdvance, claimCount, maintPassedTomb>>

MaintSet ==
    /\ maintPhase = "scanned"
    /\ cursor' = Max(cursor, maintTarget)
    /\ headLock' = IF MaintLocksHead THEN "none" ELSE headLock
    /\ maintPhase' = "idle"
    /\ maintPassedTomb' =
           (maintPassedTomb
            \/ (cursor' > cursor /\ cursor <= N /\ state[cursor] = "tomb"))
    /\ UNCHANGED <<state, append, claiming, tombing, tombAdvance,
                   pendingClaimAdvance, pendingCancelAdvance, maintTarget,
                   claimCount>>

\* A crash or failover loses committed cursor movement.
LoseCursor ==
    /\ AllowCursorLoss
    /\ \E c \in 1..cursor : cursor' = c
    /\ UNCHANGED <<state, append, headLock, claiming, tombing, tombAdvance,
                   pendingClaimAdvance, pendingCancelAdvance, maintPhase,
                   maintTarget, claimCount, maintPassedTomb>>

Next ==
    \/ LoseCursor
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
    /\ maintPassedTomb \in BOOLEAN

\* No row that is, or may yet become, claimable sits behind the cursor.
NoSkippedLive ==
    \A p \in Positions : p < cursor => state[p] \notin {"live", "inflight"}

\* A row is claimed at most once.
NoDoubleClaim == \A p \in Positions : claimCount[p] <= 1

\* A lane with a live row at or after the cursor is never stuck: unless
\* another transaction holds its head, a claim either claims the head row or
\* moves the cursor.
ClaimNeverStalls ==
    (headLock = "none" /\ \E p \in Positions : p >= cursor /\ state[p] = "live")
        => ENABLED (ClaimBegin \/ ClaimRepair)

\* The cursor never passes the enqueue head.
CursorBounded == cursor <= append

\* Outside a lost cursor, the cursor only moves forward.
CursorMonotone == [][cursor' >= cursor]_vars
\* Witness, expected to be violated: a maintenance advance moves the cursor
\* off a tombstoned row.
MaintNeverPassesTombstone == ~ maintPassedTomb

\* Spent and rolled-back positions never become claimable again. This holds
\* by construction here, since no action rewrites such a position; it
\* stands for the Rust fact that no path outside queue prune deletes a
\* ready tombstone or claim-attempt batch, and catches a future action that
\* would.
SpentIsFinal ==
    [][\A p \in Positions :
        state[p] \in {"claimed", "tomb", "gap"} => state'[p] = state[p]]_vars
=============================================================================

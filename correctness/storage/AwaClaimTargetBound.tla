---- MODULE AwaClaimTargetBound ----
EXTENDS Naturals, FiniteSets, TLC

\* One ready lane and the processes around its claim (#885). Positions are
\* lane_seq values 1..N. Each position is its own ready segment, the worst
\* case for the bounded scan, and a segment lives in one of two slots.
\*
\* status[p]: "free"     not enqueued yet
\*            "ready"    committed row with a segment, unclaimed
\*            "claimed"  committed claim evidence
\*            "pruned"   claimed row removed with its slot
\*            "lost"     unclaimed row removed with its slot
\*
\* A claim is separate statements, and enqueues, movers and prune run
\* between any two of them:
\*   Resolve    the first statement reads the segments with its snapshot and
\*              picks the target slot (queue_claim_heads is locked in the
\*              same statement, so the lock can be taken after the snapshot);
\*   Lock       the head lock is granted;
\*   Reresolve  a fresh statement picks the target again (Reresolve only);
\*   Head       finds the first visible row of the target slot at or after
\*              the cursor and moves the cursor TO it, looking at the first
\*              two segments past the cursor for the bound;
\*   Read       claim_ready_slot_N reads up to Limit rows of the target slot
\*              at or after the cursor and the cursor moves past the last.
\* Head and Read each see the state at their own start.
\*
\* Bounded       reads stop below the first segment that is in another slot
\*               among the n + 1 nearest ones past the cursor, where n is the
\*               number of rows the read takes: 1 for Head, Limit for Read
\*               (production: TRUE).
\* Reresolve     the target is picked again after the lock (production: TRUE).
\* Interleave    enqueues may put a position in either slot (#847 carry-
\*               forward); FALSE keeps each slot's positions contiguous.
\* Movers        "none", "whole" (all unclaimed rows of a slot move) or
\*               "prefix" (any prefix of them moves) to the other slot, in
\*               one transaction that also moves their segments.
\*
\* The prune gate is the cursor: a slot is truncated once the cursor is past
\* every position it holds.
CONSTANTS Bounded, Reresolve, Interleave, Movers, Limit

VARIABLES status, where, cursor, next, pc, tgt
vars == <<status, where, cursor, next, pc, tgt>>

N == 4
Positions == 1..N
Slots == {"A", "B"}
Min(S) == CHOOSE x \in S : \A y \in S : x <= y
Max(S) == CHOOSE x \in S : \A y \in S : x >= y
Lowest(S, n) == {p \in S : Cardinality({q \in S : q < p}) < n}

HasSegment(p) == status[p] \in {"ready", "claimed"}
Past == {p \in Positions : p >= cursor /\ HasSegment(p)}
FirstTarget == where[Min(Past)]

\* Smallest first_lane_seq among the n + 1 segments nearest the cursor that
\* are in another slot than t; N + 1 when there is none.
Bound(t, n) ==
    IF ~Bounded THEN N + 1
    ELSE LET other == {p \in Lowest(Past, n + 1) : where[p] # t}
         IN IF other = {} THEN N + 1 ELSE Min(other)
Rows(t, n) == {p \in Positions : p >= cursor /\ status[p] = "ready"
                                 /\ where[p] = t /\ p < Bound(t, n)}

Init == /\ status = [p \in Positions |-> "free"]
        /\ where = [p \in Positions |-> "A"]
        /\ cursor = 1 /\ next = 1
        /\ pc = "idle" /\ tgt = "A"

Enqueue(s) ==
    /\ next <= N
    /\ LET previous == IF next = 1 THEN "A" ELSE where[next - 1]
       IN Interleave \/ s = "B" \/ previous = "A"
    /\ status' = [status EXCEPT ![next] = "ready"]
    /\ where' = [where EXCEPT ![next] = s]
    /\ next' = next + 1
    /\ UNCHANGED <<cursor, pc, tgt>>

Resolve ==
    /\ pc = "idle" /\ Past # {}
    /\ tgt' = FirstTarget /\ pc' = "resolved"
    /\ UNCHANGED <<status, where, cursor, next>>

Lock ==
    /\ pc = "resolved"
    /\ pc' = "locked"
    /\ UNCHANGED <<status, where, cursor, next, tgt>>

Retarget ==
    /\ pc = "locked"
    /\ IF Reresolve
       THEN IF Past = {} THEN /\ pc' = "idle" /\ tgt' = tgt
            ELSE /\ pc' = "retargeted" /\ tgt' = FirstTarget
       ELSE /\ pc' = "retargeted" /\ tgt' = tgt
    /\ UNCHANGED <<status, where, cursor, next>>

Head ==
    /\ pc = "retargeted"
    /\ IF Rows(tgt, 1) = {}
       THEN /\ pc' = "idle" /\ UNCHANGED cursor
       ELSE /\ pc' = "headed" /\ cursor' = Max({cursor, Min(Rows(tgt, 1))})
    /\ UNCHANGED <<status, where, next, tgt>>

Read ==
    /\ pc = "headed"
    /\ pc' = "idle"
    /\ LET taken == Lowest(Rows(tgt, Limit), Limit)
       IN IF taken = {}
          THEN UNCHANGED <<status, cursor>>
          ELSE /\ status' = [p \in Positions |->
                                IF p \in taken THEN "claimed" ELSE status[p]]
               /\ cursor' = Max(taken) + 1
    /\ UNCHANGED <<where, next, tgt>>

Move(s, t) ==
    /\ Movers # "none" /\ s # t
    /\ LET unclaimed == {p \in Positions : p >= cursor /\ status[p] = "ready"
                                           /\ where[p] = s}
       IN /\ unclaimed # {}
          /\ \E moved \in SUBSET unclaimed :
                /\ moved # {}
                /\ IF Movers = "whole" THEN moved = unclaimed
                   ELSE moved = Lowest(unclaimed, Cardinality(moved))
                /\ where' = [p \in Positions |->
                                IF p \in moved THEN t ELSE where[p]]
    /\ UNCHANGED <<status, cursor, next, pc, tgt>>

Prune(s) ==
    /\ \E p \in Positions : HasSegment(p) /\ where[p] = s
    /\ \A p \in Positions : HasSegment(p) /\ where[p] = s => p < cursor
    /\ status' = [p \in Positions |->
                     IF ~HasSegment(p) \/ where[p] # s THEN status[p]
                     ELSE IF status[p] = "ready" THEN "lost" ELSE "pruned"]
    /\ UNCHANGED <<where, cursor, next, pc, tgt>>

Next == \/ \E s \in Slots : Enqueue(s)
        \/ Resolve \/ Lock \/ Retarget \/ Head \/ Read
        \/ \E s, t \in Slots : Move(s, t)
        \/ \E s \in Slots : Prune(s)

\* The cursor never passes an unclaimed row, and prune never removes one.
NoSkippedRow == \A p \in Positions : status[p] = "ready" => cursor <= p
NoLostRow == \A p \in Positions : status[p] # "lost"
TypeOK == /\ status \in [Positions -> {"free", "ready", "claimed", "pruned", "lost"}]
          /\ where \in [Positions -> Slots]
          /\ cursor \in 1..(N + 1) /\ next \in 1..(N + 1)
          /\ pc \in {"idle", "resolved", "locked", "retargeted", "headed"}
          /\ tgt \in Slots
Spec == Init /\ [][Next]_vars
====

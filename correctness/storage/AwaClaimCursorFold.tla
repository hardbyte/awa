---- MODULE AwaClaimCursorFold ----
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS PrefixClaims, NewOnly
VARIABLES state, evidence, legacy, cursor, owner, pending, mode, replay, rotated, pruned
vars == <<state, evidence, legacy, cursor, owner, pending, mode, replay, rotated, pruned>>
Positions == 1..4
Generation(p) == IF p <= 3 THEN 0 ELSE 1
Max(S) == CHOOSE x \in S : \A y \in S : x >= y
Init == /\ state = [p \in Positions |-> IF p = 2 THEN "hole" ELSE "live"]
        /\ evidence = {} /\ legacy = {}
        /\ cursor = 1
        /\ owner = 0 /\ pending = 0 /\ mode = "none"
        /\ replay = {} /\ rotated = FALSE /\ pruned = FALSE
Begin(m, p) ==
    /\ owner = 0 /\ state[p] = "live" /\ p >= cursor
    /\ (Generation(p) = 0 \/ rotated)
    /\ (~PrefixClaims \/ \A q \in Positions : q >= cursor /\ q < p => state[q] # "live")
    /\ owner' = 1 /\ pending' = p /\ mode' = m
    /\ UNCHANGED <<state, evidence, legacy, cursor, replay, rotated, pruned>>
Commit ==
    /\ owner = 1
    /\ state' = [state EXCEPT ![pending] = "claimed"]
    /\ evidence' = IF mode = "legacy" THEN evidence ELSE evidence \cup {pending}
    /\ legacy' = IF mode = "legacy" THEN legacy \cup {pending} ELSE legacy
    /\ replay' = IF mode = "new" THEN replay ELSE replay \cup {pending}
    /\ owner' = 0 /\ pending' = 0 /\ mode' = "none"
    /\ UNCHANGED <<cursor, rotated, pruned>>
Abort == /\ owner = 1
         /\ owner' = 0 /\ pending' = 0 /\ mode' = "none"
         /\ UNCHANGED <<state, evidence, legacy, cursor, replay, rotated, pruned>>
\* New in-claim catch-up reads committed evidence only. The sequence change
\* survives a later claim abort; Begin/Abort above must remain safe after it.
Fold ==
    /\ owner = 0
    /\ \E g \in 0..1 :
        /\ (\A p \in Positions : p >= cursor /\ Generation(p) < g => state[p] # "live")
        /\ LET covered == {p \in evidence : Generation(p) = g /\ p >= cursor}
           IN /\ covered # {}
              /\ cursor' = Max(covered) + 1
    /\ UNCHANGED <<state, evidence, legacy, owner, pending, mode, replay, rotated, pruned>>
\* A committed later row proves the skipped reservation gap is permanent.
\* Advancing to that row preserves it even if the next claim aborts.
GapAdvance(p) ==
    /\ owner = 0 /\ p >= cursor /\ state[p] # "hole"
    /\ \A q \in Positions : q >= cursor /\ q < p => state[q] = "hole"
    /\ cursor' = p
    /\ UNCHANGED <<state, evidence, legacy, owner, pending, mode, replay, rotated, pruned>>
\* Old post-commit receipt and legacy advances share the committed-evidence
\* boundary. Closure/prune can remove evidence; stale replay then stays behind.
OldAdvance(p) ==
    /\ owner = 0 /\ p \in replay
    /\ cursor' = IF p \in evidence \cup legacy THEN Max({cursor, p+1}) ELSE cursor
    /\ replay' = replay \ {p}
    /\ UNCHANGED <<state, evidence, legacy, owner, pending, mode, rotated, pruned>>
Cancel(p) == /\ owner = 0 /\ state[p] = "live"
             /\ state' = [state EXCEPT ![p] = "tombstone"]
             /\ UNCHANGED <<evidence, legacy, cursor, owner, pending, mode, replay, rotated, pruned>>
Close(p) == /\ owner = 0 /\ state[p] = "claimed"
            /\ state' = [state EXCEPT ![p] = "done"]
            /\ legacy' = legacy \ {p}
            /\ UNCHANGED <<evidence, cursor, owner, pending, mode, replay, rotated, pruned>>
Rotate == /\ ~rotated /\ rotated' = TRUE
          /\ UNCHANGED <<state, evidence, legacy, cursor, owner, pending, mode, replay, pruned>>
Prune ==
    /\ rotated /\ ~pruned /\ owner = 0
    /\ \A p \in 1..3 : state[p] \in {"done", "hole", "tombstone"} /\ cursor > p
    /\ pruned' = TRUE /\ evidence' = evidence \ {1,2,3}
    /\ UNCHANGED <<state, legacy, cursor, owner, pending, mode, replay, rotated>>
Next == (\E m \in (IF NewOnly THEN {"new"} ELSE {"old", "new", "legacy"}), p \in Positions : Begin(m,p))
        \/ Commit \/ Abort \/ Fold \/ (\E p \in Positions : GapAdvance(p)) \/ (\E p \in Positions : OldAdvance(p))
        \/ (\E p \in Positions : Cancel(p)) \/ (\E p \in Positions : Close(p)) \/ Rotate \/ Prune
NoSkippedLive == \A p \in Positions : state[p] = "live" => cursor <= p
TypeOK == /\ cursor \in 1..5 /\ evidence \subseteq Positions /\ replay \subseteq Positions
          /\ owner \in {0,1} /\ pending \in 0..4
Spec == Init /\ [][Next]_vars
====

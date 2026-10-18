---- MODULE AwaClaimLaneScheduling ----
EXTENDS Naturals
CONSTANT AlwaysAdvance
VARIABLES highClaimed, lowClaimed, highCursor, lastPoll
vars == <<highClaimed, lowClaimed, highCursor, lastPoll>>

\* Two committed high-priority rows occupy [1,3). An open reservation at
\* position 3 makes the enqueue cursor 4, so that claim is not lane-drained.
HighSegmentNext == 3
Init == /\ highClaimed = FALSE /\ lowClaimed = FALSE
        /\ highCursor = 1 /\ lastPoll = "none"
ClaimHigh == /\ ~highClaimed
             /\ highClaimed' = TRUE
             /\ highCursor' = IF AlwaysAdvance THEN HighSegmentNext ELSE 1
             /\ UNCHANGED <<lowClaimed, lastPoll>>
Poll == /\ highClaimed /\ ~lowClaimed /\ lastPoll = "none"
        /\ IF highCursor < HighSegmentNext
              THEN /\ highCursor' = HighSegmentNext
                   /\ lowClaimed' = FALSE
                   /\ lastPoll' = "empty"
              ELSE /\ highCursor' = highCursor
                   /\ lowClaimed' = TRUE
                   /\ lastPoll' = "low"
        /\ UNCHANGED highClaimed
Next == ClaimHigh \/ Poll
Spec == Init /\ [][Next]_vars
NoEmptyWithEligibleLow == ~(lastPoll = "empty" /\ ~lowClaimed)
TypeOK == /\ highCursor \in 1..3
          /\ highClaimed \in BOOLEAN /\ lowClaimed \in BOOLEAN
          /\ lastPoll \in {"none", "empty", "low"}
====

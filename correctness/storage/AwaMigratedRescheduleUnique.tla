---- MODULE AwaMigratedRescheduleUnique ----
EXTENDS TLC

\* Focused model for the migrated re-schedule unique-claim race in #458/#459.
\*
\* A canonical attempt J1 is running with a unique-states mask that excludes
\* `running` but includes its destination (`scheduled` or `retryable`), so it
\* legitimately holds no claim. A newer duplicate J2 acquires the key before
\* J1 re-schedules. The migrated successor must therefore become cancelled
\* terminal evidence; inserting it as deferred executable work without the
\* claim was the lost-claim bug.

CONSTANT CancelOnConflict

SuccessorStates == {"none", "deferred", "cancelled"}
ClaimHolders == {"none", "j2", "successor"}

VARIABLES canonicalRunning,
          successorState,
          claimHolder

vars == <<canonicalRunning, successorState, claimHolder>>

Init ==
    /\ canonicalRunning = TRUE
    /\ successorState = "none"
    /\ claimHolder \in {"none", "j2"}

\* The transaction consumes the guarded canonical row and records one
\* queue-storage disposition. The fixed path cancels on conflict and leaves
\* J2's claim untouched. The broken witness records the old deferred row even
\* though J2 still owns the required claim.
Reschedule ==
    /\ canonicalRunning
    /\ canonicalRunning' = FALSE
    /\ IF claimHolder = "j2"
          THEN IF CancelOnConflict
                  THEN /\ successorState' = "cancelled"
                       /\ UNCHANGED claimHolder
                  ELSE /\ successorState' = "deferred"
                       /\ UNCHANGED claimHolder
          ELSE /\ successorState' = "deferred"
               /\ claimHolder' = "successor"

Next == Reschedule \/ UNCHANGED vars

Spec == Init /\ [][Next]_vars /\ WF_vars(Reschedule)

TypeOK ==
    /\ canonicalRunning \in BOOLEAN
    /\ successorState \in SuccessorStates
    /\ claimHolder \in ClaimHolders

\* Any claim-bearing deferred successor must own its claim. This is the
\* property violated by the old ON CONFLICT DO NOTHING insertion.
NoUnclaimedExecutable ==
    successorState = "deferred" => claimHolder = "successor"

\* Once J2 has superseded J1, the migrated attempt cannot take J2's claim.
NewerDuplicateWins ==
    claimHolder = "j2" => successorState \in {"none", "cancelled"}

\* The guarded canonical attempt eventually has exactly one durable outcome.
Convergence ==
    <>[](~canonicalRunning /\ successorState \in {"deferred", "cancelled"})

=============================================================================

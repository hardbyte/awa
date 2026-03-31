---- MODULE AwaDispatchClaim ----
EXTENDS TLC, Integers

Workers == {"w1", "w2"}
States == {"available", "running", "completed"}

VARIABLES rowState, attempt, lease, candidates

vars == <<rowState, attempt, lease, candidates>>

Init ==
    /\ rowState = "available"
    /\ attempt = 0
    /\ lease = 0
    /\ candidates = [w \in Workers |-> FALSE]

SelectCandidate(w) ==
    /\ w \in Workers
    /\ rowState = "available"
    /\ ~candidates[w]
    /\ candidates' = [candidates EXCEPT ![w] = TRUE]
    /\ UNCHANGED <<rowState, attempt, lease>>

OldClaim(w) ==
    /\ w \in Workers
    /\ candidates[w]
    /\ rowState' = "running"
    /\ attempt' = attempt + 1
    /\ lease' = lease + 1
    /\ candidates' = [candidates EXCEPT ![w] = FALSE]

NewClaim(w) ==
    /\ w \in Workers
    /\ candidates[w]
    /\ rowState = "available"
    /\ rowState' = "running"
    /\ attempt' = attempt + 1
    /\ lease' = lease + 1
    /\ candidates' = [candidates EXCEPT ![w] = FALSE]

Complete ==
    /\ rowState = "running"
    /\ rowState' = "completed"
    /\ UNCHANGED <<attempt, lease, candidates>>

Stutter ==
    /\ rowState = "completed"
    /\ UNCHANGED vars

NextOld ==
    \/ \E w \in Workers : SelectCandidate(w)
    \/ \E w \in Workers : OldClaim(w)
    \/ Complete
    \/ Stutter

NextNew ==
    \/ \E w \in Workers : SelectCandidate(w)
    \/ \E w \in Workers : NewClaim(w)
    \/ Complete
    \/ Stutter

NoReclaimWhileRunning ==
    rowState = "running" => attempt = 1 /\ lease = 1

TypeOK ==
    /\ rowState \in States
    /\ attempt \in 0..2
    /\ lease \in 0..2
    /\ candidates \in [Workers -> BOOLEAN]

=============================================================================

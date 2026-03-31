---- MODULE AwaDispatchClaim ----
EXTENDS TLC, Integers, FiniteSets

Workers == {"w1", "w2", "w3"}
States == {"available", "running", "completed"}

\* Bound retries to keep state space finite. Four retry rounds exercise
\* stale candidates persisting across multiple claim cycles with three
\* concurrent workers.
MaxAttempts == 4

VARIABLES rowState, attempt, lease, candidates, claimsSinceAvail

vars == <<rowState, attempt, lease, candidates, claimsSinceAvail>>

Init ==
    /\ rowState = "available"
    /\ attempt = 0
    /\ lease = 0
    /\ candidates = [w \in Workers |-> FALSE]
    /\ claimsSinceAvail = 0

SelectCandidate(w) ==
    /\ w \in Workers
    /\ rowState = "available"
    /\ ~candidates[w]
    /\ candidates' = [candidates EXCEPT ![w] = TRUE]
    /\ UNCHANGED <<rowState, attempt, lease, claimsSinceAvail>>

OldClaim(w) ==
    /\ w \in Workers
    /\ candidates[w]
    /\ rowState' = "running"
    /\ attempt' = attempt + 1
    /\ lease' = lease + 1
    /\ candidates' = [candidates EXCEPT ![w] = FALSE]
    /\ claimsSinceAvail' = claimsSinceAvail + 1

NewClaim(w) ==
    /\ w \in Workers
    /\ candidates[w]
    /\ rowState = "available"
    /\ rowState' = "running"
    /\ attempt' = attempt + 1
    /\ lease' = lease + 1
    /\ candidates' = [candidates EXCEPT ![w] = FALSE]
    /\ claimsSinceAvail' = claimsSinceAvail + 1

Complete ==
    /\ rowState = "running"
    /\ rowState' = "completed"
    /\ UNCHANGED <<attempt, lease, candidates, claimsSinceAvail>>

\* Model job retry: completed -> available (e.g. retryable rescue).
\* Bounded by MaxAttempts to keep TLC's state space finite.
Retry ==
    /\ rowState = "completed"
    /\ attempt < MaxAttempts
    /\ rowState' = "available"
    /\ claimsSinceAvail' = 0
    /\ UNCHANGED <<attempt, lease, candidates>>

Stutter ==
    /\ rowState \in {"completed"}
    /\ (~(attempt < MaxAttempts) \/ claimsSinceAvail > 0)
    /\ UNCHANGED vars

NextOld ==
    \/ \E w \in Workers : SelectCandidate(w)
    \/ \E w \in Workers : OldClaim(w)
    \/ Complete
    \/ Retry
    \/ Stutter

NextNew ==
    \/ \E w \in Workers : SelectCandidate(w)
    \/ \E w \in Workers : NewClaim(w)
    \/ Complete
    \/ Retry
    \/ Stutter

\* Safety: a job must not be claimed more than once per available round.
\* This correctly allows attempt > 1 after a legitimate retry cycle
\* (available -> running -> completed -> available -> running).
NoDuplicateClaim == claimsSinceAvail <= 1

TypeOK ==
    /\ rowState \in States
    /\ attempt \in 0..(MaxAttempts + Cardinality(Workers))
    /\ lease \in 0..(MaxAttempts + Cardinality(Workers))
    /\ candidates \in [Workers -> BOOLEAN]
    /\ claimsSinceAvail \in 0..Cardinality(Workers)

=============================================================================

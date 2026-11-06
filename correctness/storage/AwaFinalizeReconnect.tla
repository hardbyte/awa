---- MODULE AwaFinalizeReconnect ----
EXTENDS Naturals, TLC

CONSTANTS GuardLease, AtomicFollowup

Outcomes == {"completed", "retry", "snooze", "cancelled", "failed", "callback"}
States == {"running", "available", "terminal", "waiting_external"}
Disposition(outcome) ==
    IF outcome \in {"retry", "snooze"} THEN "available"
    ELSE IF outcome = "callback" THEN "waiting_external"
    ELSE "terminal"

VARIABLES lease, state, savedOutcome, phase, response, acknowledged,
          commits, followups, newerLeaseChanged
vars == <<lease, state, savedOutcome, phase, response, acknowledged,
          commits, followups, newerLeaseChanged>>
HasFollowup == savedOutcome # "snooze"
ExpectedFollowups == IF HasFollowup THEN commits ELSE 0

Init ==
    /\ lease = 1
    /\ state = "running"
    /\ savedOutcome \in Outcomes
    /\ phase = "ready"
    /\ response = "none"
    /\ acknowledged = "none"
    /\ commits = 0
    /\ followups = 0
    /\ newerLeaseChanged = FALSE

Send ==
    /\ phase \in {"ready", "unknown"}
    /\ phase' = "request"
    /\ UNCHANGED <<lease, state, savedOutcome, response, acknowledged,
                    commits, followups, newerLeaseChanged>>

AbortRequest ==
    /\ phase = "request"
    /\ phase' = "unknown"
    /\ UNCHANGED <<lease, state, savedOutcome, response, acknowledged,
                    commits, followups, newerLeaseChanged>>

CommitFinalize ==
    LET accepted == state = "running" /\ (~GuardLease \/ lease = 1)
    IN
    /\ phase = "request"
    /\ ~accepted \/ commits < 2
    /\ phase' = "response"
    /\ response' = IF accepted THEN "applied" ELSE "stale"
    /\ state' = IF accepted THEN Disposition(savedOutcome) ELSE state
    /\ commits' = IF accepted THEN commits + 1 ELSE commits
    /\ followups' = IF accepted /\ AtomicFollowup /\ HasFollowup
                      THEN followups + 1 ELSE followups
    /\ newerLeaseChanged' = (newerLeaseChanged \/ (accepted /\ lease # 1))
    /\ UNCHANGED <<lease, savedOutcome, acknowledged>>

LoseResponse ==
    /\ phase = "response"
    /\ phase' = "unknown"
    /\ response' = "none"
    /\ UNCHANGED <<lease, state, savedOutcome, acknowledged,
                    commits, followups, newerLeaseChanged>>

Acknowledge ==
    /\ phase = "response"
    /\ acknowledged' = response
    /\ phase' = IF response = "applied" /\ ~AtomicFollowup /\ HasFollowup
                   THEN "followup" ELSE "done"
    /\ response' = "none"
    /\ UNCHANGED <<lease, state, savedOutcome, commits, followups, newerLeaseChanged>>

EmitSeparateFollowup ==
    /\ phase = "followup"
    /\ followups < 2
    /\ followups' = followups + 1
    /\ phase' = "done"
    /\ UNCHANGED <<lease, state, savedOutcome, response, acknowledged,
                    commits, newerLeaseChanged>>

\* Maintenance rescue of a stale, expired or timed-out attempt: back to
\* `available` while attempts remain, `terminal` (failed or dead-lettered)
\* once the rescued attempt was the last (#546). Either way the old lease's
\* finalize must be refused.
Rescue ==
    /\ state \in {"running", "waiting_external"}
    /\ state' \in {"available", "terminal"}
    /\ UNCHANGED <<lease, savedOutcome, phase, response, acknowledged,
                    commits, followups, newerLeaseChanged>>

ClaimNewLease ==
    /\ state = "available"
    /\ lease = 1
    /\ lease' = 2
    /\ state' = "running"
    /\ UNCHANGED <<savedOutcome, phase, response, acknowledged,
                    commits, followups, newerLeaseChanged>>

Next == Send \/ AbortRequest \/ CommitFinalize \/ LoseResponse \/ Acknowledge
        \/ EmitSeparateFollowup \/ Rescue \/ ClaimNewLease
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ lease \in 1..2
    /\ state \in States
    /\ savedOutcome \in Outcomes
    /\ phase \in {"ready", "request", "response", "unknown", "followup", "done"}
    /\ response \in {"none", "applied", "stale"}
    /\ acknowledged \in {"none", "applied", "stale"}
    /\ commits \in 0..2
    /\ followups \in 0..2
    /\ newerLeaseChanged \in BOOLEAN

NoDuplicateFinalize == commits <= 1
NoDuplicateFollowup == followups <= 1
NewerLeaseUnaffected == ~newerLeaseChanged
TransactionalFollowup == followups = ExpectedFollowups
ResolvedFinalizeHasFollowup == phase = "done" => followups = ExpectedFollowups
AppliedAcknowledgementHasCommit == acknowledged = "applied" => commits > 0
=============================================================================

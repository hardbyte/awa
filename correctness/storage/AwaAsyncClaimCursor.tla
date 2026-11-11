---- MODULE AwaAsyncClaimCursor ----
EXTENDS Naturals, FiniteSets, TLC

\* Focused refinement of Claim into acknowledged claim and post-commit
\* sequence movement. Claims and cursor advances can be lost independently
\* after reconnecting to a promoted standby. Local WAL flush and replication
\* preserve commit order; replayed advances must use evidence on this server.
\* Receipt evidence is a covering ready_claim_attempt_batches range; legacy
\* evidence is the exact materialized lease attempt. Both abstract to evidence.
\* Completion/rescue/prune are outside this focused model (covered by the
\* lifecycle model); disappearing evidence conservatively prevents replay.
CONSTANTS GuardEvidence, AllowCrash
Jobs == 1..2
VARIABLES evidence, cursor, durableEvidence, durableCursor,
          standbyEvidence, standbyCursor, pending
vars == <<evidence, cursor, durableEvidence, durableCursor,
          standbyEvidence, standbyCursor, pending>>
Init ==
    /\ evidence = {}
    /\ cursor = 1
    /\ durableEvidence = {}
    /\ durableCursor = 1
    /\ standbyEvidence = {}
    /\ standbyCursor = 1
    /\ pending = {}
Claim ==
    /\ cursor \in Jobs
    /\ cursor \notin evidence
    /\ evidence' = evidence \cup {cursor}
    /\ pending' = pending \cup {cursor}
    /\ UNCHANGED <<cursor, durableEvidence, durableCursor,
                    standbyEvidence, standbyCursor>>
Advance(j) ==
    /\ j \in pending
    /\ ~GuardEvidence \/ j \in evidence
    /\ cursor' = IF cursor > j THEN cursor ELSE j + 1
    /\ pending' = pending \ {j}
    /\ UNCHANGED <<evidence, durableEvidence, durableCursor,
                    standbyEvidence, standbyCursor>>
Repair ==
    /\ cursor \in evidence
    /\ cursor' = cursor + 1
    /\ UNCHANGED <<evidence, durableEvidence, durableCursor,
                    standbyEvidence, standbyCursor, pending>>
Flush ==
    /\ durableEvidence' = evidence
    /\ durableCursor' = cursor
    /\ UNCHANGED <<evidence, cursor, standbyEvidence, standbyCursor, pending>>
Replicate ==
    /\ standbyEvidence' = evidence
    /\ standbyCursor' = cursor
    /\ UNCHANGED <<evidence, cursor, durableEvidence, durableCursor, pending>>
Crash ==
    /\ evidence' = durableEvidence
    /\ cursor' = durableCursor
    /\ UNCHANGED <<durableEvidence, durableCursor,
                    standbyEvidence, standbyCursor, pending>>
Promote ==
    /\ evidence' = standbyEvidence
    /\ cursor' = standbyCursor
    /\ durableEvidence' = standbyEvidence
    /\ durableCursor' = standbyCursor
    /\ UNCHANGED <<standbyEvidence, standbyCursor, pending>>
Next == Claim \/ (\E j \in Jobs : Advance(j)) \/ Repair \/ Flush
        \/ Replicate \/ (AllowCrash /\ Crash) \/ Promote
Spec == Init /\ [][Next]_vars
TypeOK ==
    /\ evidence \subseteq Jobs /\ durableEvidence \subseteq Jobs
    /\ standbyEvidence \subseteq Jobs /\ pending \subseteq Jobs
    /\ cursor \in 1..3 /\ durableCursor \in 1..3 /\ standbyCursor \in 1..3
NoSkippedJob == \A j \in Jobs : j < cursor => j \in evidence
DurableOrder == \A j \in Jobs : j < durableCursor => j \in durableEvidence
ReplicaOrder == \A j \in Jobs : j < standbyCursor => j \in standbyEvidence
=============================================================================

---- MODULE AwaViewTrigger ----
\*
\* Models the INSTEAD OF UPDATE trigger on the awa.jobs UNION ALL view.
\*
\* The trigger implements UPDATE as DELETE + INSERT to handle cross-table
\* state transitions (jobs_hot ↔ scheduled_jobs). Without a version check
\* on the DELETE, two concurrent UPDATEs could both succeed — violating
\* at-most-once semantics for callback resolution.
\*
\* The fix (v006) adds an optimistic concurrency check: the DELETE includes
\* WHERE state = OLD.state AND run_lease = OLD.run_lease AND callback_id
\* IS NOT DISTINCT FROM OLD.callback_id. Combined with PostgreSQL's
\* tuple-level locking (a blocked DELETE sees the old tuple as gone after
\* the holder commits a DELETE+INSERT), this ensures that at most one
\* concurrent UPDATE succeeds for state-changing operations.
\*
\* Callers == concurrent transactions issuing UPDATE through the view.
\* We model two main scenarios:
\*   1. State-changing update (callback resolution): NEW.state ≠ OLD.state
\*   2. Non-state-changing update (heartbeat): NEW.state = OLD.state
\*
\* The model uses explicit phases for each caller (scan → delete → insert)
\* to capture the interleaving points where concurrency can bite.

EXTENDS TLC, Integers, FiniteSets

Callers == {"c1", "c2", "c3"}

\* Row version: the columns checked by the trigger's DELETE WHERE clause.
\* We abstract state + run_lease + callback_id into a single "version"
\* integer. A state-changing update bumps the version; a non-state-changing
\* update (heartbeat) preserves it.
CONSTANTS StateChanging, HeartbeatOnly

UpdateKinds == {StateChanging, HeartbeatOnly}

VARIABLES
    \* Current row version in the base table (0 = absent/deleted).
    rowVersion,
    \* Whether the row physically exists in the base table.
    rowExists,
    \* Per-caller phase: "idle", "scanned", "deleted", "inserted", "done", "rejected"
    phase,
    \* The OLD.version each caller captured during the view scan.
    scannedVersion,
    \* What kind of update each caller is doing.
    updateKind,
    \* Count of successful updates (for invariant checking).
    successCount

vars == <<rowVersion, rowExists, phase, scannedVersion, updateKind, successCount>>

Init ==
    /\ rowVersion = 1
    /\ rowExists = TRUE
    /\ phase = [c \in Callers |-> "idle"]
    /\ scannedVersion = [c \in Callers |-> 0]
    /\ updateKind = [c \in Callers |-> StateChanging]
    /\ successCount = 0

\* ── Caller actions ──────────────────────────────────────────────────

\* Phase 1: Scan the view. The caller reads OLD from the UNION ALL view.
\* Multiple callers can scan concurrently and see the same snapshot.
ScanView(c) ==
    /\ phase[c] = "idle"
    /\ rowExists
    /\ phase' = [phase EXCEPT ![c] = "scanned"]
    /\ scannedVersion' = [scannedVersion EXCEPT ![c] = rowVersion]
    /\ \E k \in UpdateKinds : updateKind' = [updateKind EXCEPT ![c] = k]
    /\ UNCHANGED <<rowVersion, rowExists, successCount>>

\* Phase 2: Trigger's DELETE with version check.
\* Models the v006 fix: DELETE WHERE id = OLD.id AND state = OLD.state ...
\*
\* PostgreSQL behavior when the row was DELETE+INSERT'd by another caller:
\*   - If the other caller's transaction is still in progress, this DELETE
\*     blocks (modeled by requiring rowExists — we don't model blocking,
\*     we model the post-commit observable state)
\*   - If the other caller committed, the original tuple is gone.
\*     Even if a new tuple with the same id exists, the blocked DELETE
\*     was waiting on the OLD tuple and sees it as deleted.
\*
\* We model the "no contention" path (other committed first) by checking
\* version match. The "blocked then released" path always fails (the old
\* tuple is gone), which we model as a version mismatch.
TriggerDelete(c) ==
    /\ phase[c] = "scanned"
    /\ rowExists
    /\ scannedVersion[c] = rowVersion  \* Version check passes
    /\ rowExists' = FALSE
    /\ phase' = [phase EXCEPT ![c] = "deleted"]
    /\ UNCHANGED <<rowVersion, scannedVersion, updateKind, successCount>>

TriggerDeleteFail(c) ==
    /\ phase[c] = "scanned"
    /\ \/ ~rowExists                          \* Row was deleted by another caller
       \/ scannedVersion[c] # rowVersion      \* Version changed (state-changing update)
    /\ phase' = [phase EXCEPT ![c] = "rejected"]
    /\ UNCHANGED <<rowVersion, rowExists, scannedVersion, updateKind, successCount>>

\* Phase 3: Trigger's INSERT (re-creates the row with NEW values).
TriggerInsert(c) ==
    /\ phase[c] = "deleted"
    /\ ~rowExists
    /\ rowExists' = TRUE
    /\ rowVersion' = IF updateKind[c] = StateChanging
                     THEN rowVersion + 1   \* State-changing: bumps version
                     ELSE rowVersion        \* Heartbeat: same version
    /\ phase' = [phase EXCEPT ![c] = "done"]
    /\ successCount' = successCount + 1
    /\ UNCHANGED <<scannedVersion, updateKind>>

\* ── Old (buggy) trigger: DELETE WHERE id = OLD.id only ───────────────
\* No version check — any caller can delete even after another modified it.
OldTriggerDelete(c) ==
    /\ phase[c] = "scanned"
    /\ rowExists
    \* No version check — the v001 bug
    /\ rowExists' = FALSE
    /\ phase' = [phase EXCEPT ![c] = "deleted"]
    /\ UNCHANGED <<rowVersion, scannedVersion, updateKind, successCount>>

OldTriggerDeleteFail(c) ==
    /\ phase[c] = "scanned"
    /\ ~rowExists
    /\ phase' = [phase EXCEPT ![c] = "rejected"]
    /\ UNCHANGED <<rowVersion, rowExists, scannedVersion, updateKind, successCount>>

\* ── Stutter (all callers finished) ──────────────────────────────────
AllDone ==
    /\ \A c \in Callers : phase[c] \in {"done", "rejected", "idle"}
    /\ UNCHANGED vars

\* Fixed trigger (v006): DELETE checks version from OLD.
Next ==
    \/ \E c \in Callers : ScanView(c)
    \/ \E c \in Callers : TriggerDelete(c)
    \/ \E c \in Callers : TriggerDeleteFail(c)
    \/ \E c \in Callers : TriggerInsert(c)
    \/ AllDone

\* Old trigger (v001): DELETE only checks id, not version.
NextOld ==
    \/ \E c \in Callers : ScanView(c)
    \/ \E c \in Callers : OldTriggerDelete(c)
    \/ \E c \in Callers : OldTriggerDeleteFail(c)
    \/ \E c \in Callers : TriggerInsert(c)
    \/ AllDone

\* ── Invariants ──────────────────────────────────────────────────────

\* Among callers that scanned the same row version, at most one
\* state-changing update succeeds. Sequential state changes (different
\* scanned versions) are legitimate — e.g., c1 completes a callback,
\* then c2 scans the new version and rescues it.
AtMostOneStateChangePerVersion ==
    \A v \in 1..10 :
        LET winners == {c \in Callers :
            phase[c] = "done"
            /\ updateKind[c] = StateChanging
            /\ scannedVersion[c] = v}
        IN Cardinality(winners) <= 1

\* The row is never permanently lost (always exists after all callers finish).
RowNeverLost ==
    (\A c \in Callers : phase[c] \in {"done", "rejected", "idle"})
        => rowExists

\* No two callers are simultaneously in the "deleted" phase
\* (row can't be deleted twice without re-insert in between).
NoConcurrentDelete ==
    Cardinality({c \in Callers : phase[c] = "deleted"}) <= 1

TypeOK ==
    /\ rowVersion \in 1..10
    /\ rowExists \in BOOLEAN
    /\ phase \in [Callers -> {"idle", "scanned", "deleted", "inserted", "done", "rejected"}]
    /\ scannedVersion \in [Callers -> 0..10]
    /\ updateKind \in [Callers -> UpdateKinds]
    /\ successCount \in 0..Cardinality(Callers)

=============================================================================

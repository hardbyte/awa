---- MODULE AwaQueuePruneTwoPhase ----
EXTENDS Naturals, TLC
CONSTANTS ReceiptClaims, RecheckAfterUpgrade
VARIABLES prune, claim, cursorPast
vars == <<prune, claim, cursorPast>>

\* One sealed queue slot holding one ready row, one claimer and the prune of
\* that slot. Prune holds SHARE ROW EXCLUSIVE on the slot's children from
\* "frozen" to "proved" and ACCESS EXCLUSIVE in "exclusive".
\*
\* claim: "idle"      the row is unclaimed
\*        "read"      a claim selected the row and holds ACCESS SHARE on the
\*                    ready child until it commits
\*        "written"   the claim inserted its lease or receipt, uncommitted
\*        "committed" the claim is visible to other transactions
\*        "done"      the job completed and its claim is closed
\*
\* A receipt claim inserts an attempt batch into the slot, which needs
\* ROW EXCLUSIVE on a child. A lease claim writes no child of the slot.
PruneFreezesWriters == prune \in {"frozen", "leases", "pending", "proved", "exclusive"}
ClaimReadsSlot == claim \in {"read", "written"}
ClaimWritesSlot == ReceiptClaims /\ claim = "written"

Init == /\ prune = "idle"
        /\ claim = "idle"
        /\ cursorPast = FALSE

ClaimRead == /\ claim = "idle"
             /\ prune \notin {"exclusive", "truncated"}
             /\ claim' = "read"
             /\ UNCHANGED <<prune, cursorPast>>
ClaimWrite == /\ claim = "read"
              /\ ReceiptClaims => ~PruneFreezesWriters
              /\ claim' = "written"
              /\ UNCHANGED <<prune, cursorPast>>
ClaimCommit == /\ claim = "written"
               /\ claim' = "committed"
               /\ UNCHANGED <<prune, cursorPast>>
\* The claim cursor moves only after the claim has committed.
AdvanceCursor == /\ claim \in {"committed", "done"}
                 /\ ~cursorPast
                 /\ cursorPast' = TRUE
                 /\ UNCHANGED <<prune, claim>>
\* Completion writes the slot's terminal children.
Complete == /\ claim = "committed"
            /\ ~PruneFreezesWriters
            /\ prune # "truncated"
            /\ claim' = "done"
            /\ UNCHANGED <<prune, cursorPast>>

\* SHARE ROW EXCLUSIVE waits for transactions that wrote the slot.
PruneFreeze == /\ prune = "idle"
               /\ ~ClaimWritesSlot
               /\ prune' = "frozen"
               /\ UNCHANGED <<claim, cursorPast>>
\* Only a committed lease claim has a visible lease row.
LeaseVisible == ~ReceiptClaims /\ claim = "committed"
LeaseGate == /\ prune = "frozen"
             /\ prune' = IF LeaseVisible THEN "skipped" ELSE "leases"
             /\ UNCHANGED <<claim, cursorPast>>
PendingGate == /\ prune = "leases"
               /\ prune' = IF cursorPast THEN "pending" ELSE "skipped"
               /\ UNCHANGED <<claim, cursorPast>>
\* The count proof: every committed receipt claim of the slot is closed.
ClaimRefProof == /\ prune = "pending"
                 /\ prune' = IF ReceiptClaims /\ claim = "committed"
                             THEN "skipped" ELSE "proved"
                 /\ UNCHANGED <<claim, cursorPast>>
\* ACCESS EXCLUSIVE waits for every reader of the slot.
Upgrade == /\ prune = "proved"
           /\ ~ClaimReadsSlot
           /\ prune' = "exclusive"
           /\ UNCHANGED <<claim, cursorPast>>
Truncate == /\ prune = "exclusive"
            /\ prune' = IF RecheckAfterUpgrade /\ (LeaseVisible \/ ~cursorPast)
                        THEN "skipped" ELSE "truncated"
            /\ UNCHANGED <<claim, cursorPast>>
\* A lock timeout rolls the prune back at any point before the truncate.
Abort == /\ PruneFreezesWriters
         /\ prune' = "skipped"
         /\ UNCHANGED <<claim, cursorPast>>
Retry == /\ prune = "skipped"
         /\ prune' = "idle"
         /\ UNCHANGED <<claim, cursorPast>>

Next == \/ ClaimRead \/ ClaimWrite \/ ClaimCommit \/ AdvanceCursor \/ Complete
        \/ PruneFreeze \/ LeaseGate \/ PendingGate \/ ClaimRefProof
        \/ Upgrade \/ Truncate \/ Abort \/ Retry
Spec == Init /\ [][Next]_vars

\* Truncating the slot never drops an unclaimed row or the row of a claim
\* that is still open.
TruncatedSlotIsFinished == prune = "truncated" => claim = "done"
====

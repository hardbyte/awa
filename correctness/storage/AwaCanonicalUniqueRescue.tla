---- MODULE AwaCanonicalUniqueRescue ----
EXTENDS TLC

\* Focused model for #388: the rescue sweep versus the unique-claim rule, on
\* the canonical engine (`awa.sync_job_unique_claims` trigger) and on queue
\* storage (`QueueStorage::settle_rescued_attempts_tx`).
\*
\* Shape: job J1 is a unique job with a "pending-only" unique-states mask
\* (claims `available`/`retryable`, but NOT `running` or `cancelled`), stuck
\* in `running` with a stale heartbeat. A newer duplicate J2 was legitimately
\* enqueued while J1 held no claim and now owns the key (J2 is modelled only
\* through the claim holder — it stays queued). J3 is an innocent stuck job
\* with no unique key.
\*
\* The claim-trigger rule: a transition entering the mask from outside it
\* must take the claim, and is refused (unique_violation) while another job
\* holds it. Rescue moves `running -> retryable`, which enters J1's mask —
\* refused forever while J2 holds the claim.
\*
\* Engine selects the rescue the model runs:
\*   "batch_only": only the pre-#388 batched sweep, which is enabled only
\*     when every stale row can transition. J1's conflict blocks it
\*     permanently and J3 starves; the Convergence liveness property fails
\*     (the production wedge, kept as an expected counterexample).
\*   "canonical": the canonical engine's per-row fallback. Each row rescues
\*     in its own step; a row whose rescue conflicts is cancelled
\*     (`cancelled` is outside the mask, so no claim is taken).
\*   "queue_storage": queue storage settles every rescued attempt of the
\*     transaction in one pass. It applies all claim changes at once, in key
\*     order, skipping the claims another job holds, and turns each skipped
\*     attempt into a duplicate cancellation in the same transaction; there
\*     is no savepoint or per-row retry. One atomic step resolves every
\*     stale row.
\* Convergence holds for "canonical" and "queue_storage". ClaimConsistency
\* holds in every configuration.

CONSTANT Engine

EngineOK == Engine \in {"batch_only", "canonical", "queue_storage"}

JobStates == {"running_stale", "retryable", "cancelled"}
Holders == {"none", "j1", "j2"}

\* J1's unique-states mask, as the set of claiming states reachable in this
\* model. `running`/`cancelled` are deliberately outside it.
ClaimingStates == {"retryable"}

VARIABLES s1,      \* J1 state
          s3,      \* J3 state (no unique key)
          holder   \* current owner of the unique claim for J1/J2's key

vars == <<s1, s3, holder>>

Init ==
    /\ s1 = "running_stale"
    /\ s3 = "running_stale"
    /\ holder = "j2"

\* Whether J1 may transition into `to` under the trigger rule, and the
\* resulting holder. Entering a claiming state requires the claim to be
\* free or already J1's; leaving the mask releases a held claim.
J1CanEnter(to) ==
    to \in ClaimingStates => holder \in {"none", "j1"}

J1HolderAfter(to) ==
    IF to \in ClaimingStates THEN "j1"
    ELSE IF holder = "j1" THEN "none"
    ELSE holder

\* The pre-#388 batched sweep: one statement rescues every stale row or none.
RescueBatch ==
    /\ Engine = "batch_only"
    /\ s1 = "running_stale" \/ s3 = "running_stale"
    /\ s1 = "running_stale" => J1CanEnter("retryable")
    /\ s1' = IF s1 = "running_stale" THEN "retryable" ELSE s1
    /\ s3' = IF s3 = "running_stale" THEN "retryable" ELSE s3
    /\ holder' = IF s1 = "running_stale" THEN J1HolderAfter("retryable") ELSE holder

\* Per-row fallback (#388 fix): each row rescues independently…
RescueRowJ3 ==
    /\ Engine = "canonical"
    /\ s3 = "running_stale"
    /\ s3' = "retryable"
    /\ UNCHANGED <<s1, holder>>

RescueRowJ1 ==
    /\ Engine = "canonical"
    /\ s1 = "running_stale"
    /\ J1CanEnter("retryable")
    /\ s1' = "retryable"
    /\ holder' = J1HolderAfter("retryable")
    /\ UNCHANGED s3

\* …and a row whose rescue conflicts is cancelled instead — the claim
\* holder (the duplicate that superseded it) wins.
CancelRowJ1 ==
    /\ Engine = "canonical"
    /\ s1 = "running_stale"
    /\ ~J1CanEnter("retryable")
    /\ J1CanEnter("cancelled")
    /\ s1' = "cancelled"
    /\ holder' = J1HolderAfter("cancelled")
    /\ UNCHANGED s3

\* Queue storage: one settle pass over every stale row. A row whose claim
\* another job holds is cancelled as a duplicate; every other row is
\* rescued. The pass commits or aborts as a whole, so it is one step.
SettleSinglePass ==
    /\ Engine = "queue_storage"
    /\ s1 = "running_stale" \/ s3 = "running_stale"
    /\ s3' = IF s3 = "running_stale" THEN "retryable" ELSE s3
    /\ s1' = IF s1 # "running_stale" THEN s1
             ELSE IF J1CanEnter("retryable") THEN "retryable"
             ELSE "cancelled"
    /\ holder' = IF s1 # "running_stale" THEN holder
                 ELSE IF J1CanEnter("retryable") THEN J1HolderAfter("retryable")
                 ELSE J1HolderAfter("cancelled")

Progress ==
    RescueBatch \/ RescueRowJ3 \/ RescueRowJ1 \/ CancelRowJ1 \/ SettleSinglePass

Next == Progress \/ UNCHANGED vars

Spec == Init /\ [][Next]_vars /\ WF_vars(Progress)

TypeOK ==
    /\ EngineOK
    /\ s1 \in JobStates
    /\ s3 \in JobStates
    /\ holder \in Holders

\* The trigger's contract: a held claim always belongs to a job whose state
\* is inside the mask. J2 is permanently queued (claiming) in this model,
\* so `holder = "j2"` is always consistent.
ClaimConsistency ==
    holder = "j1" => s1 \in ClaimingStates

\* Every stuck job eventually resolves: the innocent one is rescued and the
\* conflicted one is rescued or cancelled. This is what the production wedge
\* violated — one poisoned row starved the sweep for every other job.
Convergence ==
    <>[](s3 = "retryable" /\ s1 \in {"retryable", "cancelled"})

=============================================================================

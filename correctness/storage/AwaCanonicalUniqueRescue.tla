---- MODULE AwaCanonicalUniqueRescue ----
EXTENDS TLC

\* Focused model for #388: the canonical rescue sweep versus the unique-claim
\* trigger (`awa.sync_job_unique_claims`).
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
\* With PerRowFallback = FALSE the model has only the batched sweep, which
\* is enabled only when every stale row can transition: J1's conflict blocks
\* it permanently and J3 starves — the Convergence liveness property fails
\* (this is the production wedge, kept as an expected counterexample).
\* With PerRowFallback = TRUE the per-row actions rescue J3 and cancel J1
\* (`cancelled` is outside the mask, so no claim is taken), and Convergence
\* holds. ClaimConsistency holds in both configurations.

CONSTANT PerRowFallback

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
    /\ ~PerRowFallback
    /\ s1 = "running_stale" \/ s3 = "running_stale"
    /\ s1 = "running_stale" => J1CanEnter("retryable")
    /\ s1' = IF s1 = "running_stale" THEN "retryable" ELSE s1
    /\ s3' = IF s3 = "running_stale" THEN "retryable" ELSE s3
    /\ holder' = IF s1 = "running_stale" THEN J1HolderAfter("retryable") ELSE holder

\* Per-row fallback (#388 fix): each row rescues independently…
RescueRowJ3 ==
    /\ PerRowFallback
    /\ s3 = "running_stale"
    /\ s3' = "retryable"
    /\ UNCHANGED <<s1, holder>>

RescueRowJ1 ==
    /\ PerRowFallback
    /\ s1 = "running_stale"
    /\ J1CanEnter("retryable")
    /\ s1' = "retryable"
    /\ holder' = J1HolderAfter("retryable")
    /\ UNCHANGED s3

\* …and a row whose rescue conflicts is cancelled instead — the claim
\* holder (the duplicate that superseded it) wins.
CancelRowJ1 ==
    /\ PerRowFallback
    /\ s1 = "running_stale"
    /\ ~J1CanEnter("retryable")
    /\ J1CanEnter("cancelled")
    /\ s1' = "cancelled"
    /\ holder' = J1HolderAfter("cancelled")
    /\ UNCHANGED s3

Progress == RescueBatch \/ RescueRowJ3 \/ RescueRowJ1 \/ CancelRowJ1

Next == Progress \/ UNCHANGED vars

Spec == Init /\ [][Next]_vars /\ WF_vars(Progress)

TypeOK ==
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

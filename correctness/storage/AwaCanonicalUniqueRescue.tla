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
\*
\* Since #546 a rescued attempt that was the job's last fails in place
\* (`running -> failed`) instead of moving to `retryable`. Whether each stuck
\* job is on its last attempt is chosen at Init. `failed` is outside this
\* mask, so an exhausted J1 never conflicts; a mask that claimed `failed`
\* would conflict the same way `retryable` does and take the same fallback.

CONSTANT PerRowFallback

JobStates == {"running_stale", "retryable", "failed", "cancelled"}
Holders == {"none", "j1", "j2"}

\* J1's unique-states mask, as the set of claiming states reachable in this
\* model. `running`/`cancelled` are deliberately outside it.
ClaimingStates == {"retryable"}

VARIABLES s1,      \* J1 state
          s3,      \* J3 state (no unique key)
          holder,  \* current owner of the unique claim for J1/J2's key
          last1,   \* J1's stuck attempt is its last
          last3    \* J3's stuck attempt is its last

vars == <<s1, s3, holder, last1, last3>>

Init ==
    /\ s1 = "running_stale"
    /\ s3 = "running_stale"
    /\ holder = "j2"
    /\ last1 \in BOOLEAN
    /\ last3 \in BOOLEAN

\* Where rescue sends a stuck job.
RescueTarget(last) == IF last THEN "failed" ELSE "retryable"
T1 == RescueTarget(last1)
T3 == RescueTarget(last3)

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
    /\ s1 = "running_stale" => J1CanEnter(T1)
    /\ s1' = IF s1 = "running_stale" THEN T1 ELSE s1
    /\ s3' = IF s3 = "running_stale" THEN T3 ELSE s3
    /\ holder' = IF s1 = "running_stale" THEN J1HolderAfter(T1) ELSE holder
    /\ UNCHANGED <<last1, last3>>

\* Per-row fallback (#388 fix): each row rescues independently…
RescueRowJ3 ==
    /\ PerRowFallback
    /\ s3 = "running_stale"
    /\ s3' = T3
    /\ UNCHANGED <<s1, holder, last1, last3>>

RescueRowJ1 ==
    /\ PerRowFallback
    /\ s1 = "running_stale"
    /\ J1CanEnter(T1)
    /\ s1' = T1
    /\ holder' = J1HolderAfter(T1)
    /\ UNCHANGED <<s3, last1, last3>>

\* …and a row whose rescue conflicts is cancelled instead — the claim
\* holder (the duplicate that superseded it) wins.
CancelRowJ1 ==
    /\ PerRowFallback
    /\ s1 = "running_stale"
    /\ ~J1CanEnter(T1)
    /\ J1CanEnter("cancelled")
    /\ s1' = "cancelled"
    /\ holder' = J1HolderAfter("cancelled")
    /\ UNCHANGED <<s3, last1, last3>>

Progress == RescueBatch \/ RescueRowJ3 \/ RescueRowJ1 \/ CancelRowJ1

Next == Progress \/ UNCHANGED vars

Spec == Init /\ [][Next]_vars /\ WF_vars(Progress)

TypeOK ==
    /\ s1 \in JobStates
    /\ s3 \in JobStates
    /\ holder \in Holders
    /\ last1 \in BOOLEAN
    /\ last3 \in BOOLEAN

\* A rescue never retries a job whose rescued attempt was its last.
ExhaustedRescueFails ==
    /\ (last1 => s1 # "retryable")
    /\ (last3 => s3 # "retryable")

\* The trigger's contract: a held claim always belongs to a job whose state
\* is inside the mask. J2 is permanently queued (claiming) in this model,
\* so `holder = "j2"` is always consistent.
ClaimConsistency ==
    holder = "j1" => s1 \in ClaimingStates

\* Every stuck job eventually resolves: the innocent one is rescued and the
\* conflicted one is rescued or cancelled. This is what the production wedge
\* violated — one poisoned row starved the sweep for every other job.
Convergence ==
    <>[](s3 = T3 /\ s1 \in {T1, "cancelled"})

=============================================================================

---- MODULE AwaStagedFollowUps ----
\* ADR-046: a handler stages follow-up requests on its context; the runtime
\* inserts them in the attempt's lease-guarded completion transaction and
\* discards them on every other outcome. Two jobs share a small key space so
\* deduplication on a live unique claim is exercised; each attempt stages a
\* nondeterministic key, so a rescued attempt and its replacement can stage
\* different values.
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Jobs, Keys, MaxLease,
          \* Completion matches only the attempt's own run lease (ADR-013).
          GuardLease,
          \* "atomic": staged inserts commit in the completion transaction.
          \* "separate": inserted after the completion is acknowledged.
          \* "early": inserted by the handler before it returns.
          InsertMode,
          \* Finalize retries reuse the staged requests instead of draining
          \* them on the first attempt.
          KeepStagedOnRetry,
          \* A job that staged requests never takes the batched completion
          \* path, which carries no follow-ups.
          StagingSelectsPath

NoKey == "none"
\* Jobs and keys are interchangeable, so configs may check modulo their
\* permutations.
Symmetry == Permutations(Jobs) \union Permutations(Keys)
Leases == 1..MaxLease
Attempts == Jobs \X Leases
Phases == {"idle", "running", "ready", "request", "response", "unknown",
           "followup", "done", "dead"}

VARIABLES state, lease, phase, staged, held, inFlight, sentOnce, response,
          completions, external, rows, witnessed
vars == <<state, lease, phase, staged, held, inFlight, sentOnce, response,
          completions, external, rows, witnessed>>

Live(k) == \E r \in rows : r.key = k

\* Insert with ADR-029 deduplication: a live row holding the key stands in
\* for the new one.
InsertOrDedup(a, k) ==
    IF Live(k) THEN rows ELSE rows \union {[src |-> a, key |-> k]}

Init ==
    /\ state = [j \in Jobs |-> "running"]
    /\ lease = [j \in Jobs |-> 1]
    /\ phase = [a \in Attempts |-> "idle"]
    /\ staged = [a \in Attempts |-> NoKey]
    /\ held = [a \in Attempts |-> NoKey]
    /\ inFlight = [a \in Attempts |-> [key |-> NoKey, direct |-> TRUE]]
    /\ sentOnce = [a \in Attempts |-> FALSE]
    /\ response = [a \in Attempts |-> "none"]
    /\ completions = {}
    /\ external = {}
    /\ rows = {}
    /\ witnessed = {}

Start(j, l) ==
    /\ state[j] = "running" /\ lease[j] = l
    /\ phase[<<j, l>>] = "idle"
    /\ phase' = [phase EXCEPT ![<<j, l>>] = "running"]
    /\ UNCHANGED <<state, lease, staged, held, inFlight, sentOnce, response,
                   completions, external, rows, witnessed>>

\* The handler stages k (or nothing) and returns Completed. In "early" mode
\* it inserts the follow-up itself through the pool instead.
StageAndComplete(j, l, k) ==
    LET a == <<j, l>> IN
    /\ phase[a] = "running"
    /\ phase' = [phase EXCEPT ![a] = "ready"]
    /\ staged' = [staged EXCEPT ![a] = k]
    /\ held' = [held EXCEPT ![a] = IF InsertMode = "early" THEN NoKey ELSE k]
    /\ rows' = IF InsertMode = "early" /\ k # NoKey THEN InsertOrDedup(a, k) ELSE rows
    /\ UNCHANGED <<state, lease, inFlight, sentOnce, response, completions,
                   external, witnessed>>

\* Any other outcome: staged requests are discarded with the attempt. The
\* guarded transition is collapsed into one step; its replay behaviour is
\* AwaFinalizeReconnect's subject.
StageAndFail(j, l, k) ==
    LET a == <<j, l>> IN
    /\ phase[a] = "running"
    /\ staged' = [staged EXCEPT ![a] = k]
    /\ phase' = [phase EXCEPT ![a] = "done"]
    /\ IF state[j] = "running" /\ lease[j] = l
          THEN state' = [state EXCEPT ![j] = IF l < MaxLease THEN "available" ELSE "failed"]
          ELSE UNCHANGED state
    /\ rows' = IF InsertMode = "early" /\ k # NoKey THEN InsertOrDedup(a, k) ELSE rows
    /\ UNCHANGED <<lease, held, inFlight, sentOnce, response, completions,
                   external, witnessed>>

\* The first send may use the batched path; retries are direct. A staged
\* job skips the batched path when StagingSelectsPath holds.
Send(j, l) ==
    LET a == <<j, l>>
        direct == sentOnce[a] \/ (StagingSelectsPath /\ held[a] # NoKey)
    IN
    /\ phase[a] \in {"ready", "unknown"}
    /\ phase' = [phase EXCEPT ![a] = "request"]
    /\ inFlight' = [inFlight EXCEPT ![a] = [key |-> held[a], direct |-> direct]]
    /\ held' = IF KeepStagedOnRetry THEN held ELSE [held EXCEPT ![a] = NoKey]
    /\ sentOnce' = [sentOnce EXCEPT ![a] = TRUE]
    /\ UNCHANGED <<state, lease, staged, response, completions, external, rows, witnessed>>

\* Transport failure before the transaction commits.
AbortRequest(j, l) ==
    LET a == <<j, l>> IN
    /\ phase[a] = "request"
    /\ phase' = [phase EXCEPT ![a] = "unknown"]
    /\ UNCHANGED <<state, lease, staged, held, inFlight, sentOnce, response,
                   completions, external, rows, witnessed>>

\* The guarded completion transaction. Staged inserts ride in it only on the
\* direct path in "atomic" mode; the batched path carries none.
Commit(j, l) ==
    LET a == <<j, l>>
        req == inFlight[a]
        accepted == state[j] = "running" /\ (~GuardLease \/ lease[j] = l)
        inserts == accepted /\ InsertMode = "atomic" /\ req.direct /\ req.key # NoKey
    IN
    /\ phase[a] = "request"
    /\ phase' = [phase EXCEPT ![a] = "response"]
    /\ response' = [response EXCEPT ![a] = IF accepted THEN "applied" ELSE "stale"]
    /\ state' = IF accepted THEN [state EXCEPT ![j] = "completed"] ELSE state
    /\ completions' = IF accepted THEN completions \union {a} ELSE completions
    /\ rows' = IF inserts THEN InsertOrDedup(a, req.key) ELSE rows
    /\ witnessed' = IF inserts THEN witnessed \union {a} ELSE witnessed
    /\ UNCHANGED <<lease, staged, held, inFlight, sentOnce, external>>

\* The commit landed but its acknowledgement did not reach the executor.
LoseResponse(j, l) ==
    LET a == <<j, l>> IN
    /\ phase[a] = "response"
    /\ phase' = [phase EXCEPT ![a] = "unknown"]
    /\ response' = [response EXCEPT ![a] = "none"]
    /\ UNCHANGED <<state, lease, staged, held, inFlight, sentOnce,
                   completions, external, rows, witnessed>>

Acknowledge(j, l) ==
    LET a == <<j, l>> IN
    /\ phase[a] = "response"
    /\ phase' = [phase EXCEPT ![a] =
                    IF response[a] = "applied" /\ InsertMode = "separate"
                       /\ held[a] # NoKey
                    THEN "followup" ELSE "done"]
    /\ response' = [response EXCEPT ![a] = "none"]
    /\ UNCHANGED <<state, lease, staged, held, inFlight, sentOnce,
                   completions, external, rows, witnessed>>

SeparateInsert(j, l) ==
    LET a == <<j, l>> IN
    /\ phase[a] = "followup"
    /\ phase' = [phase EXCEPT ![a] = "done"]
    /\ rows' = InsertOrDedup(a, held[a])
    /\ witnessed' = witnessed \union {a}
    /\ UNCHANGED <<state, lease, staged, held, inFlight, sentOnce, response,
                   completions, external>>

\* A crash or drain abandons the attempt wherever it is.
Crash(j, l) ==
    LET a == <<j, l>> IN
    /\ phase[a] \in {"running", "ready", "unknown", "followup"}
    /\ phase' = [phase EXCEPT ![a] = "dead"]
    /\ UNCHANGED <<state, lease, staged, held, inFlight, sentOnce, response,
                   completions, external, rows, witnessed>>

\* Heartbeat, deadline or callback rescue. It may fire while the attempt is
\* still alive, which is the stale-attempt race the lease guard exists for.
Rescue(j) ==
    /\ state[j] = "running"
    /\ state' = [state EXCEPT ![j] = IF lease[j] < MaxLease THEN "available" ELSE "failed"]
    /\ UNCHANGED <<lease, phase, staged, held, inFlight, sentOnce, response,
                   completions, external, rows, witnessed>>

\* `Client::complete_external` completes a job whose handler is waiting
\* in `ctx.wait_for_callback`, under the same lease, from a process with no
\* handler context. Registered specs may fire there; staged requests cannot,
\* and the waiting handler's own completion then finds no running row.
ExternalComplete(j) ==
    /\ state[j] = "running"
    /\ state' = [state EXCEPT ![j] = "completed"]
    /\ external' = external \union {j}
    /\ UNCHANGED <<lease, phase, staged, held, inFlight, sentOnce, response,
                   completions, rows, witnessed>>

Claim(j) ==
    /\ state[j] = "available"
    /\ lease[j] < MaxLease
    /\ lease' = [lease EXCEPT ![j] = lease[j] + 1]
    /\ state' = [state EXCEPT ![j] = "running"]
    /\ UNCHANGED <<phase, staged, held, inFlight, sentOnce, response,
                   completions, external, rows, witnessed>>

\* The key leaves its unique states (retried away or pruned), so a later
\* duplicate may insert again.
ReleaseKey(r) ==
    /\ r \in rows
    /\ rows' = rows \ {r}
    /\ UNCHANGED <<state, lease, phase, staged, held, inFlight, sentOnce,
                   response, completions, external, witnessed>>

Next ==
    \/ \E j \in Jobs, l \in Leases :
          \/ Start(j, l)
          \/ \E k \in Keys \union {NoKey} : StageAndComplete(j, l, k)
          \* A failing attempt's key matters only when the handler inserts
          \* early; otherwise it is discarded unseen.
          \/ \E k \in IF InsertMode = "early" THEN Keys \union {NoKey} ELSE {NoKey} :
                StageAndFail(j, l, k)
          \/ Send(j, l) \/ AbortRequest(j, l) \/ Commit(j, l)
          \/ LoseResponse(j, l) \/ Acknowledge(j, l) \/ SeparateInsert(j, l)
          \/ Crash(j, l)
    \/ \E j \in Jobs : Rescue(j) \/ Claim(j) \/ ExternalComplete(j)
    \/ \E r \in rows : ReleaseKey(r)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ state \in [Jobs -> {"running", "available", "completed", "failed"}]
    /\ lease \in [Jobs -> Leases]
    /\ phase \in [Attempts -> Phases]
    /\ staged \in [Attempts -> Keys \union {NoKey}]
    /\ held \in [Attempts -> Keys \union {NoKey}]
    /\ response \in [Attempts -> {"none", "applied", "stale"}]
    /\ completions \subseteq Attempts
    /\ external \subseteq Jobs
    /\ witnessed \subseteq Attempts

\* A job completes at most once, by one handler attempt or by an external
\* completion, so at most one attempt's staged requests can be inserted.
AtMostOneCompletion ==
    \A j \in Jobs :
        Cardinality({l \in Leases : <<j, l>> \in completions})
          + (IF j \in external THEN 1 ELSE 0) <= 1

\* Only the attempt holding the current lease completes the job.
CompletionByCurrentLease ==
    \A a \in completions : a[2] = lease[a[1]]

\* Every follow-up row comes from a committed completion of the attempt
\* that staged its key: none from a rescued, failed or superseded attempt.
FollowupTracesToCommit ==
    \A r \in rows : r.src \in completions /\ staged[r.src] = r.key

\* Deduplication: at most one live row per key.
UniqueKeyExclusive ==
    \A r1, r2 \in rows : r1.key = r2.key => r1 = r2

\* Atomic mode: every committed completion that staged a key inserted it or
\* found a live row holding it, in the same step.
CommittedHasFollowupOrDedup ==
    \A a \in completions : staged[a] # NoKey => a \in witnessed

\* Weaker form that any design must meet: once the executor considers the
\* attempt finished, its committed completion has its follow-up or a
\* deduplication witness.
ResolvedCompletionHasFollowup ==
    \A a \in Attempts :
        (phase[a] = "done" /\ a \in completions /\ staged[a] # NoKey)
            => a \in witnessed
=============================================================================

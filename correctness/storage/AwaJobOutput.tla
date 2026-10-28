---- MODULE AwaJobOutput ----
\* ADR-048: a handler records an output on its attempt. The output is sealed
\* when the handler returns and published only by the attempt's own
\* lease-guarded transition: as the job's output when the completion
\* commits, or in the error entry of a failure the attempt's executor
\* records. Rescue and external completion publish nothing from the
\* attempt; an external completion may supply its own output.
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Jobs, Vals, MaxLease,
          \* Transitions match only the attempt's own run lease (ADR-013).
          GuardLease,
          \* "atomic": the output is written by the attempt's transition.
          \* "early": written when the handler records it.
          \* "separate": written after the completion is acknowledged.
          OutputMode,
          \* Finalize retries reuse the sealed output instead of draining it
          \* on the first send.
          KeepOutputOnRetry,
          \* The batched completion path carries the output, as the compact
          \* receipt batch does. FALSE models a path that drops it.
          BatchCarriesOutput

NoVal == "none"
\* Sources are attempts (a job and lease pair); these stand for "none" and
\* an external completion with the same shape, so TLC can compare them.
NoSrc == <<"none">>
Ext == <<"external">>
Symmetry == Permutations(Jobs) \union Permutations(Vals)
Leases == 1..MaxLease
Attempts == Jobs \X Leases
Phases == {"idle", "running", "ready", "request", "response", "unknown",
           "publish", "done", "dead"}

VARIABLES state, lease, phase, sealed, held, inFlight, sentOnce, response,
          completions, failures, output, errs, rescued
vars == <<state, lease, phase, sealed, held, inFlight, sentOnce, response,
          completions, failures, output, errs, rescued>>

NoOutput == [src |-> NoSrc, val |-> NoVal]

Init ==
    /\ state = [j \in Jobs |-> "running"]
    /\ lease = [j \in Jobs |-> 1]
    /\ phase = [a \in Attempts |-> "idle"]
    /\ sealed = [a \in Attempts |-> NoVal]
    /\ held = [a \in Attempts |-> NoVal]
    /\ inFlight = [a \in Attempts |-> [val |-> NoVal, direct |-> TRUE]]
    /\ sentOnce = [a \in Attempts |-> FALSE]
    /\ response = [a \in Attempts |-> "none"]
    /\ completions = {}
    /\ failures = {}
    /\ output = [j \in Jobs |-> NoOutput]
    /\ errs = {}
    /\ rescued = {}

Start(j, l) ==
    /\ state[j] = "running" /\ lease[j] = l
    /\ phase[<<j, l>>] = "idle"
    /\ phase' = [phase EXCEPT ![<<j, l>>] = "running"]
    /\ UNCHANGED <<state, lease, sealed, held, inFlight, sentOnce, response,
                   completions, failures, output, errs, rescued>>

\* The handler records v (or nothing) and returns Completed. In "early"
\* mode recording writes the job's output at once.
RecordAndComplete(j, l, v) ==
    LET a == <<j, l>> IN
    /\ phase[a] = "running"
    /\ phase' = [phase EXCEPT ![a] = "ready"]
    /\ sealed' = [sealed EXCEPT ![a] = v]
    /\ held' = [held EXCEPT ![a] = IF OutputMode = "early" THEN NoVal ELSE v]
    /\ output' = IF OutputMode = "early" /\ v # NoVal
                    THEN [output EXCEPT ![j] = [src |-> a, val |-> v]]
                    ELSE output
    /\ UNCHANGED <<state, lease, inFlight, sentOnce, response, completions,
                   failures, errs, rescued>>

\* The handler records v and fails (an error, panic, cancel or exhaustion).
\* The executor's lease-guarded failure transition appends the attempt's
\* error entry with its output, collapsed into one step; its replay
\* behaviour is AwaFinalizeReconnect's subject. In "early" mode the output
\* was already written to the job when recorded.
RecordAndFail(j, l, v) ==
    LET a == <<j, l>>
        accepted == state[j] = "running" /\ (~GuardLease \/ lease[j] = l)
    IN
    /\ phase[a] = "running"
    /\ phase' = [phase EXCEPT ![a] = "done"]
    /\ sealed' = [sealed EXCEPT ![a] = v]
    /\ state' = IF accepted
                   THEN [state EXCEPT ![j] = IF l < MaxLease THEN "available" ELSE "failed"]
                   ELSE state
    /\ failures' = IF accepted THEN failures \union {a} ELSE failures
    /\ errs' = IF accepted THEN errs \union {[src |-> a, out |-> v]} ELSE errs
    /\ output' = IF OutputMode = "early" /\ v # NoVal
                    THEN [output EXCEPT ![j] = [src |-> a, val |-> v]]
                    ELSE output
    /\ UNCHANGED <<lease, held, inFlight, sentOnce, response, completions, rescued>>

\* The first send may use the batched path; retries are direct.
Send(j, l) ==
    LET a == <<j, l>> IN
    /\ phase[a] \in {"ready", "unknown"}
    /\ phase' = [phase EXCEPT ![a] = "request"]
    /\ inFlight' = [inFlight EXCEPT ![a] = [val |-> held[a], direct |-> sentOnce[a]]]
    /\ held' = IF KeepOutputOnRetry THEN held ELSE [held EXCEPT ![a] = NoVal]
    /\ sentOnce' = [sentOnce EXCEPT ![a] = TRUE]
    /\ UNCHANGED <<state, lease, sealed, response, completions, failures,
                   output, errs, rescued>>

AbortRequest(j, l) ==
    LET a == <<j, l>> IN
    /\ phase[a] = "request"
    /\ phase' = [phase EXCEPT ![a] = "unknown"]
    /\ UNCHANGED <<state, lease, sealed, held, inFlight, sentOnce, response,
                   completions, failures, output, errs, rescued>>

\* The guarded completion transaction publishes the output in "atomic"
\* mode, on the direct path always and on the batched path when it carries
\* outputs.
Commit(j, l) ==
    LET a == <<j, l>>
        req == inFlight[a]
        accepted == state[j] = "running" /\ (~GuardLease \/ lease[j] = l)
        publishes == accepted /\ OutputMode = "atomic" /\ req.val # NoVal
                     /\ (req.direct \/ BatchCarriesOutput)
    IN
    /\ phase[a] = "request"
    /\ phase' = [phase EXCEPT ![a] = "response"]
    /\ response' = [response EXCEPT ![a] = IF accepted THEN "applied" ELSE "stale"]
    /\ state' = IF accepted THEN [state EXCEPT ![j] = "completed"] ELSE state
    /\ completions' = IF accepted THEN completions \union {a} ELSE completions
    /\ output' = IF publishes THEN [output EXCEPT ![j] = [src |-> a, val |-> req.val]]
                 ELSE output
    /\ UNCHANGED <<lease, sealed, held, inFlight, sentOnce, failures, errs, rescued>>

LoseResponse(j, l) ==
    LET a == <<j, l>> IN
    /\ phase[a] = "response"
    /\ phase' = [phase EXCEPT ![a] = "unknown"]
    /\ response' = [response EXCEPT ![a] = "none"]
    /\ UNCHANGED <<state, lease, sealed, held, inFlight, sentOnce,
                   completions, failures, output, errs, rescued>>

Acknowledge(j, l) ==
    LET a == <<j, l>> IN
    /\ phase[a] = "response"
    /\ phase' = [phase EXCEPT ![a] =
                    IF response[a] = "applied" /\ OutputMode = "separate"
                       /\ held[a] # NoVal
                    THEN "publish" ELSE "done"]
    /\ response' = [response EXCEPT ![a] = "none"]
    /\ UNCHANGED <<state, lease, sealed, held, inFlight, sentOnce,
                   completions, failures, output, errs, rescued>>

\* "separate" mode: the output is written in its own step after the
\* completion was acknowledged.
SeparatePublish(j, l) ==
    LET a == <<j, l>> IN
    /\ phase[a] = "publish"
    /\ phase' = [phase EXCEPT ![a] = "done"]
    /\ output' = [output EXCEPT ![j] = [src |-> a, val |-> held[a]]]
    /\ UNCHANGED <<state, lease, sealed, held, inFlight, sentOnce, response,
                   completions, failures, errs, rescued>>

Crash(j, l) ==
    LET a == <<j, l>> IN
    /\ phase[a] \in {"running", "ready", "unknown", "publish"}
    /\ phase' = [phase EXCEPT ![a] = "dead"]
    /\ UNCHANGED <<state, lease, sealed, held, inFlight, sentOnce, response,
                   completions, failures, output, errs, rescued>>

\* Heartbeat or deadline rescue, from a process that never saw the output.
\* It may fire while the attempt is alive.
Rescue(j) ==
    /\ state[j] = "running"
    /\ state' = [state EXCEPT ![j] = IF lease[j] < MaxLease THEN "available" ELSE "failed"]
    /\ errs' = errs \union {[src |-> <<j, lease[j]>>, out |-> NoVal]}
    /\ rescued' = rescued \union {<<j, lease[j]>>}
    /\ UNCHANGED <<lease, phase, sealed, held, inFlight, sentOnce, response,
                   completions, failures, output>>

\* `complete_external_with_output` (or plain `complete_external`, v =
\* NoVal) completes a waiting job from a process with no handler context.
ExternalComplete(j, v) ==
    /\ state[j] = "running"
    /\ state' = [state EXCEPT ![j] = "completed"]
    /\ output' = IF v # NoVal THEN [output EXCEPT ![j] = [src |-> Ext, val |-> v]]
                 ELSE output
    /\ UNCHANGED <<lease, phase, sealed, held, inFlight, sentOnce, response,
                   completions, failures, errs, rescued>>

Claim(j) ==
    /\ state[j] = "available"
    /\ lease[j] < MaxLease
    /\ lease' = [lease EXCEPT ![j] = lease[j] + 1]
    /\ state' = [state EXCEPT ![j] = "running"]
    /\ UNCHANGED <<phase, sealed, held, inFlight, sentOnce, response,
                   completions, failures, output, errs, rescued>>

Next ==
    \/ \E j \in Jobs, l \in Leases :
          \/ Start(j, l)
          \/ \E v \in Vals \union {NoVal} :
                RecordAndComplete(j, l, v) \/ RecordAndFail(j, l, v)
          \/ Send(j, l) \/ AbortRequest(j, l) \/ Commit(j, l)
          \/ LoseResponse(j, l) \/ Acknowledge(j, l) \/ SeparatePublish(j, l)
          \/ Crash(j, l)
    \/ \E j \in Jobs :
          \/ Rescue(j) \/ Claim(j)
          \/ \E v \in Vals \union {NoVal} : ExternalComplete(j, v)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ state \in [Jobs -> {"running", "available", "completed", "failed"}]
    /\ lease \in [Jobs -> Leases]
    /\ phase \in [Attempts -> Phases]
    /\ sealed \in [Attempts -> Vals \union {NoVal}]
    /\ held \in [Attempts -> Vals \union {NoVal}]
    /\ response \in [Attempts -> {"none", "applied", "stale"}]
    /\ completions \subseteq Attempts
    /\ failures \subseteq Attempts

\* Only the attempt holding the current lease completes the job.
CompletionByCurrentLease ==
    \A a \in completions : a[2] = lease[a[1]]

\* A job's output is the sealed output of the attempt whose completion
\* committed, or the value an external completion supplied: never one from a
\* rescued, failed or superseded attempt.
OutputTracesToCommit ==
    \A j \in Jobs :
        output[j] # NoOutput =>
            \/ /\ output[j].src \in completions
               /\ output[j].val = sealed[output[j].src]
            \/ output[j].src = Ext

\* Nothing a rescued attempt sealed is published, as the job's output or in
\* an error entry, even after its job is claimed again.
NoOutputFromRescuedAttempt ==
    /\ \A j \in Jobs : output[j].src \notin rescued
    /\ \A e \in errs : e.out # NoVal => e.src \notin rescued

\* Only a completed job has an output.
OutputOnlyWhenCompleted ==
    \A j \in Jobs : output[j] # NoOutput => state[j] = "completed"

\* An output in an error entry belongs to the attempt whose own failure
\* transition wrote that entry, and is what that attempt sealed.
ErrorOutputBelongsToFailedAttempt ==
    \A e \in errs :
        e.out # NoVal =>
            /\ e.src \in failures
            /\ e.src \notin completions
            /\ sealed[e.src] = e.out

\* "atomic" mode: every committed completion whose attempt sealed an output
\* stored it in the same step.
CommittedCompletionHasOutput ==
    \A a \in completions :
        sealed[a] # NoVal => output[a[1]] = [src |-> a, val |-> sealed[a]]

\* Weaker form that any design must meet: once the executor considers the
\* attempt finished, its committed completion has stored its output.
ResolvedCompletionHasOutput ==
    \A a \in Attempts :
        (phase[a] = "done" /\ a \in completions /\ sealed[a] # NoVal)
            => output[a[1]] = [src |-> a, val |-> sealed[a]]
=============================================================================

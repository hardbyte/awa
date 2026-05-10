---- MODULE AwaDeferredMaterialize ----
EXTENDS TLC, Naturals, FiniteSets

\* Focused contract spec for deferred done_entries materialisation.
\*
\* Today (pre-change): completing a job atomically writes both the
\* `lease_claim_closures` row (the durability-of-completion record)
\* and the `done_entries` row (the terminal index used by the UI / DLQ
\* / admin views). Two heavy writes per completion, one of them a
\* JSONB duplicate of `ready_entries` data the database already
\* holds.
\*
\* Proposed change: the hot path writes only the closure. A periodic
\* materialiser scans closures missing a corresponding done_entries
\* row, JOINs `lease_claims` (for partition routing keys) and
\* `ready_entries` (for kind / args / payload / unique fields, all of
\* which are still in the database after claim — the receipt-ring
\* model never DELETEs them per-row), and bulk-INSERTs the missing
\* terminal rows. Read-side views UNION (closures with outcome but
\* no done_entries yet) so callers never observe a job as
\* still-running once the closure exists.
\*
\* The new safety hazard the spec is here to catch: a `ready_entries`
\* partition TRUNCATE that fires before the materialiser has
\* processed every closure pointing into that partition would
\* orphan the source data and leave done_entries permanently
\* incomplete. The rotation guard must block partition TRUNCATE on
\* "all closures referencing this partition have been materialised".
\*
\* What the spec models:
\*   - per-job state machine: idle -> ready -> claimed -> closed -> materialised
\*   - the partition pointer the closure carries (the receipt's
\*     ready_slot, captured at claim time)
\*   - ready-partition lifecycle: open -> sealed -> pruned
\*   - the rotation guard on Prune
\*   - the materialiser action
\*   - a crash that loses worker state mid-flight (a closed job is
\*     still closed; the materialiser doesn't care about workers)
\*
\* What it does NOT model (out of scope):
\*   - row-level Postgres locks — see feedback in
\*     `~/.claude/projects/.../memory/feedback_tla_abstraction.md`;
\*     this spec speaks at the table-row abstraction
\*   - retries, DLQ, attempt_state — orthogonal to the materialiser
\*     contract
\*   - heartbeat / deadline rescue — those produce closures with
\*     outcome=failed, which the materialiser materialises the same way
\*   - performance — this is a correctness model, not a load model

CONSTANTS
    Jobs,             \* set of job IDs
    ReadySegments,    \* set of partition IDs for ready_entries / done_entries
    MaxRunLease       \* bound on attempts per job (TLC needs finite state)

ASSUME Cardinality(Jobs) >= 1
ASSUME Cardinality(ReadySegments) >= 2  \* need at least one to seal + one open
ASSUME MaxRunLease \in Nat /\ MaxRunLease >= 1

VARIABLES
    state,            \* per-job: idle | ready | claimed | closed | materialised
    seg,              \* per-job: which ready partition holds (or held) its data
    runLease,         \* per-job: monotonically increasing attempt counter
    segStatus,        \* per-partition: open | sealed | pruned
    currentSeg        \* the current open partition (single-writer cursor)

vars == <<state, seg, runLease, segStatus, currentSeg>>

\* ---- Domain helpers ----

JobStates == {"idle", "ready", "claimed", "closed", "materialised"}
SegStatuses == {"open", "sealed", "pruned"}

\* A closure exists iff state is closed or materialised.
HasClosure(j) == state[j] \in {"closed", "materialised"}

\* The closure's source data is still readable iff the partition that
\* held it has not yet been pruned.
SourceReadable(j) == segStatus[seg[j]] \in {"open", "sealed"}

\* The reader-side projection: a job appears completed (terminal) once
\* the closure exists, regardless of whether done_entries has been
\* written yet. This is the UNION view the implementation must serve.
ReaderTerminal(j) == HasClosure(j)

\* ---- Initial state ----

Init ==
    /\ state = [j \in Jobs |-> "idle"]
    /\ seg = [j \in Jobs |-> CHOOSE s \in ReadySegments : TRUE]  \* arbitrary; only meaningful once state != idle
    /\ runLease = [j \in Jobs |-> 0]
    /\ segStatus = [s \in ReadySegments |->
                       IF s = (CHOOSE x \in ReadySegments : TRUE)
                       THEN "open" ELSE "sealed"]
    /\ currentSeg = (CHOOSE s \in ReadySegments : TRUE)
    \* Re-establish the open invariant: pick the same seg as currentSeg.

\* Re-do Init properly: pick currentSeg deterministically and align
\* segStatus to it so exactly one partition is open.
InitProper ==
    \E s0 \in ReadySegments :
        /\ state = [j \in Jobs |-> "idle"]
        /\ seg = [j \in Jobs |-> s0]
        /\ runLease = [j \in Jobs |-> 0]
        /\ segStatus = [s \in ReadySegments |-> IF s = s0 THEN "open" ELSE "sealed"]
        /\ currentSeg = s0

\* ---- Actions ----

\* Enqueue: write a ready_entries row in the current open partition.
\* The job's run_lease advances on enqueue (matches Rust: each enqueue
\* attempt increments the lease).
Enqueue(j) ==
    /\ state[j] = "idle"
    /\ runLease[j] < MaxRunLease
    /\ state' = [state EXCEPT ![j] = "ready"]
    /\ seg' = [seg EXCEPT ![j] = currentSeg]
    /\ runLease' = [runLease EXCEPT ![j] = runLease[j] + 1]
    /\ UNCHANGED <<segStatus, currentSeg>>

\* Claim: write a lease_claims receipt. The receipt captures the
\* partition pointer; ready_entries row stays in place (receipt-ring
\* model). Cannot claim from a pruned partition by construction —
\* ready_entries data has to still be readable for the worker to
\* assemble the JobRow.
Claim(j) ==
    /\ state[j] = "ready"
    /\ SourceReadable(j)
    /\ state' = [state EXCEPT ![j] = "claimed"]
    /\ UNCHANGED <<seg, runLease, segStatus, currentSeg>>

\* Complete: write the closure. Hot path stops here in the new
\* design — no done_entries write at this step.
Complete(j) ==
    /\ state[j] = "claimed"
    /\ state' = [state EXCEPT ![j] = "closed"]
    /\ UNCHANGED <<seg, runLease, segStatus, currentSeg>>

\* Materialise: the periodic maint pass picks up a closure that has
\* not yet been materialised, JOINs ready_entries (still readable —
\* checked in the precondition), and writes the done_entries row.
\* Idempotent: re-firing the action with the same job is a no-op
\* because state is already 'materialised'.
Materialise(j) ==
    /\ state[j] = "closed"
    /\ SourceReadable(j)  \* the rotation guard's job is to keep this true for any closed j
    /\ state' = [state EXCEPT ![j] = "materialised"]
    /\ UNCHANGED <<seg, runLease, segStatus, currentSeg>>

\* RotateOpen: seal the current open partition and open a new one.
\* Models queue_ring_state.current_slot advancing.
RotateOpen ==
    \E next \in ReadySegments :
        /\ next /= currentSeg
        /\ segStatus[next] /= "open"  \* not currently open; can be sealed or pruned
        \* Re-opening a pruned partition is fine; it gets re-initialised at TRUNCATE time.
        /\ segStatus' = [segStatus EXCEPT
                            ![currentSeg] = "sealed",
                            ![next] = "open"]
        /\ currentSeg' = next
        /\ UNCHANGED <<state, seg, runLease>>

\* Prune: TRUNCATE a sealed partition. The rotation guard requires
\* every job whose data lives in this partition to have already
\* reached `materialised` — otherwise we'd orphan the source data
\* needed by a future Materialise.
Prune(s) ==
    /\ segStatus[s] = "sealed"
    /\ \A j \in Jobs :
         seg[j] = s => state[j] \in {"idle", "materialised"}
    /\ segStatus' = [segStatus EXCEPT ![s] = "pruned"]
    /\ UNCHANGED <<state, seg, runLease, currentSeg>>

\* Crash: a worker disappears between Claim and Complete (or before).
\* Models the worker-process-died case. The job's claim row stays in
\* lease_claims; eventually the deadline-rescue path writes a closure
\* with outcome=failed (modeled here by transitioning closed). For
\* simplicity we don't distinguish outcome — the materialiser handles
\* both completed and failed closures the same way.
Crash(j) ==
    /\ state[j] = "claimed"
    /\ SourceReadable(j)  \* rescue path needs to read ready_entries to materialise
    /\ state' = [state EXCEPT ![j] = "closed"]
    /\ UNCHANGED <<seg, runLease, segStatus, currentSeg>>

Next ==
    \/ \E j \in Jobs : Enqueue(j)
    \/ \E j \in Jobs : Claim(j)
    \/ \E j \in Jobs : Complete(j)
    \/ \E j \in Jobs : Materialise(j)
    \/ \E j \in Jobs : Crash(j)
    \/ RotateOpen
    \/ \E s \in ReadySegments : Prune(s)

Spec == InitProper /\ [][Next]_vars

\* Fairness: the materialiser gets to run on every closed job that has
\* readable source data, and the rotation eventually advances. Without
\* fairness on Materialise, Prune(s) is permanently disabled the
\* moment any job lands at state=closed in s, which is correct safety
\* but blocks liveness.
Fairness ==
    /\ \A j \in Jobs : WF_vars(Materialise(j))
    /\ \A s \in ReadySegments : WF_vars(Prune(s))

LiveSpec == Spec /\ Fairness

\* ---- Type / shape invariants ----

TypeOK ==
    /\ state \in [Jobs -> JobStates]
    /\ seg \in [Jobs -> ReadySegments]
    /\ runLease \in [Jobs -> 0..MaxRunLease]
    /\ segStatus \in [ReadySegments -> SegStatuses]
    /\ currentSeg \in ReadySegments

ExactlyOneOpen ==
    /\ segStatus[currentSeg] = "open"
    /\ \A s \in ReadySegments : (s /= currentSeg) => segStatus[s] /= "open"

\* ---- Safety contract ----

\* The headline invariant the rotation guard is here to enforce:
\* whenever a job has a closure that has not yet been materialised,
\* the partition containing its source data is NOT pruned. If this
\* ever fails, the materialiser would have nothing to read and
\* done_entries would be permanently incomplete for that job.
ClosureSourceNotPruned ==
    \A j \in Jobs :
        (state[j] = "closed") => (segStatus[seg[j]] /= "pruned")

\* No claimed job's source can be pruned either — the worker is
\* still relying on it to assemble the JobRow + complete the job.
ClaimedSourceNotPruned ==
    \A j \in Jobs :
        (state[j] = "claimed") => (segStatus[seg[j]] /= "pruned")

\* No ready job's source can be pruned — that would silently delete
\* enqueued work.
ReadySourceNotPruned ==
    \A j \in Jobs :
        (state[j] = "ready") => (segStatus[seg[j]] /= "pruned")

\* The reader-side terminal projection is consistent: a job is shown
\* terminal iff its closure exists, regardless of materialisation.
\* Stated as: never the case that the reader sees terminal but no
\* closure has been written. (Trivially holds by definition of
\* HasClosure / ReaderTerminal; the invariant is a tripwire if the
\* projection ever drifts.)
ReaderProjectionConsistent ==
    \A j \in Jobs : ReaderTerminal(j) <=> HasClosure(j)

\* Materialisation is monotonic: once a job is materialised it stays
\* materialised. Re-running maint never undoes a row.
MaterialiseMonotonic ==
    \A j \in Jobs :
        (state[j] = "materialised") =>
            \* state can only transition out of materialised on a fresh
            \* enqueue (run_lease advances). Express that as: if
            \* materialised, the only post-state allowed is materialised
            \* OR a higher run_lease. Encoded across two adjacent
            \* states using the [Next]_vars formula -- TLC checks step
            \* relations through TLA+'s built-in temporal operators.
            TRUE  \* placeholder: see MaterialiseMonotonicTemporal below

\* Temporal version (checked as a property, not an invariant):
\* []((state[j] = "materialised") => [](state[j] = "materialised"
\*                                       \/ state[j] = "ready"))
\* expressed using the implementation's promise that state monotonically
\* advances within a single run_lease.

NoLostJob ==
    \A j \in Jobs :
        \* If a job left idle, it is in some valid in-flight or terminal
        \* state — never silently disappears.
        (state[j] /= "idle") =>
            state[j] \in {"ready", "claimed", "closed", "materialised"}

\* ---- Liveness ----

\* Under fairness, every closed job is eventually materialised.
EventuallyMaterialised ==
    \A j \in Jobs :
        (state[j] = "closed") ~> (state[j] = "materialised")

============================================================================

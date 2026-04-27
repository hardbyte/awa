---- MODULE AwaDeadTupleContract ----
EXTENDS TLC, FiniteSets, Sequences, Naturals

\* Architectural-contract spec for dead-tuple accumulation.
\*
\* This is NOT a numerical / dynamic model. It does not count tuples,
\* model autovacuum scheduling, or simulate steady-state load. Those
\* belong in the bench harness (see `benchmarks/portable/long_horizon.py`
\* and `benchmarks/baseline.json`). What this spec models is the
\* *architectural contract* that bounds dead tuples in production:
\*
\*   1. Every hot-path table has a declared reclaim kind:
\*      `PartitionTruncate` — rotate + TRUNCATE child. Suitable for
\*        hot append workloads where row count scales with traffic.
\*      `Warm` — autovacuum reclaims dead tuples, but the LIVE row
\*        count is bounded by something that does NOT scale with
\*        traffic (e.g. worker fleet size, queue lane count). Dead
\*        tuples accumulate at mutation rate but the heap stays
\*        small, so autovacuum keeps up cheaply. Requires a declared
\*        `bounded_by` field naming what bounds the row count.
\*      `RowVacuum` — autovacuum reclaims dead tuples on a low-
\*        mutation-rate cold table. Storage-side metadata only.
\*      `BacklogRowVacuum` — autovacuum reclaims dead tuples on an
\*        unpartitioned backlog/retention table whose live row count is
\*        workload- or operator-policy-bounded rather than structurally
\*        constant. This is not a formal bounded-dead-tuple proof; it is
\*        a declaration that the release gate must use runtime bench data.
\*      `AppendOnly` — never reclaimed; archive / audit shape.
\*
\*   2. Hot tables MUST NOT be `RowVacuum`. Either they grow with
\*      traffic and need partition-truncate reclaim, or their row
\*      count is bounded by something traffic-independent and they're
\*      `Warm`. The ADR-019/023 redesign existed because
\*      `open_receipt_claims` violated this contract — flat, hot
\*      mutation, unbounded row count.
\*
\*   3. Every `PartitionTruncate` table must have at least one
\*      transaction in `Transactions` that performs `Truncate` on it.
\*      Otherwise the partitions never get reclaimed and the table
\*      grows monotonically — same failure mode as RowVacuum-on-hot,
\*      different mechanism.
\*
\*   4. Every `Warm` table must declare its `bounded_by` and must be
\*      hot. The `bounded_by` is a free-text label (e.g.
\*      "worker_fleet_size") whose meaning the operator commits to —
\*      breaking that bound makes the table effectively unbounded
\*      and graduates it to `PartitionTruncate`.
\*
\*   5. `AppendOnly` tables forbid Update and Delete. They're for
\*      audit / archive shapes; if a tx mutates one, that's either
\*      the wrong op or the wrong reclaim kind.
\*
\* If a future change adds (or proposes adding) a new SQL site that
\* does INSERT+DELETE on an unpartitioned hot table, the
\* corresponding addition to this spec will fire one of these
\* invariants at TLC time. The historical case in point: the original
\* `open_receipt_claims` design — flat, RowVacuum, hot, unbounded
\* row count — would fire `HotTablesAreNotRowVacuum`. The fix
\* (ADR-023) replaces it with partition-rotated `lease_claims` +
\* `lease_claim_closures`, which satisfies the contract.
\*
\* The spec only catches what is ENCODED in `TableSpec` and
\* `Transactions`. Drift between the spec and the Rust source is the
\* operator's responsibility — when adding a new table, add it here
\* with the correct kind. When adding a new SQL site, add the
\* mutation list here.

\* ---- Reclaim kinds and operations ----

ReclaimKinds == {"PartitionTruncate", "Warm", "BacklogRowVacuum", "RowVacuum", "AppendOnly"}
Hotness == {"hot", "cold"}
Operations == {"Insert", "Update", "Delete", "Truncate"}

\* ---- Per-table contract ----
\*
\* Source of truth for what each table commits to. When a new table
\* is added to `prepare_schema()` in queue_storage.rs, add the
\* corresponding row here. Get the `kind` wrong and the invariants
\* fire — which is the spec doing its job.
\*
\* `hot` means: high mutation rate at production load (e.g. one
\* INSERT per claim, one INSERT per completion). `cold` means: small
\* fixed footprint, mutation rate scales with operator/admin
\* activity, not job throughput. The hot/cold judgment is the
\* dimension that lets the spec catch ADR-023-style accumulation
\* bugs; if you can't tell, lean toward marking `hot` and the spec
\* will force you to think about reclaim.
\*
\* `bounded_by` is meaningful for `Warm` tables only — it names the
\* operational quantity that bounds the table's live row count
\* (e.g. "worker_fleet_size", "queue_lane_count"). For other kinds
\* it must be the empty string. The invariant
\* `WarmTablesDocumentTheirBound` enforces this.

TableSpec == [
    \* ---- Hot path: every row visited per claim/complete ----
    \* All partitioned by their respective ring slot, reclaimed by
    \* TRUNCATE on partition prune.
    ready_entries          |-> [kind |-> "PartitionTruncate", hot |-> "hot", bounded_by |-> ""],
    done_entries           |-> [kind |-> "PartitionTruncate", hot |-> "hot", bounded_by |-> ""],
    leases                 |-> [kind |-> "PartitionTruncate", hot |-> "hot", bounded_by |-> ""],
    lease_claims           |-> [kind |-> "PartitionTruncate", hot |-> "hot", bounded_by |-> ""],
    lease_claim_closures   |-> [kind |-> "PartitionTruncate", hot |-> "hot", bounded_by |-> ""],

    \* ---- Backlog / retention tables ----
    \* deferred_jobs and dlq_entries are unpartitioned in the current Rust
    \* schema. They are reclaimed by row-level DELETE + autovacuum, not by
    \* partition TRUNCATE. Their live rows are bounded by backlog/retention
    \* policy rather than by a traffic-independent constant, so the static
    \* contract cannot prove them bounded; the release gate must verify dead
    \* tuple behavior with the long-horizon bench harness.
    deferred_jobs |->
        [kind |-> "BacklogRowVacuum", hot |-> "hot",
         bounded_by |-> "scheduled_retry_backlog_and_promotion_rate"],
    dlq_entries |->
        [kind |-> "BacklogRowVacuum", hot |-> "hot",
         bounded_by |-> "dlq_retention_policy_and_operator_drain_rate"],

    \* ---- Warm: hot mutation rate, bounded live row count ----
    \* These tables receive UPDATEs at traffic rate but their LIVE
    \* row count is bounded by something traffic-independent, so
    \* autovacuum scans a small heap and keeps up cheaply.
    \* All three have aggressive autovacuum knobs in DDL
    \* (autovacuum_vacuum_scale_factor=0, autovacuum_vacuum_threshold
    \* in the low hundreds, autovacuum_vacuum_cost_limit=2000).
    \*
    \* Risk profile: if a `bounded_by` ever becomes false in
    \* production (e.g. attempt_state retains terminated rows;
    \* queue_claim_heads grows a per-attempt index), the table
    \* effectively grows with traffic and graduates to
    \* `PartitionTruncate`. The bench harness's per-table
    \* dead-tuple peak is the numerical check; a
    \* steadily-growing peak across phases means the bound has
    \* been broken.
    attempt_state |->
        [kind |-> "Warm", hot |-> "hot",
         bounded_by |-> "worker_fleet_size"],
    queue_claim_heads |->
        [kind |-> "Warm", hot |-> "hot",
         bounded_by |-> "queue_lane_count"],
    queue_enqueue_heads |->
        [kind |-> "Warm", hot |-> "hot",
         bounded_by |-> "queue_lane_count"],
    \* Mutation rate is claimer-heartbeat × claimer_slot_count, which
    \* scales with the worker fleet (one heartbeat per active claimer
    \* per heartbeat tick). Live row count is bounded by claimer_slots
    \* per queue, which is itself operator-set. The Warm contract holds
    \* iff updates are HOT — see the INCLUDE-clause index in
    \* `queue_storage.rs:idx_*_queue_claimer_leases_owner` which keeps
    \* expires_at out of the indexed-key set so heartbeat updates stay
    \* HOT-eligible.
    queue_claimer_leases |->
        [kind |-> "Warm", hot |-> "hot",
         bounded_by |-> "claimer_slot_count"],

    \* ---- Cold metadata: small singletons / per-queue rows ----
    \* Mutation rate scales with operator activity (queue creation,
    \* schema changes), not job throughput. RowVacuum is fine.
    queue_lanes            |-> [kind |-> "RowVacuum", hot |-> "cold", bounded_by |-> ""],
    queue_terminal_rollups |-> [kind |-> "RowVacuum", hot |-> "cold", bounded_by |-> ""],
    queue_count_snapshots  |-> [kind |-> "RowVacuum", hot |-> "cold", bounded_by |-> ""],
    queue_claimer_state    |-> [kind |-> "RowVacuum", hot |-> "cold", bounded_by |-> ""],

    \* ---- Ring-state singletons ----
    \* One row per ring; updated O(seconds) by maintenance. Pure
    \* singletons — UPDATE in place forever, autovacuum trivially
    \* keeps up. Listed for completeness.
    queue_ring_state       |-> [kind |-> "RowVacuum", hot |-> "cold", bounded_by |-> ""],
    lease_ring_state       |-> [kind |-> "RowVacuum", hot |-> "cold", bounded_by |-> ""],
    claim_ring_state       |-> [kind |-> "RowVacuum", hot |-> "cold", bounded_by |-> ""],

    \* ---- Ring-slot rows ----
    \* One row per slot, ~handful of slots. Generation is the only
    \* mutating column.
    queue_ring_slots       |-> [kind |-> "RowVacuum", hot |-> "cold", bounded_by |-> ""],
    lease_ring_slots       |-> [kind |-> "RowVacuum", hot |-> "cold", bounded_by |-> ""],
    claim_ring_slots       |-> [kind |-> "RowVacuum", hot |-> "cold", bounded_by |-> ""]
]

Tables == DOMAIN TableSpec

\* ---- Transactions ----
\*
\* Each transaction is a sequence of (op, table) records describing
\* the mutations the tx performs at run time. Plain SELECTs (no
\* mutation) are not modelled — they don't generate dead tuples.
\* `FOR UPDATE` is also not modelled (row lock without UPDATE doesn't
\* mutate the row).
\*
\* The list below covers the current hot path. Add a new transaction
\* here when adding a new SQL site. Update an existing transaction
\* when its mutation profile changes (e.g. a new INSERT added to the
\* claim CTE, or a DELETE removed from the completion path).

Mut(op, tbl) == [op |-> op, table |-> tbl]

\* claim_runtime_batch (receipts mode) — queue_storage.rs claim CTE
ClaimReceiptsTx == <<
    Mut("Insert", "lease_claims"),
    Mut("Update", "queue_claim_heads")
>>

\* claim_runtime_batch (legacy mode) — pre-receipts path
ClaimLegacyTx == <<
    Mut("Insert", "leases"),
    Mut("Insert", "ready_entries"),
    Mut("Update", "queue_claim_heads")
>>

\* complete_runtime_batch (receipts mode) — queue_storage.rs:4677
\* The Phase-4 hot path: append-only on the receipt plane.
CompleteReceiptsTx == <<
    Mut("Insert", "lease_claim_closures"),
    Mut("Insert", "done_entries"),
    Mut("Delete", "attempt_state")
>>

\* complete_runtime_batch (legacy mode) — pre-receipts path
\* The DELETE on `leases` is fine: leases is partitioned, so the
\* dead tuple lives in the lease child until the next prune
\* TRUNCATEs the whole partition.
CompleteLegacyTx == <<
    Mut("Delete", "leases"),
    Mut("Insert", "done_entries"),
    Mut("Delete", "attempt_state")
>>

\* close_receipt_tx — queue_storage.rs:5450
\* Used by cancel_job_tx and the rescue path.
CloseReceiptTx == <<
    Mut("Insert", "lease_claim_closures")
>>

\* rescue_stale_receipt_claims_tx — queue_storage.rs:6672
\* Anti-joins lease_claims against closures + leases; closes
\* stragglers by appending to lease_claim_closures.
RescueReceiptsTx == <<
    Mut("Insert", "lease_claim_closures")
>>

\* ensure_running_leases_from_receipts_tx — queue_storage.rs:6102
\* Materialize a receipt into a real lease row. INSERTs the lease,
\* UPDATEs the claim's materialized_at column.
EnsureRunningTx == <<
    Mut("Insert", "leases"),
    Mut("Update", "lease_claims")
>>

\* cancel_job_tx receipt-only branch — queue_storage.rs:5621
\* Closes the receipt with a `cancelled` outcome, inserts a done row,
\* defensively deletes any orphan lease.
CancelReceiptOnlyTx == <<
    Mut("Insert", "done_entries"),
    Mut("Insert", "lease_claim_closures"),
    Mut("Delete", "leases")
>>

\* cancel_job_tx running-lease branch — queue_storage.rs ~:5581
CancelRunningTx == <<
    Mut("Delete", "leases"),
    Mut("Insert", "done_entries"),
    Mut("Insert", "lease_claim_closures")
>>

\* fail_to_dlq / fail_terminal — queue_storage.rs:8055, 8088
FailToDlqTx == <<
    Mut("Delete", "leases"),
    Mut("Delete", "attempt_state"),
    Mut("Insert", "dlq_entries")
>>

\* retry_after / snooze — queue_storage.rs:7945, 7981
RetryToDeferredTx == <<
    Mut("Delete", "leases"),
    Mut("Insert", "deferred_jobs")
>>

\* promote_due_state — DELETE deferred_jobs, INSERT ready_entries
PromoteDeferredTx == <<
    Mut("Delete", "deferred_jobs"),
    Mut("Insert", "ready_entries")
>>

\* enter_callback_wait — waiting_external is a state on the leases row.
EnterCallbackWaitTx == <<
    Mut("Update", "leases")
>>

\* complete_external(..., resume=true) — resume waiting_external to running
\* and store the callback result on attempt_state.
ResumeWaitingTx == <<
    Mut("Update", "leases"),
    Mut("Insert", "attempt_state"),
    Mut("Update", "attempt_state")
>>

\* complete_external / fail_external — terminal callback resolution.
ResolveExternalTerminalTx == <<
    Mut("Delete", "leases"),
    Mut("Insert", "done_entries")
>>

\* retry_external — delete the waiting lease and park in deferred_jobs.
RetryExternalTx == <<
    Mut("Delete", "leases"),
    Mut("Insert", "deferred_jobs")
>>

\* DLQ admin lifecycle.
MoveFailedToDlqTx == <<
    Mut("Delete", "done_entries"),
    Mut("Insert", "dlq_entries")
>>

RetryFromDlqTx == <<
    Mut("Delete", "dlq_entries"),
    Mut("Insert", "ready_entries")
>>

PurgeDlqTx == <<
    Mut("Delete", "dlq_entries")
>>

\* Heartbeat — UPDATE leases SET heartbeat_at = now()
\* Receipts-mode short jobs don't materialize a lease, so no UPDATE
\* fires for them. Legacy-mode and long-running receipts both go
\* through this path after `ensure_running_leases_from_receipts_tx`
\* has materialized.
HeartbeatTx == <<
    Mut("Update", "leases")
>>

\* Progress flush — UPDATE attempt_state SET progress = ...
ProgressFlushTx == <<
    Mut("Update", "attempt_state")
>>

\* Rotate / prune for each ring family. Rotate updates the ring
\* state singleton; prune TRUNCATEs the partition child.

RotateLeasesTx == << Mut("Update", "lease_ring_state") >>
PruneLeasesTx  == <<
    Mut("Update", "lease_ring_slots"),
    Mut("Truncate", "leases")
>>

RotateReadyTx == << Mut("Update", "queue_ring_state") >>
PruneReadyTx  == <<
    Mut("Update", "queue_ring_slots"),
    Mut("Truncate", "ready_entries"),
    Mut("Truncate", "done_entries")
>>

RotateClaimsTx == << Mut("Update", "claim_ring_state") >>
PruneClaimsTx  == <<
    Mut("Update", "claim_ring_slots"),
    Mut("Truncate", "lease_claims"),
    Mut("Truncate", "lease_claim_closures")
>>

Transactions == {
    ClaimReceiptsTx, ClaimLegacyTx,
    CompleteReceiptsTx, CompleteLegacyTx,
    CloseReceiptTx, RescueReceiptsTx, EnsureRunningTx,
    CancelReceiptOnlyTx, CancelRunningTx,
    FailToDlqTx, RetryToDeferredTx, PromoteDeferredTx,
    EnterCallbackWaitTx, ResumeWaitingTx, ResolveExternalTerminalTx,
    RetryExternalTx, MoveFailedToDlqTx, RetryFromDlqTx, PurgeDlqTx,
    HeartbeatTx, ProgressFlushTx,
    RotateLeasesTx, PruneLeasesTx,
    RotateReadyTx, PruneReadyTx,
    RotateClaimsTx, PruneClaimsTx
}

\* ---- Invariants ----

\* The architectural ground truth from ADR-023: a table that takes
\* hot mutation traffic cannot be reclaimed by row vacuum, because
\* autovacuum cannot keep up with production rates without falling
\* arbitrarily far behind under burst.
\*
\* If a new table is added to `TableSpec` with `hot |-> "hot"` and
\* `kind |-> "RowVacuum"`, this fires. Either the kind is wrong
\* (and the table needs to be partitioned + rotation/prune wired
\* up before merge) or the hotness is wrong (and the bench harness
\* should confirm this empirically).
\*
HotTablesAreNotRowVacuum ==
    \A t \in Tables :
        TableSpec[t].hot = "hot" =>
            TableSpec[t].kind \in {"PartitionTruncate", "Warm", "BacklogRowVacuum"}

\* Every PartitionTruncate table must have at least one transaction
\* registered that performs `Truncate` on it. A PartitionTruncate
\* table without a corresponding prune transaction grows
\* monotonically — same failure mode as RowVacuum-on-hot, different
\* mechanism. If a new partitioned table is added but the
\* `prune_*` maintenance tx isn't wired, this fires.
PartitionTruncateTablesAreReclaimed ==
    \A t \in Tables :
        TableSpec[t].kind = "PartitionTruncate" =>
            \E tx \in Transactions :
                \E i \in 1..Len(tx) :
                    tx[i].op = "Truncate" /\ tx[i].table = t

\* Every Warm table must declare what bounds its live row count and
\* must be marked `hot` (Warm only makes sense for hot mutation
\* rates — at low mutation rate, RowVacuum is the right kind).
\* If `bounded_by` is empty, the operator hasn't committed to a
\* bound and the table's "Warm" claim is unfounded. This invariant
\* fires at parse time and forces the operator to either name the
\* bound or pick a different kind.
WarmTablesDocumentTheirBound ==
    \A t \in Tables :
        TableSpec[t].kind = "Warm" =>
            (TableSpec[t].hot = "hot" /\ TableSpec[t].bounded_by # "")

BacklogRowVacuumTablesDocumentTheirBound ==
    \A t \in Tables :
        TableSpec[t].kind = "BacklogRowVacuum" =>
            (TableSpec[t].hot = "hot" /\ TableSpec[t].bounded_by # "")

\* Conversely, only Warm tables may declare a `bounded_by`. The
\* field is dead weight on every other kind, and a non-empty
\* `bounded_by` on a non-Warm row is a hint that the operator
\* meant Warm.
OnlyBoundedKindsHaveBoundedBy ==
    \A t \in Tables :
        TableSpec[t].kind \notin {"Warm", "BacklogRowVacuum"} =>
            TableSpec[t].bounded_by = ""

\* AppendOnly tables forbid mutation. Insert is fine; Update and
\* Delete create dead tuples that will never be reclaimed (since
\* AppendOnly explicitly opts out of all reclaim mechanisms).
\* Truncate is also forbidden — a truncate-ed AppendOnly table
\* loses audit history.
AppendOnlyAcceptsOnlyInsert ==
    \A tx \in Transactions :
        \A i \in 1..Len(tx) :
            TableSpec[tx[i].table].kind = "AppendOnly" =>
                tx[i].op = "Insert"

\* No transaction may TRUNCATE a RowVacuum or Warm table. TRUNCATE
\* is the PartitionTruncate reclaim mechanism; using it on a row-
\* level-vacuum table either contradicts the table's contract or
\* means the contract is wrong.
VacuumKindTablesNotTruncated ==
    \A tx \in Transactions :
        \A i \in 1..Len(tx) :
            TableSpec[tx[i].table].kind \in {"RowVacuum", "Warm", "BacklogRowVacuum"} =>
                tx[i].op # "Truncate"

\* ---- Spec wiring ----
\*
\* This is a static-shape check: the invariants above are predicates
\* over the constants `TableSpec` and `Transactions`, with no state
\* evolution. We assert each as an `ASSUME`, which TLC checks during
\* semantic analysis and reports as a parse-time error if any
\* fails. That gives the right ergonomics: a violation surfaces as
\* a build-time failure on the spec itself, before TLC even starts
\* the model-check loop. The accompanying `.cfg` lists no
\* invariants because there is no model state to check.

ASSUME HotTablesAreNotRowVacuum
ASSUME PartitionTruncateTablesAreReclaimed
ASSUME WarmTablesDocumentTheirBound
ASSUME BacklogRowVacuumTablesDocumentTheirBound
ASSUME OnlyBoundedKindsHaveBoundedBy
ASSUME AppendOnlyAcceptsOnlyInsert
ASSUME VacuumKindTablesNotTruncated

\* TLC still wants a Spec / Init / Next. Provide the smallest
\* possible ones. The check has happened by the time these are
\* evaluated.
VARIABLE checked
vars == <<checked>>

Init == checked = "yes"
Next == UNCHANGED checked
Spec == Init /\ [][Next]_vars

============================================================

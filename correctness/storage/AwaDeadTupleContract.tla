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
\* (ADR-023) replaces it with partition-rotated `lease_claims` /
\* `lease_claim_batches` plus `lease_claim_closures`, which satisfies
\* the contract.
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
    \* One row per claim batch, storing lane-range evidence.
    ready_claim_attempt_batches
                           |-> [kind |-> "PartitionTruncate", hot |-> "hot", bounded_by |-> ""],
    ready_tombstones       |-> [kind |-> "PartitionTruncate", hot |-> "hot", bounded_by |-> ""],
    \* One row per committed ready lane range, not per completion.
    \* Prune truncates the segment child when it truncates the owning
    \* ready slot.
    ready_segments         |-> [kind |-> "PartitionTruncate", hot |-> "hot",
                                bounded_by |-> ""],
    done_entries           |-> [kind |-> "PartitionTruncate", hot |-> "hot", bounded_by |-> ""],
    receipt_completion_batches
                           |-> [kind |-> "PartitionTruncate", hot |-> "hot", bounded_by |-> ""],
    receipt_completion_tombstones
                           |-> [kind |-> "PartitionTruncate", hot |-> "hot", bounded_by |-> ""],
    queue_terminal_count_deltas
                           |-> [kind |-> "PartitionTruncate", hot |-> "hot", bounded_by |-> ""],
    leases                 |-> [kind |-> "PartitionTruncate", hot |-> "hot", bounded_by |-> ""],
    lease_claims           |-> [kind |-> "PartitionTruncate", hot |-> "hot", bounded_by |-> ""],
    lease_claim_batches    |-> [kind |-> "PartitionTruncate", hot |-> "hot", bounded_by |-> ""],
    lease_claim_closures   |-> [kind |-> "PartitionTruncate", hot |-> "hot", bounded_by |-> ""],
    lease_claim_closure_batches
                           |-> [kind |-> "PartitionTruncate", hot |-> "hot", bounded_by |-> ""],

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
    \* These have aggressive autovacuum knobs in DDL
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
    \* queue_claim_heads stores the sequence-backed claim cursor plus a
    \* nullable ready-segment routing cache. The cache is overwritten on
    \* the same bounded per-lane row and is not claim evidence; it only
    \* avoids scanning all ready-segment partitions while the cursor stays
    \* inside the cached ready slot/generation.
    queue_claim_heads |->
        [kind |-> "Warm", hot |-> "hot",
         bounded_by |-> "queue_lane_count"],
    queue_enqueue_heads |->
        [kind |-> "Warm", hot |-> "hot",
         bounded_by |-> "queue_lane_count"],
    \* Mutation rate is claimer-heartbeat × active_claimer_slot_count.
    \* `QueueConfig.claimers` can add several dispatcher/claimer loops
    \* inside one runtime, but the queue-storage control plane still bounds
    \* live lease rows by the queue's configured claimer slots
    \* (`max(AWA_MAX_CLAIMERS_PER_QUEUE, QueueConfig.claimers)` at runtime).
    \* The Warm contract holds iff updates are HOT — see the INCLUDE-clause
    \* index in `queue_storage.rs:idx_*_queue_claimer_leases_owner` which
    \* keeps expires_at out of the indexed-key set so heartbeat updates stay
    \* HOT-eligible.
    queue_claimer_leases |->
        [kind |-> "Warm", hot |-> "hot",
         bounded_by |-> "claimer_slot_count"],

    \* ---- Cold metadata: small singletons / per-queue rows ----
    \* Mutation rate scales with operator activity (queue creation,
    \* schema changes), not job throughput. RowVacuum is fine.
    queue_lanes            |-> [kind |-> "RowVacuum", hot |-> "cold", bounded_by |-> ""],
    queue_terminal_live_counts
                           |-> [kind |-> "RowVacuum", hot |-> "cold", bounded_by |-> ""],
    queue_terminal_rollups |-> [kind |-> "RowVacuum", hot |-> "cold", bounded_by |-> ""],
    queue_claimer_state    |-> [kind |-> "RowVacuum", hot |-> "cold", bounded_by |-> ""],

    \* ---- Ring-state singletons ----
    \* One row per ring. Since #371 these are genuinely cold config
    \* (slot_count, plus the #290 trust marker on the queue ring): the
    \* per-rotation cursor UPDATE that historically made "cold" a lie
    \* under a pinned MVCC horizon (~3.6k dead versions/hour/ring at a
    \* 1s cadence) moved to the append-only rotation ledgers below.
    \* Writes now happen at operator cadence only.
    queue_ring_state       |-> [kind |-> "RowVacuum", hot |-> "cold", bounded_by |-> ""],
    lease_ring_state       |-> [kind |-> "RowVacuum", hot |-> "cold", bounded_by |-> ""],
    claim_ring_state       |-> [kind |-> "RowVacuum", hot |-> "cold", bounded_by |-> ""],

    \* ---- Ring rotation ledgers (#371) ----
    \* Append-only cursor ledgers: rotation INSERTs one row (the
    \* max-generation row IS the cursor); nothing ever UPDATEs a row.
    \* Reclaim is the maintenance leader's horizon-gated fold
    \* (RingLedgerFoldTx): when no other backend pins the MVCC horizon
    \* it DELETEs generations older than one full ring wrap, so the
    \* dead versions it creates are immediately reclaimable; while a
    \* horizon is pinned the fold stands down (RingLedgerFoldPinnedTx)
    \* and the ledger merely grows by one live row per rotation —
    \* bounded by rotation cadence, not job throughput, hence "cold" +
    \* RowVacuum rather than a hot partition-truncate family.
    queue_ring_rotations   |-> [kind |-> "RowVacuum", hot |-> "cold", bounded_by |-> ""],
    lease_ring_rotations   |-> [kind |-> "RowVacuum", hot |-> "cold", bounded_by |-> ""],
    claim_ring_rotations   |-> [kind |-> "RowVacuum", hot |-> "cold", bounded_by |-> ""],

    \* ---- Terminal-rollup delta landing table (#371) ----
    \* Queue prune appends its pruned terminal counts here instead of
    \* upserting queue_terminal_rollups inside the prune tx (that upsert
    \* left one unreclaimable dead rollup version per prune under a
    \* pinned horizon). Same reclaim shape as the rotation ledgers:
    \* horizon-gated DELETE ... RETURNING fold
    \* (TerminalRollupDeltaFoldTx) at maintenance cadence.
    queue_terminal_rollup_deltas
                           |-> [kind |-> "RowVacuum", hot |-> "cold", bounded_by |-> ""],

    \* ---- Ring-slot rows ----
    \* One row per slot, ~handful of slots. Since #371 the queue/lease
    \* slot rows are pure row-lock targets (per-slot generations are
    \* derived from the rotation ledger cursor); claim slots still carry
    \* the receipt-rescue and deadline-rescue cursors, updated at
    \* maintenance cadence, not per job.
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

\* enqueue_params_batch / enqueue_params_copy immediate-ready path.
\* Ready rows and compact ready segment metadata commit together.
EnqueueReadyTx == <<
    Mut("Insert", "ready_entries"),
    Mut("Insert", "ready_segments")
>>

\* claim_runtime_batch (receipts mode) — queue_storage.rs claim CTE.
\* The queue_claim_heads UPDATE covers both the sequence-backed claim cursor
\* advancement and the ready-segment routing-cache refresh on that same
\* bounded lane row.
ClaimReceiptsTx == <<
    Mut("Insert", "ready_claim_attempt_batches"),
    Mut("Insert", "lease_claims"),
    Mut("Insert", "lease_claim_batches"),
    Mut("Update", "queue_claim_heads")
>>

\* claim_runtime_batch (legacy mode) — pre-receipts path
ClaimLegacyTx == <<
    Mut("Insert", "leases"),
    Mut("Insert", "ready_entries"),
    Mut("Update", "queue_claim_heads")
>>

\* complete_runtime_batch (receipts mode)
\* Successful compact receipt completion appends a compact terminal batch and a
\* compact claim-local closure batch. Compact terminal counts are read from the
\* retained batch ledger directly; terminal-count deltas are reserved for
\* done_entries terminal mutations. Terminal history and claim-closure evidence
\* are separate partition-truncated ledgers.
\* Non-success, cancellation, rescue, and wide terminal paths close by claim
\* shape: a row-local lease_claims claim appends an explicit
\* lease_claim_closures row, while a compact lease_claim_batches claim (which
\* has no lease_claims row to balance an explicit closure against in the prune
\* count proofs) appends a lease_claim_closure_batches row. Both closure
\* children are partition-truncated hot ledgers.
CompleteReceiptsTx == <<
    Mut("Insert", "receipt_completion_batches"),
    Mut("Insert", "lease_claim_closure_batches"),
    Mut("Delete", "attempt_state")
>>

\* complete_runtime_batch (legacy mode) — pre-receipts path
\* The DELETE on `leases` is fine: leases is partitioned, so the
\* dead tuple lives in the lease child until the next prune
\* TRUNCATEs the whole partition.
CompleteLegacyTx == <<
    Mut("Delete", "leases"),
    Mut("Insert", "done_entries"),
    Mut("Insert", "queue_terminal_count_deltas"),
    Mut("Delete", "attempt_state")
>>

\* close_receipt_pairs_tx — the general receipt closer (cancel, rescue,
\* terminal-delete). A row-local lease_claims claim appends an explicit
\* lease_claim_closures row and stamps lease_claims.closed_at; a compact
\* lease_claim_batches claim appends a lease_claim_closure_batches row instead
\* (no lease_claims row exists for it). A given transaction takes whichever arm
\* matches the claim shape; both closure children are partition-truncated hot
\* ledgers.
CloseReceiptTx == <<
    Mut("Insert", "lease_claim_closures"),
    Mut("Insert", "lease_claim_closure_batches"),
    Mut("Update", "lease_claims")
>>

\* rescue_stale_receipt_claims_for_slot_tx — sweeps from the per-slot
\* claim_ring_slots rescue cursor, anti-joins lease_claims and
\* lease_claim_batches against both closure ledgers + leases, and closes stale
\* stragglers: a row-local claim appends to lease_claim_closures and stamps
\* lease_claims.closed_at, a compact batch claim appends to
\* lease_claim_closure_batches. It then advances the tiny control-plane cursor
\* across closed, lease-managed, fresh, or newly rescued rows. Stale open rows
\* not closed by the transaction stop the cursor until a later pass.
RescueReceiptsTx == <<
    Mut("Insert", "lease_claim_closures"),
    Mut("Insert", "lease_claim_closure_batches"),
    Mut("Update", "lease_claims"),
    Mut("Update", "claim_ring_slots")
>>

\* rescue_expired_receipt_deadlines_tx
\* Uses a second per-slot claim_ring_slots cursor ordered by deadline_at.
\* Closed / lease-managed claims advance the cursor, expired open receipt
\* claims are closed by appending deadline_expired closure rows, and the
\* first open future-deadline claim stops advancement. The cursor UPDATE is
\* on the tiny claim_ring_slots control table; no receipt-history row is
\* updated or deleted.
RescueReceiptDeadlinesTx == <<
    Mut("Insert", "lease_claim_closures"),
    Mut("Update", "lease_claims"),
    Mut("Update", "claim_ring_slots")
>>

\* ensure_running_leases_from_receipts_tx
\* Materialize a receipt into a real lease row. INSERTs the lease,
\* UPDATEs the claim's materialized_at column.
EnsureRunningTx == <<
    Mut("Insert", "leases"),
    Mut("Update", "lease_claims")
>>

\* cancel_job_tx receipt-only branch — closes the receipt with a `cancelled`
\* outcome, inserts a done row, defensively deletes any orphan lease. A
\* row-local claim closes into lease_claim_closures (+ lease_claims.closed_at);
\* a compact batch claim closes into lease_claim_closure_batches instead.
CancelReceiptOnlyTx == <<
    Mut("Insert", "done_entries"),
    Mut("Insert", "queue_terminal_count_deltas"),
    Mut("Insert", "lease_claim_closures"),
    Mut("Insert", "lease_claim_closure_batches"),
    Mut("Update", "lease_claims"),
    Mut("Delete", "leases")
>>

\* cancel_job_tx running-lease branch — deletes the materialized lease, writes
\* the done row, and closes the underlying receipt via close_receipt_pairs_tx:
\* a row-local claim into lease_claim_closures, a compact batch claim into
\* lease_claim_closure_batches.
CancelRunningTx == <<
    Mut("Delete", "leases"),
    Mut("Insert", "done_entries"),
    Mut("Insert", "queue_terminal_count_deltas"),
    Mut("Insert", "lease_claim_closures"),
    Mut("Insert", "lease_claim_closure_batches"),
    Mut("Update", "lease_claims")
>>

\* cancel_job_tx ready branch (append-only ready segments)
CancelReadyTx == <<
    Mut("Insert", "ready_tombstones"),
    Mut("Insert", "done_entries"),
    Mut("Insert", "queue_terminal_count_deltas")
>>

\* age_ready_jobs_tx (reprioritize)
ReprioritizeReadyTx == <<
    Mut("Insert", "ready_tombstones"),
    Mut("Insert", "ready_entries"),
    Mut("Insert", "ready_segments")
>>

\* delete_job_compat ready branch. SQL DELETE from the public awa.jobs view
\* hides the ready row by tombstoning the lane; the retained ready row remains
\* append-only until queue prune.
DeleteReadyCompatTx == <<
    Mut("Insert", "ready_tombstones")
>>

\* fail_to_dlq / fail_terminal
FailToDlqTx == <<
    Mut("Delete", "leases"),
    Mut("Delete", "attempt_state"),
    Mut("Insert", "dlq_entries")
>>

\* retry_after / snooze
RetryToDeferredTx == <<
    Mut("Delete", "leases"),
    Mut("Insert", "deferred_jobs")
>>

\* promote_due_state — DELETE deferred_jobs, INSERT ready_entries
PromoteDeferredTx == <<
    Mut("Delete", "deferred_jobs"),
    Mut("Insert", "ready_entries"),
    Mut("Insert", "ready_segments")
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
    Mut("Insert", "lease_claim_closures"),
    Mut("Update", "lease_claims"),
    Mut("Insert", "done_entries"),
    Mut("Insert", "queue_terminal_count_deltas")
>>

\* retry_external — delete the waiting lease and park in deferred_jobs.
RetryExternalTx == <<
    Mut("Delete", "leases"),
    Mut("Insert", "lease_claim_closures"),
    Mut("Update", "lease_claims"),
    Mut("Insert", "deferred_jobs")
>>

\* DLQ admin lifecycle.
MoveFailedToDlqTx == <<
    Mut("Insert", "lease_claim_closures"),
    Mut("Update", "lease_claims"),
    Mut("Delete", "done_entries"),
    Mut("Insert", "queue_terminal_count_deltas"),
    Mut("Insert", "dlq_entries")
>>

DiscardTerminalTx == <<
    Mut("Insert", "lease_claim_closures"),
    Mut("Update", "lease_claims"),
    Mut("Delete", "done_entries"),
    Mut("Insert", "queue_terminal_count_deltas")
>>

DeleteTerminalCompatTx == <<
    Mut("Insert", "lease_claim_closures"),
    Mut("Update", "lease_claims"),
    Mut("Delete", "done_entries"),
    Mut("Insert", "queue_terminal_count_deltas")
>>

DeleteCompactReceiptTerminalCompatTx == <<
    Mut("Insert", "receipt_completion_tombstones")
>>

RetryFromTerminalTx == <<
    Mut("Insert", "lease_claim_closures"),
    Mut("Update", "lease_claims"),
    Mut("Delete", "done_entries"),
    Mut("Insert", "queue_terminal_count_deltas"),
    Mut("Insert", "ready_entries"),
    Mut("Insert", "ready_segments")
>>

RetryFromDlqTx == <<
    Mut("Delete", "dlq_entries"),
    Mut("Insert", "ready_entries"),
    Mut("Insert", "ready_segments")
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

\* Rotate / prune for each ring family (#371). Rotate APPENDS one row
\* to the ring's rotation ledger — the ring-state singleton is no
\* longer written by rotation, and an idle ring (PR A's idle gate)
\* appends nothing at all. Prune TRUNCATEs the partition child.

RotateLeasesTx == << Mut("Insert", "lease_ring_rotations") >>
PruneLeasesTx  == <<
    Mut("Truncate", "leases")
>>

RotateReadyTx == << Mut("Insert", "queue_ring_rotations") >>
\* Queue prune: carried failed rows re-home into the live done segment
\* (with their count-delta evidence), pruned terminal counts append to
\* the rollup delta table (no queue_terminal_rollups upsert here since
\* #371), then the sealed children are truncated and the slot's folded
\* live-counter rows are deleted.
PruneReadyTx  == <<
    Mut("Insert", "done_entries"),
    Mut("Insert", "queue_terminal_count_deltas"),
    Mut("Insert", "queue_terminal_rollup_deltas"),
    Mut("Truncate", "ready_entries"),
    Mut("Truncate", "ready_claim_attempt_batches"),
    Mut("Truncate", "done_entries"),
    Mut("Truncate", "receipt_completion_batches"),
    Mut("Truncate", "receipt_completion_tombstones"),
    Mut("Truncate", "ready_tombstones"),
    Mut("Truncate", "queue_terminal_count_deltas"),
    Mut("Truncate", "ready_segments"),
    Mut("Delete", "queue_terminal_live_counts")
>>

TerminalDeltaRollupTx == <<
    Mut("Update", "queue_terminal_live_counts"),
    Mut("Truncate", "queue_terminal_count_deltas")
>>
\* If another backend genuinely pins the MVCC horizon — a long-lived open
\* snapshot (older than MVCC_HORIZON_PIN_MIN_AGE) or an idle-in-transaction
\* backend holding a write xid — terminal-count rollup returns before mutating
\* the folded counter or truncating the delta child. (The implementation
\* age-gates the open-snapshot case so the transient backend_xmin of ordinary
\* hot-path statements does not count as pinning; the pinned/clear abstraction
\* here is unchanged.) This SELECT-only shape generates no dead tuples; exact
\* reads include the still-pending delta rows.
TerminalDeltaRollupPinnedTx == << >>

\* #371 maintenance folds, both gated on the same clear-horizon check as
\* TerminalDeltaRollupTx so every dead version they create is
\* immediately reclaimable by autovacuum.
\*
\* fold_terminal_rollup_deltas: DELETE ... RETURNING the pending prune
\* deltas and upsert them into the permanent rollups in deterministic
\* (queue, priority) order.
TerminalRollupDeltaFoldTx == <<
    Mut("Delete", "queue_terminal_rollup_deltas"),
    Mut("Update", "queue_terminal_rollups")
>>
\* fold_ring_rotation_ledgers: trim each ledger to one full ring wrap.
RingLedgerFoldTx == <<
    Mut("Delete", "queue_ring_rotations"),
    Mut("Delete", "lease_ring_rotations"),
    Mut("Delete", "claim_ring_rotations")
>>
\* Pinned-horizon stand-down shape shared by both folds: SELECT-only,
\* no dead tuples; readers keep summing the unfolded rows.
HorizonPinnedFoldTx == << >>

\* #371 STAGED-UPGRADE TRANSITION EXEMPTION. During a rolling 0.6 -> 0.7
\* upgrade a schema runs in compat authority: 0.7 rotators (and any live 0.6
\* rotator) advance the cursor by UPDATEing the `{ring}_ring_state` singleton
\* columns and the per-slot `{ring}_ring_slots.generation` — reintroducing
\* exactly the singleton churn #371 removed — while ALSO shadowing the
\* append-only ledger. This is bounded and temporary: it exists only in the
\* pre-flip window, and the one-way flip to ledger authority ends it, after
\* which rotation is a bare ledger append again (RotateReadyTx etc. above).
\* Modelled like TerminalDeltaRollupPinnedTx / HorizonPinnedFoldTx: the
\* singleton/slot rows are RowVacuum + cold, so a low-rate UPDATE during the
\* transition does not violate HotTablesAreNotRowVacuum, and the churn is
\* reclaimable the moment no MVCC horizon is pinned (the same reclaim
\* discipline the pre-#371 fleet relied on). The ledger shadow inserts are
\* append-only.
CompatRotateReadyTx == <<
    Mut("Update", "queue_ring_state"),
    Mut("Update", "queue_ring_slots"),
    Mut("Insert", "queue_ring_rotations")
>>
CompatRotateLeasesTx == <<
    Mut("Update", "lease_ring_state"),
    Mut("Update", "lease_ring_slots"),
    Mut("Insert", "lease_ring_rotations")
>>
CompatRotateClaimsTx == <<
    Mut("Update", "claim_ring_state"),
    Mut("Update", "claim_ring_slots"),
    Mut("Insert", "claim_ring_rotations")
>>
\* The one-way flip poisons the stale compat columns (current_slot = -1) and
\* sets the authority; a bounded one-shot UPDATE on the cold singletons.
FlipRingAuthorityTx == <<
    Mut("Update", "queue_ring_state"),
    Mut("Update", "lease_ring_state"),
    Mut("Update", "claim_ring_state")
>>

RotateClaimsTx == << Mut("Insert", "claim_ring_rotations") >>
PruneClaimsTx  == <<
    Mut("Update", "claim_ring_slots"),
    Mut("Truncate", "lease_claims"),
    Mut("Truncate", "lease_claim_batches"),
    Mut("Truncate", "lease_claim_closures"),
    Mut("Truncate", "lease_claim_closure_batches")
>>

Transactions == {
    EnqueueReadyTx, ClaimReceiptsTx, ClaimLegacyTx,
    CompleteReceiptsTx, CompleteLegacyTx,
    CloseReceiptTx, RescueReceiptsTx, RescueReceiptDeadlinesTx, EnsureRunningTx,
    CancelReceiptOnlyTx, CancelRunningTx, CancelReadyTx, DeleteReadyCompatTx,
    ReprioritizeReadyTx,
    FailToDlqTx, RetryToDeferredTx, PromoteDeferredTx,
    EnterCallbackWaitTx, ResumeWaitingTx, ResolveExternalTerminalTx,
    RetryExternalTx, MoveFailedToDlqTx, DiscardTerminalTx,
    DeleteTerminalCompatTx, DeleteCompactReceiptTerminalCompatTx,
    RetryFromTerminalTx, RetryFromDlqTx, PurgeDlqTx,
    HeartbeatTx, ProgressFlushTx,
    RotateLeasesTx, PruneLeasesTx,
    RotateReadyTx, PruneReadyTx, TerminalDeltaRollupTx, TerminalDeltaRollupPinnedTx,
    TerminalRollupDeltaFoldTx, RingLedgerFoldTx, HorizonPinnedFoldTx,
    RotateClaimsTx, PruneClaimsTx,
    CompatRotateReadyTx, CompatRotateLeasesTx, CompatRotateClaimsTx,
    FlipRingAuthorityTx
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

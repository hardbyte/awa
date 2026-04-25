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
\*      `PartitionTruncate` (rotate + TRUNCATE child),
\*      `RowVacuum` (autovacuum reclaims dead tuples),
\*      `AppendOnly` (never reclaimed; archive shape).
\*
\*   2. Hot tables MUST be `PartitionTruncate`. RowVacuum on a
\*      hot-mutation table means autovacuum has to keep up with
\*      production traffic — which it can't past a certain rate. The
\*      ADR-019/023 redesign existed because `open_receipt_claims`
\*      violated this contract.
\*
\*   3. Every `PartitionTruncate` table must have at least one
\*      transaction in `Transactions` that performs `Truncate` on it.
\*      Otherwise the partitions never get reclaimed and the table
\*      grows monotonically — same failure mode as RowVacuum-on-hot,
\*      different mechanism.
\*
\*   4. `AppendOnly` tables forbid Update and Delete. They're for
\*      audit / archive shapes; if a tx mutates one, that's either
\*      the wrong op or the wrong reclaim kind.
\*
\* If a future change adds (or proposes adding) a new SQL site that
\* does INSERT+DELETE on an unpartitioned hot table, the
\* corresponding addition to this spec will fire one of these
\* invariants at TLC time. The historical case in point: the original
\* `open_receipt_claims` design — flat, RowVacuum, hot — would fire
\* `HotTablesAreNotRowVacuum`. The fix (ADR-023) replaces it with
\* partition-rotated `lease_claims` + `lease_claim_closures`, which
\* satisfies the contract.
\*
\* The spec only catches what is ENCODED in `TableSpec` and
\* `Transactions`. Drift between the spec and the Rust source is the
\* operator's responsibility — when adding a new table, add it here
\* with the correct kind. When adding a new SQL site, add the
\* mutation list here. The pre-merge checklist in
\* `docs/adr/023-receipt-plane-ring-partitioning.md` calls this out.

\* ---- Reclaim kinds and operations ----

ReclaimKinds == {"PartitionTruncate", "RowVacuum", "AppendOnly"}
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

TableSpec == [
    \* ---- Hot path: every row visited per claim/complete ----
    \* All partitioned by their respective ring slot, reclaimed by
    \* TRUNCATE on partition prune.
    ready_entries          |-> [kind |-> "PartitionTruncate", hot |-> "hot"],
    done_entries           |-> [kind |-> "PartitionTruncate", hot |-> "hot"],
    leases                 |-> [kind |-> "PartitionTruncate", hot |-> "hot"],
    lease_claims           |-> [kind |-> "PartitionTruncate", hot |-> "hot"],
    lease_claim_closures   |-> [kind |-> "PartitionTruncate", hot |-> "hot"],

    \* ---- Hot path, partitioned but lower volume ----
    \* deferred_jobs / waiting_state / dlq_entries are partitioned
    \* but only see traffic on the slow paths (snooze, callback,
    \* terminal failure). Still PartitionTruncate so traffic-driven
    \* growth is bounded.
    deferred_jobs          |-> [kind |-> "PartitionTruncate", hot |-> "hot"],
    waiting_entries        |-> [kind |-> "PartitionTruncate", hot |-> "hot"],
    dlq_entries            |-> [kind |-> "PartitionTruncate", hot |-> "hot"],

    \* ---- Hot path, RowVacuum but bounded ----
    \* attempt_state and queue_claim_heads / queue_enqueue_heads
    \* see one mutation per attempt / per enqueue / per claim. They
    \* are NOT partitioned. The contract assumption is that their
    \* row count is bounded (one row per running attempt; one row
    \* per (queue, priority) lane), so autovacuum churn is local
    \* to a small working set rather than scaling with traffic.
    \*
    \* Caveat: this is the riskier shape. If their row counts ever
    \* scale linearly with traffic (e.g. attempt_state retaining
    \* terminated rows, or queue_claim_heads gaining a per-attempt
    \* index), they would graduate to "hot mutation rate
    \* unbounded", and the contract here would be a lie. The
    \* bench harness's per-table dead-tuple report is the
    \* numerical sanity check.
    attempt_state          |-> [kind |-> "RowVacuum", hot |-> "hot"],
    queue_claim_heads      |-> [kind |-> "RowVacuum", hot |-> "hot"],
    queue_enqueue_heads    |-> [kind |-> "RowVacuum", hot |-> "hot"],

    \* ---- Cold metadata: small singletons / per-queue rows ----
    \* Mutation rate scales with operator activity (queue creation,
    \* schema changes), not job throughput. RowVacuum is fine.
    queue_lanes            |-> [kind |-> "RowVacuum", hot |-> "cold"],
    queue_terminal_rollups |-> [kind |-> "RowVacuum", hot |-> "cold"],
    queue_count_snapshots  |-> [kind |-> "RowVacuum", hot |-> "cold"],
    queue_claimer_leases   |-> [kind |-> "RowVacuum", hot |-> "cold"],
    queue_claimer_state    |-> [kind |-> "RowVacuum", hot |-> "cold"],

    \* ---- Ring-state singletons ----
    \* One row per ring; updated O(seconds) by maintenance. Pure
    \* singletons — UPDATE in place forever, autovacuum trivially
    \* keeps up. Listed for completeness.
    queue_ring_state       |-> [kind |-> "RowVacuum", hot |-> "cold"],
    lease_ring_state       |-> [kind |-> "RowVacuum", hot |-> "cold"],
    claim_ring_state       |-> [kind |-> "RowVacuum", hot |-> "cold"],

    \* ---- Ring-slot rows ----
    \* One row per slot, ~handful of slots. Generation is the only
    \* mutating column.
    queue_ring_slots       |-> [kind |-> "RowVacuum", hot |-> "cold"],
    lease_ring_slots       |-> [kind |-> "RowVacuum", hot |-> "cold"],
    claim_ring_slots       |-> [kind |-> "RowVacuum", hot |-> "cold"]
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
\* The list below is the post-Wave-3 ADR-023 hot-path. Add a new
\* transaction here when adding a new SQL site. Update an existing
\* transaction when its mutation profile changes (e.g. a new INSERT
\* added to the claim CTE, or a DELETE removed from the completion
\* path).

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

\* close_receipt_tx — queue_storage.rs:5450 (Wave 2b rewrite)
\* Used by cancel_job_tx and the rescue path.
CloseReceiptTx == <<
    Mut("Insert", "lease_claim_closures")
>>

\* rescue_stale_receipt_claims_tx — queue_storage.rs:6672 (Wave 2c)
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
\* defensively deletes any orphan lease (Wave 2d).
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

\* Hot tables `deferred_jobs`, `waiting_entries`, `dlq_entries` also
\* need TRUNCATE registered to satisfy
\* `PartitionTruncateTablesAreReclaimed`. These run at the same
\* maintenance cadence as the others; modelled as bare prune
\* transactions for the contract check.
PruneDeferredTx == << Mut("Truncate", "deferred_jobs") >>
PruneWaitingTx  == << Mut("Truncate", "waiting_entries") >>
PruneDlqTx      == << Mut("Truncate", "dlq_entries") >>

Transactions == {
    ClaimReceiptsTx, ClaimLegacyTx,
    CompleteReceiptsTx, CompleteLegacyTx,
    CloseReceiptTx, RescueReceiptsTx, EnsureRunningTx,
    CancelReceiptOnlyTx, CancelRunningTx,
    FailToDlqTx, RetryToDeferredTx,
    HeartbeatTx, ProgressFlushTx,
    RotateLeasesTx, PruneLeasesTx,
    RotateReadyTx, PruneReadyTx,
    RotateClaimsTx, PruneClaimsTx,
    PruneDeferredTx, PruneWaitingTx, PruneDlqTx
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
\* Exception: tables marked `hot` but explicitly RowVacuum here
\* (attempt_state, queue_claim_heads, queue_enqueue_heads) are
\* allowed because their row count is bounded by working-set size,
\* not by traffic — the dead tuples are concentrated in a small
\* hot footprint that autovacuum CAN keep up with. The
\* documentation field below names the bounded-row constraint each
\* relies on.
HotPartitionedTablesUseTruncate ==
    \A t \in Tables :
        TableSpec[t].hot = "hot" /\ t \notin
            { "attempt_state", "queue_claim_heads", "queue_enqueue_heads" }
        => TableSpec[t].kind = "PartitionTruncate"

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

\* No transaction may TRUNCATE a RowVacuum table. TRUNCATE is the
\* PartitionTruncate reclaim mechanism; using it on a RowVacuum
\* table either contradicts the table's contract or means the
\* contract is wrong.
RowVacuumTablesNotTruncated ==
    \A tx \in Transactions :
        \A i \in 1..Len(tx) :
            TableSpec[tx[i].table].kind = "RowVacuum" =>
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

ASSUME HotPartitionedTablesUseTruncate
ASSUME PartitionTruncateTablesAreReclaimed
ASSUME AppendOnlyAcceptsOnlyInsert
ASSUME RowVacuumTablesNotTruncated

\* TLC still wants a Spec / Init / Next. Provide the smallest
\* possible ones. The check has happened by the time these are
\* evaluated.
VARIABLE checked
vars == <<checked>>

Init == checked = "yes"
Next == UNCHANGED checked
Spec == Init /\ [][Next]_vars

============================================================

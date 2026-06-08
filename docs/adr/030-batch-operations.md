# ADR-030: Durable batch operations for operator bulk mutation

## Status

Accepted. The initial implementation scope is the framework plus `set_priority` and `move_queue` operation kinds. Existing synchronous bulk retry, bulk cancel, and DLQ bulk endpoints stay as-is for `0.6`; migrating them to the framework is deferred.

## Context

Awa needs a first-class way to change the priority of an already queued job. Queue storage makes that more than a metadata update. An available row's priority is part of its physical dispatch lane, so reprioritizing requires moving the row from one `(queue, priority, enqueue_shard)` lane to another, assigning a new lane sequence, and leaving committed spent evidence in the source lane so claim cursors do not block.

The operator problem is broader than priority. The same shape appears in incident workflows such as:

- escalate tens of thousands of available jobs,
- move a queue backlog to a different queue,
- cancel everything matching a kind or tag,
- retry a failed slice from a time window,
- add or remove a tag from a filtered set.

Those are all filter-driven bulk mutations with the same operator requirements: preview, confirmation, asynchronous execution, progress, cancellation, resumability after session loss, and an audit trail. A narrow synchronous `bulk-priority` endpoint would solve only the first symptom and would be difficult to retrofit into the right operational shape later.

Existing job frameworks point in the same direction, but none maps exactly to Awa's queue-storage invariants:

| System | Relevant behavior | Takeaway for Awa |
| --- | --- | --- |
| Celery | Remote control supports revoking ids and stamped headers, but revocation is broadcast to workers and persisted only through worker state files. | Broadcast control is useful for live workers, but it is the wrong durability primitive for persisted queue mutation. |
| Sidekiq | The API can scan queues, scheduled sets, retries, and dead sets, but the documentation warns that scanning shared Redis structures while mutating them is race-prone and inefficient at scale. | Operator repair scripts are useful escape hatches, not the primary interface for bulk mutation. |
| BullMQ | Single-job mutation is explicit, and locked active jobs generally cannot be removed. | Awa should keep clear per-state eligibility rules; active/running work is not the same as queued work. |
| Oban | SQL-backed APIs expose `cancel_all_jobs`, `retry_all_jobs`, and `delete_all_jobs` over queryables and return counts synchronously. | Filter/queryable bulk mutation is a proven SQL-job-queue surface, but Awa's queue-storage lane rewrites and large operator batches need durable async progress rather than one large synchronous call. |
| Cloud Tasks | Management APIs are task- and queue-oriented, with at-least-once dispatch and per-task deduplication. | Queue-level control and task-level mutation do not replace filtered bulk edits across stored work. |

The design should therefore treat a bulk mutation as its own durable control-plane object, similar to a job that mutates other jobs, but not dispatched through a user queue.

## Decision

Add a first-class `awa.batch_operations` table and a maintenance-leader runner. Operators submit a batch operation with an operation kind, a structured filter, and an operation-specific spec. The runner scans a bounded set of matching jobs, applies the mutation in small transactions, persists progress after each chunk, and finalizes the operation as completed, cancelled, or failed.

### Table

The control-plane table is global, not part of the queue-storage schema:

```sql
CREATE TABLE awa.batch_operations (
    id              UUID PRIMARY KEY,
    op_kind         TEXT NOT NULL,
    filter          JSONB NOT NULL,
    spec            JSONB NOT NULL,
    state           TEXT NOT NULL,
    submitted_by    TEXT,
    submitted_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    started_at      TIMESTAMPTZ,
    finalized_at    TIMESTAMPTZ,
    cursor          JSONB,
    total_matched   BIGINT,
    processed       BIGINT NOT NULL DEFAULT 0,
    skipped         BIGINT NOT NULL DEFAULT 0,
    errored         BIGINT NOT NULL DEFAULT 0,
    last_error      TEXT,
    runner_instance UUID,
    retention_until TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
```

The scan snapshot lives in a companion item table:

```sql
CREATE TABLE awa.batch_operation_items (
    operation_id UUID NOT NULL REFERENCES awa.batch_operations(id) ON DELETE CASCADE,
    job_id       BIGINT NOT NULL,
    state        TEXT NOT NULL DEFAULT 'pending',
    error        TEXT,
    processed_at TIMESTAMPTZ,
    PRIMARY KEY (operation_id, job_id)
);
```

`state` is one of `pending`, `scanning`, `running`, `cancelling`, `completed`, `cancelled`, or `failed`. `retention_until` is set from configuration when the operation finalizes; the default retention is 90 days.

`cursor` is retained for coarse progress/debugging, but resumability is item-table based. Scanning captures the eligible job ids into `batch_operation_items`, which freezes membership and lets the runner account each item exactly once as `processed`, `skipped`, or `errored`.

For `0.6`, per-row errors are summarized by `errored` and `last_error`. A future `awa.batch_operation_errors` side table may be added if operators need full per-row audit trails.

### Lifecycle

The lifecycle is:

```text
pending -> scanning -> running -> completed
                            \-> cancelling -> cancelled
                            \-> failed
```

Scanning compiles the filter, captures the upper scan bound, computes `total_matched`, and inserts the eligible job ids into `batch_operation_items`. `total_matched` is the number of operation-eligible rows at scan time, not all rows that satisfy the raw filter. For example, `set_priority` and `move_queue` count only `available` and `scheduled` rows because running, waiting, and terminal rows are not eligible for those operations.

Running claims pending item rows in job-id order and processes each item in a short transaction. The row mutation and the item/accounting update commit together, so a leader failover cannot replay an already-accounted item. Rows that matched during scanning but are no longer eligible by execution time are counted as `skipped`; examples include rows claimed by a worker, cancelled by another operator, or already moved by an overlapping batch operation.

Cancellation is cooperative. `PATCH /api/batch-ops/:id { "state": "cancelling" }` requests cancellation, and the runner checks at chunk boundaries. Rows already mutated remain mutated. Undo is modeled as a separate inverse operation.

### Runner ownership

The maintenance leader owns scheduling and execution. This keeps batch operations with other internal control-plane work such as cron evaluation, promotion, rescue, and retention, and avoids requiring a user-worker queue to be healthy before an operator can repair that queue.

The initial runner is a new maintenance lane with bounded concurrency. The default for `0.6` may be one running operation for simplicity, but the table and handler contract allow a higher limit later. Each operation is owned by at most one runner instance at a time through the existing maintenance leadership pattern and the `runner_instance` field. If concurrency is raised, row-level and lane-level storage locks preserve safety; overlapping operations may interleave and individual rows may be skipped if they are no longer eligible when reached.

Batch operations should not run inside the claim hot path. A target throughput around hundreds or low thousands of rows per second is enough for operator workflows and keeps pressure well below dispatch throughput.

### Filter language

Use a structured JSON filter for `0.6`:

```json
{
  "kind": "send_email",
  "queue": "default",
  "ids": [1, 2, 3],
  "tag": "customer:acme",
  "state": "available",
  "created_at_gte": "2026-06-01T00:00:00Z",
  "created_at_lt": "2026-06-08T00:00:00Z"
}
```

All fields are optional, but mutation endpoints that can affect large sets must require either a non-empty filter or an explicit `all=true` confirmation in the API/CLI layer. `ids` composes with other fields as an intersection, not a union.

The initial field set is deliberately small: `kind`, `queue`, `ids`, `tag`, `state`, and created-at bounds. Add attempt-count, finalized-at, error-text, metadata, or argument filters only when an operator use case needs them and the storage projection can support them predictably.

CEL is not the initial persisted filter language. CEL is attractive because Awa already uses CEL for callback expressions and because it is safe, portable, and designed for compile-once/evaluate-many usage. It is a poor first filter substrate for batch mutation because the runner needs SQL pushdown, exact preview counts, stable pagination, and clear indexability. Evaluating a CEL predicate after a broad SQL scan would make previews expensive and could surprise operators about what was actually bounded by the database.

CEL remains a future extension. A later version may allow a `cel` field only when paired with structured SQL-pushdown bounds, storing the checked expression/AST and evaluating it against candidate rows. Until then, the JSON filter is the contract.

### Preview and search

Preview uses the same filter compiler and operation eligibility rules as scanning, but does not enqueue an operation. It returns at least:

```json
{
  "total_matched": 47000,
  "sample": [
    { "id": 123, "kind": "send_email", "queue": "default", "state": "available" }
  ]
}
```

The sample is a small deterministic page, ordered by job id, so an operator sees representative rows before confirmation. The preview is advisory, not a lock or snapshot. The operation's later `scanning` phase recomputes the count and upper bound.

The UI may implement preview through either a dedicated endpoint or through search APIs that return both rows and full counts. The important architectural constraint is that preview and execution share the same filter compiler so the UI does not show one population and mutate another.

### API shape

The admin API gains:

```text
POST  /api/batch-ops/preview       { op_kind, filter, spec } -> { total_matched, sample }
POST  /api/batch-ops               { op_kind, filter, spec } -> { id }
GET   /api/batch-ops?state=running
GET   /api/batch-ops/:id
PATCH /api/batch-ops/:id           { state: "cancelling" }
```

Client libraries and CLI should expose operation-specific helpers for common actions, but those helpers submit batch operations rather than creating separate one-off API semantics. The first CLI surface is the generic envelope: `awa batch-ops submit --op-kind set_priority --filter '{"queue":"default"}' --spec '{"priority":1}'` and `awa batch-ops submit --op-kind move_queue --filter '{"queue":"default"}' --spec '{"queue":"escalations"}'`.

Single-row priority and queue moves may still have direct helpers for ergonomics, but internally they should use the same storage mutation functions as batch handlers. They do not need to create a `batch_operations` row unless the caller requests audit/progress semantics.

### Operation: `set_priority`

`set_priority` has:

```json
{ "op_kind": "set_priority", "spec": { "priority": 1 } }
```

Only `available` and `scheduled` rows are eligible in `0.6`.

For canonical storage, the mutation is a guarded row update on the live queued/scheduled row.

For queue storage scheduled rows in `deferred_jobs`, the mutation is a guarded `UPDATE SET priority = $new_priority`.

For queue storage available rows, changing priority moves the row to a new physical lane. The per-row helper must run in one transaction and reuse the same lane-safety invariants as claim and ready cancellation:

1. Recheck that the source ready row is still live and operation-eligible.
2. Resolve the target `(queue, priority, enqueue_shard)` lane.
3. Lock the source `ready_entries` row with `FOR UPDATE`; the source claim-head row is read but not locked.
4. Reserve a new `lane_seq` from the destination lane, which takes the destination lane/head lock in the normal enqueue order.
5. Insert a source `ready_tombstones` row.
6. Insert the destination `ready_entries` row with the new priority.
7. Stamp `_awa_original_priority` once if missing.
8. Commit.

Rows already running, waiting for callbacks, completed, failed, cancelled, or in the DLQ are skipped. Changing a running job's current attempt priority is intentionally out of scope; if the operator needs immediate effect they must cancel/retry or wait for the next attempt.

### Operation: `move_queue`

`move_queue` has:

```json
{ "op_kind": "move_queue", "spec": { "queue": "escalations", "priority": 1 } }
```

`priority` is optional. If omitted, the row keeps its current priority. If present, queue and priority move in the same row transaction.

Only `available` and `scheduled` rows are eligible in `0.6`.

For canonical storage and queue-storage scheduled rows in `deferred_jobs`, the mutation is a guarded `UPDATE SET queue = $new_queue, priority = COALESCE($new_priority, priority)`.

For queue storage available rows, the per-row helper is the same lane move as `set_priority` with additional destination queue resolution:

1. Recheck that the source ready row is still live and operation-eligible.
2. Resolve the destination queue's `enqueue_shards` configuration.
3. Route the destination row through the normal queue-storage reinsert helper. Because ready/deferred storage does not retain the producer's original ordering key, moved no-key rows use the destination queue's shard rotor. The source shard is irrelevant after the move.
4. Resolve the destination `(queue, priority, enqueue_shard)` lane.
5. Lock the source `ready_entries` row with `FOR UPDATE`; the source claim-head row is read but not locked.
6. Reserve the destination `lane_seq`, taking the destination lane/head lock in the normal enqueue order.
7. Insert a source `ready_tombstones` row.
8. Insert the destination `ready_entries` row with the queue column rewritten.
9. Stamp `_awa_original_queue` once if missing, and `_awa_original_priority` if priority is also changed.
10. Commit.

If the destination queue is paused, the move still succeeds. Jobs land as available in the paused queue and dispatch when the queue resumes, matching cron-into-paused-queue semantics.

If the destination insert conflicts on uniqueness, the row-level mutation fails for that row only. The runner increments `errored`, updates `last_error` with the job id and conflict kind, and continues.

Lane move helpers must be written with explicit lock ordering or retry-on-deadlock behavior. Concurrent inverse moves such as `A -> B` and `B -> A` must not be able to corrupt storage; at worst one chunk transaction aborts, records or retries the row, and the operation continues.

### Retention and cleanup

Batch operation retention is configurable with `AWA_BATCH_OP_RETENTION_DAYS`, with a conservative default of 90 days. Finalized rows get `retention_until = finalized_at + configured_retention`. Maintenance deletes finalized operations whose retention has expired.

An explicit CLI/API purge should also exist for operators who need shorter retention in a particular environment, for example `awa batch-ops purge --before 2026-03-01`.

Pending, scanning, running, and cancelling operations are never removed by retention cleanup.

### Observability

The operation detail surface reports state, counts, submitted/finalized timestamps, runner instance, cursor summary, and last error. Metrics should cover submitted operations, active operations, rows processed/skipped/errored, chunk duration, and final states.

Batch operation rows are the audit log for who did what and when. They are not intended to be a forensic record of every row changed in `0.6`; that is the possible future error/audit side table.

## Consequences

### Positive

- Reprioritization lands with the right async operator workflow instead of a narrow synchronous endpoint that will need replacement.
- `move_queue` shares the same framework and most of the same lane-move implementation as `set_priority`.
- Operators get preview, progress, cancellation, resumability, and history for high-blast-radius actions.
- Future operations such as bulk cancel, retry, DLQ purge, tag mutation, and scheduled-at edits can reuse the same table, runner, preview, and UI surfaces.
- Client libraries and the CLI can expose stable operation-specific commands while the HTTP/API substrate remains one generic batch-operations surface.

### Negative / Risks

- Adds a new control-plane entity and table for operators to learn.
- Adds maintenance-runner complexity and a new failure mode: an unhealthy maintenance leader means batch operations do not progress.
- The UI needs both a batch-operations history/progress surface and operation triggers from jobs/queues/kinds pages.
- Preview is advisory. Operators must understand that rows can be claimed, completed, or otherwise changed between preview and execution.
- Structured JSON filters are less expressive than CEL or arbitrary SQL. This is intentional for `0.6`, but some operator requests will require new filter fields.
- Batch operations have higher blast radius than single-row admin calls. Read-only mode must disable them, and future RBAC should likely distinguish batch permissions from ordinary job mutation permissions.

## Alternatives considered

### A. Add only synchronous `bulk-priority`

This directly addresses reprioritization but leaves the same UX and safety gap for queue moves, cancel-by-filter, retry-by-filter, and tag edits. It also encourages large HTTP requests to sit on database work synchronously, with no durable progress or resume after client disconnect. Rejected.

### B. Add separate async tables per operation kind

For example, `priority_changes`, `queue_moves`, and future `bulk_cancels`. This keeps each operation schema explicit but duplicates lifecycle, progress, cancellation, retention, and UI work. Rejected in favor of a single operation table with operation-specific `spec` validation.

### C. Run batch operations as ordinary Awa jobs / system jobs

This is tempting because Awa already has durable jobs, retry, progress, and UI inspection. A future explicit **system jobs** lane could be a useful general-purpose internal work mechanism, but ordinary Awa jobs are the wrong substrate for this v0.6 operator workflow.

The argument against ordinary jobs is dependency direction. Batch operations are repair/control-plane actions: an operator may need to reprioritize or move the exact queue backlog that is currently paused, overloaded, misconfigured, or unable to drain. If batch execution itself is dispatched through user-job claiming, the repair path depends on the subsystem being repaired. That creates bad incident behavior: a paused destination/source queue, a saturated queue, a broken handler registry, or a fleet intentionally running maintenance-only would block the bulk mutation.

The second issue is privilege and blast radius. Batch operations mutate storage metadata and lane placement directly; they should run with maintenance/control-plane authority, not through user handler dispatch. Putting them in ordinary job tables also blurs retention and audit semantics: a batch op is an admin command with item accounting, cancellation, and 90-day audit retention, not a user workload with attempts, handler code, uniqueness, and DLQ policy.

An explicit system-job engine could be revisited later if it has these properties: independent from user queues and pause state, maintenance-owned execution, no user handler registry dependency, separate permissions, and item-level accounting. At that point it would be close to the chosen `batch_operations` control-plane table. Rejected for `0.6` in favor of the smaller dedicated control-plane entity.

### D. Let operators run SQL or scripts

This is effectively the current state. It is unsafe for queue storage because lane movement requires storage-specific invariants, tombstones, metadata stamps, uniqueness checks, and claim-head coordination. Rejected as the primary interface, though emergency SQL remains an operator escape hatch.

### E. Use CEL as the initial filter language

CEL is safe and expressive, and Awa already has CEL experience in callback expressions. However, batch mutation needs SQL pushdown and exact counts. A CEL-first design would either require compiling a useful subset to SQL immediately or scanning too broadly and filtering in the runner. Rejected for `0.6`; kept as a future extension paired with structured bounds.

### F. Chosen: durable batch-operation control plane

This adds one table and one runner lane, then amortizes that cost across priority changes, queue moves, and future operator workflows. It matches Awa's Postgres-first durability model and keeps queue-storage mutation invariants inside tested storage helpers rather than client scripts.

## Relationship to other ADRs

- **ADR-019** (Queue Storage Engine). Batch operation handlers must use queue-storage lane, tombstone, deferred, and uniqueness invariants rather than rewriting `ready_entries` directly.
- **ADR-020** (Dead Letter Queue). Existing DLQ bulk retry/purge/move endpoints remain synchronous in `0.6`; migrating them to batch operations is a future compatibility/UX decision.
- **ADR-023** (Receipt Plane Ring Partitioning). Running and waiting rows are not mutated by `set_priority` or `move_queue` in `0.6`; active attempts remain guarded by receipt/lease semantics.
- **ADR-025** (Sharded Enqueue Heads). Queue moves must hash against the destination queue's `enqueue_shards` configuration and assign a destination lane sequence there.
- **ADR-026** (Narrow Terminal History). Terminal rows are not eligible for the initial operations; history remains immutable operator evidence.
- **ADR-028** (Maintenance-only runtime role). Batch operation execution is another maintenance-owned responsibility. A maintenance-only runtime must run it; a callback-only or read-only UI must not.
- **ADR-029** (Transactional follow-up jobs). Batch operations are not follow-up jobs. They are internal control-plane work that may later emit observation events or follow-up jobs, but their execution is owned by maintenance.

## Open implementation notes

- Define the exact Rust model types: `BatchOperation`, `BatchOperationKind`, `BatchOperationFilter`, `BatchOperationSpec`, and progress DTOs.
- Decide whether `set_priority` and `move_queue` single-row admin helpers create audit rows or only call the shared storage helper directly.
- Add storage-level chaos tests for concurrent claim plus `set_priority`, concurrent claim plus `move_queue`, and inverse queue moves.
- Add docs that direct SQL bypasses batch-operation accounting and must not be used for ordinary operator workflows.

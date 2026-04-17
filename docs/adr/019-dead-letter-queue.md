# ADR-019: First-Class Dead Letter Queue

## Status

Accepted

## Context

Operators needed a durable, inspectable home for permanently-failed jobs that
survives past the normal `failed_retention` window (default 72h) so that
postmortems and bulk replays can happen on human timescales rather than
Postgres cleanup timescales. Before this ADR, jobs that exhausted their
retries or hit a terminal error stayed in `awa.jobs_hot` with `state='failed'`
and were eventually deleted by the retention sweep — indistinguishable from
jobs that are merely "done and not worth keeping."

Keeping failed jobs in the hot table also had operational costs:

- `failed` rows compete with live work for autovacuum on `jobs_hot`
- Every `jobs_hot` index carries entries for dead rows that dispatchers must
  skip
- Retention tuning for "how long to keep failed jobs for forensics" conflicts
  with retention tuning for "how fast must the hot table shrink after an
  incident"

A separate, append-mostly DLQ table decouples those concerns.

## Decision

Add `awa.jobs_dlq` (migration v008) as an absorbing, off-hot-path resting
place for permanently-failed jobs. Opt-in per queue, with atomic lease-guarded
moves, an independent retention policy, and operator surfaces across CLI,
Python, REST, and Web UI.

### Separate table, not a new state

`awa.jobs_dlq` mirrors the columns of `awa.jobs_hot` and adds three fields:
`dlq_reason TEXT`, `dlq_at TIMESTAMPTZ`, `original_run_lease BIGINT`. It has
no dequeue index — dispatchers never claim DLQ rows.

Rejected alternatives:

- **`state = 'dlq'` on `jobs_hot`.** The claim indexes (`state, queue,
  priority, run_at`) would carry DLQ entries until retention aged them out.
  That defeats the purpose — the hot path stays polluted even though DLQ rows
  are never executable. It also makes retention tuning for DLQ forensics
  couple back to `failed_retention`.
- **Partial index excluding `state = 'dlq'`.** Works for the claim path, but
  still leaves DLQ rows in the heap, which affects vacuum, scans, and MVCC
  horizon on the hottest table.
- **Move to `awa.scheduled_jobs` or a new view.** Those carry semantic
  expectations about promotion. DLQ is absorbing — rows only leave by explicit
  operator action or retention cleanup.

A dedicated table means the hot-path query plan (always enforced by the
plan-guard test) never touches DLQ data, and DLQ growth is orthogonal to
`failed_retention` tuning.

### Lease-guarded atomic move

The runtime-path move uses `awa.move_to_dlq_guarded(id, run_lease, reason,
error_json, progress)`, which runs the DELETE-from-`jobs_hot` +
INSERT-into-`jobs_dlq` in a single statement with the same
`state = 'running' AND run_lease = $lease` guard as normal finalization (see
ADR-013). That means:

- Heartbeat/deadline rescue and admin cancel remain the sole winners against a
  late DLQ move — no new race surface versus existing finalization
- The lease guard is identical to the in-place `failed` UPDATE, so the DLQ
  toggle does not change concurrency invariants

The TLA+ spec (`AwaCore.tla`) carries this as `MoveToDlqAccepted(w, j)` with
the same `taskLease[w][j] = lease[j]` guard and transitions directly to the
absorbing `dlq` state.

For admin-initiated bulk moves that don't own a `run_lease`,
`awa.move_failed_to_dlq` is guarded by `state = 'failed'` instead — the state
check is sufficient because `failed` rows are not being transitioned by any
other process.

### Opt-in, per-queue policy

`dlq_enabled_by_default` defaults to **`false`**. Individual queues can be
opted in via `queue_dlq_enabled(queue, true)` on the client builder. This
preserves the upgrade path: existing deployments see zero behavior change
when they pick up v008 migration until they opt in.

Rationale: DLQ semantics change where failed jobs live, how retention works,
and what admin metadata operators see. Flipping that universally on upgrade
would surprise existing dashboards and alerts that count `failed` rows in
`jobs_hot`.

### Single-invariant DLQ population

Only the executor's terminal path (Terminal errors and `Retryable` at
`attempt >= max_attempts`) and the callback-timeout rescue at exhausted
attempts route directly into the DLQ. Heartbeat and deadline rescue always
transition to `retryable` — the next claim re-enters `apply_terminal_failure`
which then routes to DLQ (or stays `failed`) based on queue policy. This
"single invariant, covered transitively" pattern keeps the DLQ entry points
narrow and means every DLQ row came through one well-tested choke point.

### Retention decoupled from failed\_retention

`dlq_retention` defaults to **30 days**, intentionally longer than the 72h
`failed_retention`. The global cleanup pass sweeps DLQ rows past that window;
`RetentionPolicy.dlq` per-queue overrides let individual queues carry longer
(or shorter) DLQ retention without touching other queues. The global pass
excludes queues with an override so those queues follow only their explicit
policy, not both.

### Metrics and surfaces

- `awa.job.dlq_moved` — counter with `{kind, queue, reason}` for runtime
  moves, and an optional `{queue, reason}` form for bulk admin moves
- `awa.job.dlq_retried` — counter when operators revive DLQ rows
- `awa.job.dlq_purged` — counter for retention sweeps
- `awa.job.dlq_depth` — gauge per queue, emitted for every queue seen in
  `queue_stats` so that a drained queue's depth resets to zero
- CLI: `awa dlq list|depth|retry|retry-bulk|move|purge`
- Python (sync + async): `list_dlq`, `get_dlq_job`, `dlq_depth`,
  `retry_from_dlq`, `bulk_retry_from_dlq`, `move_failed_to_dlq`,
  `bulk_move_failed_to_dlq`, `purge_dlq_job`, `purge_dlq`
- REST: `/api/dlq` (list), `/api/dlq/{id}` (detail), retry/purge endpoints
- Web UI: `/dlq` list + `/dlq/$id` detail, plus a DLQ-depth banner on
  `/queues`
- Admin visibility: `awa job dump <id>` and `GET /api/jobs/{id}` fall back to
  `jobs_dlq` and attach DLQ metadata. `can_retry`/`can_cancel` report `false`
  for DLQ rows so UI consumers route via the DLQ endpoints.

## Consequences

### Positive

- Hot-path claim indexes and MVCC horizon are unaffected by DLQ growth
- DLQ retention tuning is independent of `failed_retention`
- Lease-guarded move inherits the same safety proof as regular finalization
- Opt-in default preserves the upgrade path for existing deployments
- Single population choke point simplifies reasoning and tests

### Negative

- Operators now have two places a failed job might live (`jobs_hot` with
  `state='failed'` when DLQ is off, or `jobs_dlq` when DLQ is on) — admin
  surfaces abstract over this, but raw SQL consumers may need awareness
- Manual admin moves (`bulk_move_failed_to_dlq`) are the only way to migrate
  pre-existing `failed` rows into DLQ after opting in; there is no automatic
  backfill
- Callback-timeout rescues on exhausted attempts currently route into DLQ
  directly (mirroring executor exhaustion); other rescue paths go through
  `retryable` and re-enter the DLQ decision — this asymmetry is documented
  but is a source of future simplification

### Upgrade notes

- v008 is additive. No rows move at migration time.
- `dlq_enabled_by_default = false` means zero behavior change on upgrade.
- Existing `failed` rows age out per `failed_retention` unless explicitly
  moved with `awa dlq move` / `bulk_move_failed_to_dlq`.
- `awa.job.dlq_*` counters stay at zero for deployments that don't opt in;
  `awa.job.dlq_depth` gauges emit zero for known queues once any queue on
  the worker has been observed (no stale carryover — see the gauge emission
  logic in `maintenance::run_dlq_cleanup_pass`).
- TLA+ `JobStates` gained `"dlq"` as an absorbing state. `RetryFromDlq`
  resets the lease to 0 to match the SQL, which inserts the revived row
  with `run_lease = 0`.

## References

- Migration: `awa-model/migrations/v008_dead_letter_queue.sql`
- Core model: `awa-model/src/dlq.rs`
- Runtime policy + atomic move: `awa-worker/src/executor.rs`
  (`apply_terminal_failure`)
- Maintenance retention: `awa-worker/src/maintenance.rs`
  (`run_dlq_cleanup_pass`)
- Spec: `correctness/core/AwaCore.tla` — `MoveToDlqAccepted`, `RetryFromDlq`
- Related ADRs: [012-hot-deferred-job-storage](012-hot-deferred-job-storage.md),
  [013-run-lease-and-guarded-finalization](013-run-lease-and-guarded-finalization.md),
  [014-structured-progress](014-structured-progress.md)

# ADR-007: Periodic/Cron Jobs via Leader-Evaluated Schedules

## Status

Accepted

## Context

Every production job queue needs periodic/cron job support. Without it, users must wire up an external cron daemon (systemd timers, Kubernetes CronJobs, or crontab) to insert jobs on a schedule, which defeats Awa's "Postgres is the only dependency" pitch. Every competitor (River, Oban, GoodJob) ships cron scheduling as a core feature.

The question is: where do schedules live and who evaluates them?

Options considered:

1. **Application-side timers.** Each worker instance runs its own `tokio::time::interval` and inserts jobs when timers fire. Simple, but without coordination every instance would insert duplicates.

2. **Database-side `pg_cron` extension.** Schedules are defined as `pg_cron` jobs. Zero application code, but requires the `pg_cron` extension which isn't available on all managed Postgres services (RDS, Cloud SQL). Violates the "vanilla Postgres" constraint.

3. **`awa.cron_jobs` table + leader-evaluated schedules.** Schedules are defined in application code, synced to a database table by the leader, and evaluated on a timer within the existing `MaintenanceService`. When a schedule fires, the leader atomically marks it as enqueued AND inserts the job.

## Decision

Option 3: `awa.cron_jobs` table with leader-evaluated schedules.

Schedules are defined in Rust (`ClientBuilder::periodic()`) or Python (`client.periodic()`), validated eagerly at registration time (bad cron expressions or timezones fail at startup), and synced to `awa.cron_jobs` via UPSERT every 60 seconds. The leader evaluates all schedules every 1 second and enqueues jobs when they're due.

### Crash-safe atomic enqueue

The biggest risk in any cron scheduler is the "mark-then-insert" gap: if the process marks a schedule as fired, then crashes before inserting the job, the fire is lost. Awa solves this with a single CTE that atomically does both:

```sql
WITH mark AS (
    UPDATE awa.cron_jobs
    SET last_enqueued_at = $2, updated_at = now()
    WHERE name = $1
      AND (last_enqueued_at IS NOT DISTINCT FROM $3)
    RETURNING name, kind, queue, args, ...
)
INSERT INTO awa.jobs (kind, queue, args, state, ...)
SELECT kind, queue, args, 'available', ...
FROM mark
RETURNING *
```

The `IS NOT DISTINCT FROM` clause acts as a compare-and-swap: if another leader already claimed this fire (last_enqueued_at changed), the UPDATE matches 0 rows and the INSERT produces nothing. If the process crashes mid-transaction, Postgres rolls back both. Either both happen or neither does.

### Multi-deployment safety

A naive sync would `DELETE FROM awa.cron_jobs WHERE name NOT IN (...)` to remove schedules that no longer exist in code. But when multiple deployments share the same database (e.g., a web service and a background worker), each deployment would delete the other's schedules.

Awa's sync is additive: UPSERT only, never DELETE. Stale schedules from decommissioned deployments can be cleaned up manually via `awa cron remove <name>` or direct SQL.

### Missed-fire policy

By default, when evaluation is delayed or a worker starts after being down, the scheduler enqueues only the most recent missed fire time per schedule, not every missed occurrence. This `coalesce` policy avoids a thundering herd of jobs when a worker recovers from a long outage.

Schedules that represent reconciliation or polling work can opt into `catch_up`, which enqueues each missed fire in timestamp order subject to the worker's bounded per-pass catch-up limit. Catch-up schedules should use idempotent handlers and expect several jobs to appear in quick succession after downtime.

### Pause and resume

Schedules can be paused without deleting them. `cron_jobs.paused_at` (a nullable timestamp) records when the schedule was paused; `paused_by` records the operator label. The column shape mirrors `queue_meta` so operators see one convention for both pause surfaces.

The evaluator skips paused rows up front to avoid wasted CAS work, and the `atomic_enqueue` CTE re-checks `paused_at IS NULL` inside the same UPDATE that performs the compare-and-swap on `last_enqueued_at`. The CTE guard is load-bearing: it closes the window where a leader read the schedule, the operator paused it, and the leader then attempted to enqueue.

`last_enqueued_at` is not touched while paused. On resume, the existing `missed_fire_policy` decides catch-up behaviour — a coalesced schedule fires once on resume; a catch-up schedule replays missed fires in order, bounded by the per-pass limit. Operators choose the catch-up shape when they register the schedule; pausing does not change it.

Manual triggers (`trigger_cron_job`) bypass pause. Pause stops *automatic* fires; an explicit operator action is always allowed. Operators who want the schedule to be entirely inert should delete the row.

#### In-flight jobs and queue pause

Once `atomic_enqueue` commits, the job is a normal row in the queue with no back-reference to its cron source other than `metadata.cron_name` as a label. Pausing a schedule does not affect:

- jobs already sitting `available` in the queue (a worker will pick them up),
- jobs currently `running` (heartbeats, deadlines, and callbacks continue),
- subsequent retries of those jobs (the per-job `max_attempts` budget governs, not the schedule),
- DLQ entries derived from those jobs.

An operator who wants to also purge in-flight cron-fired work pauses the schedule and then bulk-cancels jobs filtered by `metadata.cron_name = <name>`. The two operations are intentionally separate because there is no canonical answer to "should already-running jobs be killed too?".

Cron pause and queue pause are also independent. Queue pause is enforced at dispatch, not enqueue: the atomic CTE inserts into a paused queue without complaint, and `last_enqueued_at` advances normally. With cron active + queue paused, fires accumulate as available jobs and dispatch on queue resume. With cron paused + queue active, no new fires arrive but in-flight work continues. Each surface is resumed independently. The `/cron` UI surfaces a "queue paused" badge so an operator looking at a quiet schedule sees the cause even when it is the consumer side that is stopped.

#### Model

The pause guarantee is verified in TLA+. `AwaCron` includes `Pause` and `Resume` actions; the `PausedBlocksEnqueue` property asserts that no step taken from a paused state increments `jobCount`. Liveness is checked under weak fairness on `Resume`, so a paused schedule still satisfies the "due fires eventually enqueue" property once Resume is taken.

### Leader liveness verification

The advisory lock is session-scoped: as long as the connection is alive, the lock is held. But if the connection drops silently, Postgres releases the lock and another node becomes leader -- while the old node may still think it's leader. Awa verifies liveness by pinging the leader connection every 30 seconds (`SELECT 1`). If the ping fails, the node re-enters the election loop.

## Consequences

### Positive

- **No external dependencies.** Schedules live in Postgres, evaluated by the existing leader. No systemd, no Kubernetes CronJob, no `pg_cron`.
- **Crash-safe.** The atomic CTE eliminates the mark-then-insert gap.
- **Multi-deployment safe.** Additive-only sync prevents accidental deletion of other deployments' schedules.
- **Timezone-aware.** Schedules support IANA timezones via `chrono-tz`, handling DST transitions correctly.
- **Eager validation.** Invalid cron expressions or timezones fail at builder time, not at fire time.
- **Pause without delete.** Operators can pause a noisy or misbehaving schedule and resume it later; `last_enqueued_at` and the schedule definition survive a pause/resume cycle. Re-deploying the schedule does not clear pause.

### Negative

- **1-second evaluation granularity.** Sub-second schedules are not supported. This is acceptable for cron-style workloads (hourly, daily, etc.).
- **Leader bottleneck.** All cron evaluation happens on the leader. For thousands of schedules this could become a bottleneck, though in practice most deployments have dozens, not thousands.
- **No automatic orphan cleanup.** Decommissioned schedules must be removed manually. This is an intentional trade-off for multi-deployment safety.
- **Coalesced by default.** If a schedule was down for 24 hours, only the most recent fire is recovered unless the schedule explicitly opts into `catch_up`.

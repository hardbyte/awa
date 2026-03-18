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

### No backfill

When a worker starts after being down, the scheduler enqueues only the most recent missed fire time per schedule, not every missed occurrence. This avoids a thundering herd of jobs when a worker recovers from a long outage. This matches River's approach.

### Leader liveness verification

The advisory lock is session-scoped: as long as the connection is alive, the lock is held. But if the connection drops silently, Postgres releases the lock and another node becomes leader -- while the old node may still think it's leader. Awa verifies liveness by pinging the leader connection every 30 seconds (`SELECT 1`). If the ping fails, the node re-enters the election loop.

## Consequences

### Positive

- **No external dependencies.** Schedules live in Postgres, evaluated by the existing leader. No systemd, no Kubernetes CronJob, no `pg_cron`.
- **Crash-safe.** The atomic CTE eliminates the mark-then-insert gap.
- **Multi-deployment safe.** Additive-only sync prevents accidental deletion of other deployments' schedules.
- **Timezone-aware.** Schedules support IANA timezones via `chrono-tz`, handling DST transitions correctly.
- **Eager validation.** Invalid cron expressions or timezones fail at builder time, not at fire time.

### Negative

- **1-second evaluation granularity.** Sub-second schedules are not supported. This is acceptable for cron-style workloads (hourly, daily, etc.).
- **Leader bottleneck.** All cron evaluation happens on the leader. For thousands of schedules this could become a bottleneck, though in practice most deployments have dozens, not thousands.
- **No automatic orphan cleanup.** Decommissioned schedules must be removed manually. This is an intentional trade-off for multi-deployment safety.
- **No backfill.** If a schedule was down for 24 hours, only the most recent fire is recovered. Users who need guaranteed delivery of every missed fire must implement their own catch-up logic.

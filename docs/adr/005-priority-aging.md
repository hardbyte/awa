# ADR-005: Priority Aging Algorithm

## Status

Accepted

## Context

Awa supports four priority levels (1 = highest, 4 = lowest) per job (PRD section 6.2). Priority determines the order in which jobs are claimed from a queue. Without any aging mechanism, low-priority jobs can be starved indefinitely if higher-priority jobs are continuously enqueued.

Starvation is a real operational problem:

- A batch of 10,000 priority-1 jobs is enqueued for a migration. Priority-3 email jobs stop processing for the duration.
- A misconfigured producer enqueues high-priority jobs in a tight loop. All other priorities effectively halt.
- During peak load, the queue never drains completely. Priority-4 analytics jobs accumulate without bound.

Job queues with fixed priority ordering (no aging) require operators to manually intervene -- pausing high-priority producers, re-prioritizing jobs, or provisioning dedicated queues. Aging provides an automatic, tunable solution.

## Decision

Use the formula `GREATEST(1, priority - FLOOR(EXTRACT(EPOCH FROM (now() - run_at)) / aging_interval)::int)` to compute an effective priority at claim time.

This is implemented in the claim CTE (`awa-worker/src/dispatcher.rs` and `awa-model/queries/claim.sql`):

```sql
ORDER BY
  GREATEST(1, priority - FLOOR(EXTRACT(EPOCH FROM (now() - run_at)) / $4)::int) ASC,
  run_at ASC,
  id ASC
```

### How It Works

The effective priority decreases (improves) by 1 level for each `aging_interval` that a job has been waiting in the queue. The `GREATEST(1, ...)` clamp prevents the effective priority from going below 1 (the highest level).

With the default `priority_aging_interval` of 60 seconds:

| Original Priority | Wait Time | Effective Priority |
|---|---|---|
| 4 (lowest) | 0s | 4 |
| 4 | 60s | 3 |
| 4 | 120s | 2 |
| 4 | 180s+ | 1 (clamped) |
| 3 | 0s | 3 |
| 3 | 60s | 2 |
| 3 | 120s+ | 1 (clamped) |
| 2 | 0s | 2 |
| 2 | 60s+ | 1 (clamped) |
| 1 (highest) | any | 1 (already highest) |

A priority-4 job waiting 3 minutes is treated as priority-1, equal to a freshly enqueued high-priority job. When effective priorities tie, `run_at ASC, id ASC` breaks the tie -- older jobs go first.

### Configuration

The aging interval is configurable per queue via `QueueConfig::priority_aging_interval` (default: 60 seconds). A shorter interval ages jobs faster (stronger starvation prevention, weaker priority enforcement). A longer interval ages jobs slower (stricter priority ordering, weaker starvation prevention).

Setting the interval to a very large value (e.g., `Duration::from_secs(86400)`) effectively disables aging, reverting to strict priority ordering.

### Why This Formula

The formula is evaluated entirely in SQL, in the `ORDER BY` clause of the claim CTE. This means:

1. **No materialized column:** Effective priority is computed at query time, not stored. There is no column to update, no background job to recalculate priorities.
2. **Exact, not approximate:** The aging is based on the actual wall-clock time the job has been waiting, not a periodic "bump" operation.
3. **Index-compatible:** The base index `idx_awa_jobs_dequeue` on `(queue, priority, run_at, id)` is still used for the initial scan. The `GREATEST(...)` expression reorders within the candidate set, which is bounded by the `LIMIT` clause.

Alternative approaches considered:

- **Periodic priority bump (River-style):** A maintenance task periodically decrements priority for old jobs. This requires write I/O proportional to the number of waiting jobs and introduces lag (jobs are only aged at the bump interval).
- **Weight-based fair queueing:** Complex to implement in SQL. Over-engineered for 4 priority levels.
- **Separate queues per priority:** Requires the dispatcher to implement a scheduling policy across queues. Increases configuration surface area.

## Consequences

### Positive

- **No starvation:** Every job eventually reaches effective priority 1, guaranteeing it will be claimed ahead of any newly enqueued lower-effective-priority job.
- **Zero write amplification:** Aging is computed at read time in the claim query. No background updates, no additional writes.
- **Tunable per queue:** Different queues can have different aging intervals based on their workload characteristics.
- **Transparent:** The effective priority is a function of the original priority and wait time -- both visible in the job row. Operators can predict when a low-priority job will be promoted.

### Negative

- **Non-obvious ordering:** The effective priority at claim time may differ from the stored `priority` column. Operators inspecting the queue may be surprised that a priority-4 job was claimed before a priority-2 job. The `run_at` column and the aging interval explain the behavior, but it requires understanding the formula.
- **Index efficiency:** The `ORDER BY GREATEST(...)` expression cannot use a simple btree index scan. Postgres must evaluate the expression for all candidate rows (those matching `state = 'available' AND queue = $1 AND run_at <= now()`). This is acceptable because the claim query already uses `LIMIT` (default: 10 per poll) and `SKIP LOCKED`, so the evaluated set is small.
- **Aggressive aging at small intervals:** If `priority_aging_interval` is set very low (e.g., 5 seconds), all priorities collapse to 1 almost immediately, making the priority system meaningless. The default of 60 seconds is a reasonable balance.

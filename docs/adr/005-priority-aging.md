# ADR-005: Priority Ordering and Aging

## Status

Accepted

## Note

The starvation-prevention policy in this ADR still applies under ADR-019. The
implementation detail changes: the canonical engine ages rows in place on
`awa.jobs_hot`, while the queue storage engine must age work by moving entries
between `lane_state`-tracked segments rather than rewriting a single hot heap
row.

## Context

Awa supports four priority levels (1 = highest, 4 = lowest) per job. Priority
determines the order in which jobs are claimed from a queue. Without any aging
mechanism, low-priority jobs can be starved indefinitely if higher-priority jobs
are continuously enqueued.

Starvation is a real operational problem:

- A batch of 10,000 priority-1 jobs is enqueued for a migration. Priority-3 email jobs stop processing for the duration.
- A misconfigured producer enqueues high-priority jobs in a tight loop. All other priorities effectively halt.
- During peak load, the queue never drains completely. Priority-4 analytics jobs accumulate without bound.

## Decision

### Dispatch ordering

The dispatch query claims jobs in strict `priority ASC, run_at ASC, id ASC`
order using a single `FOR UPDATE SKIP LOCKED` index scan on
`idx_awa_jobs_hot_dequeue (queue, priority, run_at, id) WHERE state = 'available'`.

This keeps the dispatch query simple and fast — a single index scan with no
expression evaluation, which scales linearly with worker count.

### Starvation prevention via maintenance-based aging

The maintenance leader runs a periodic `age_waiting_priorities` task at
`priority_aging_interval` (default: 60 seconds). This task finds `available`
jobs with `priority > 1` that have been waiting longer than their aging
threshold and decrements their `priority` column.

The effective priority improves by one level for each `priority_aging_interval`
that a job has waited:

| Original Priority | Wait Time  | Effective Priority |
|-------------------|------------|--------------------|
| 4 (lowest)        | 0s         | 4                  |
| 4                 | 60s        | 3                  |
| 4                 | 120s       | 2                  |
| 4                 | 180s+      | 1 (clamped)        |
| 3                 | 0s         | 3                  |
| 3                 | 60s        | 2                  |
| 3                 | 120s+      | 1 (clamped)        |
| 2                 | 0s         | 2                  |
| 2                 | 60s+       | 1 (clamped)        |
| 1 (highest)       | any        | 1 (already highest) |

A priority-4 job waiting 3 minutes is promoted to priority-1, equal to a freshly
enqueued high-priority job. When priorities tie, `run_at ASC, id ASC` breaks the
tie — older jobs go first.

### Configuration

The aging interval is configurable:

- **Per maintenance service:** `MaintenanceService::priority_aging_interval()` (default: 60s)
- **Per queue (dispatch config):** `QueueConfig::priority_aging_interval` (default: 60s, retained for per-queue overrides in future)

A shorter interval ages jobs faster (stronger starvation prevention, weaker
priority enforcement). A longer interval ages jobs slower (stricter priority
ordering, weaker starvation prevention). Setting the interval to a very large
value effectively disables aging.

### Why maintenance-based rather than query-time aging

The original implementation computed aging at query time using a SQL expression:

```sql
ORDER BY GREATEST(1, priority - FLOOR(EXTRACT(EPOCH FROM (now() - run_at)) / aging_interval)::int) ASC
```

This was moved to the maintenance task because:

1. **Dispatch scalability.** The dispatch query is the hottest path in the
   system. Under high concurrency (200+ workers), keeping it as a simple index
   scan reduces `SKIP LOCKED` contention and avoids CTE materialization fences
   that amplify lock collisions.
2. **Index compatibility.** The strict `ORDER BY priority, run_at, id` matches
   the dequeue index exactly, allowing Postgres to satisfy both the ORDER BY
   and the range predicate from a single index scan with no sort step.
3. **Write amplification is bounded.** The maintenance task only updates jobs
   whose effective priority has actually changed, and limits each pass to 1,000
   rows. At the default 60s interval, this is negligible compared to the
   insert/complete write volume.

## Consequences

### Positive

- **No starvation.** Every waiting job eventually reaches priority 1.
- **Simple dispatch query.** The claim SQL is a single index scan with no
  expression evaluation — maximizes throughput under contention.
- **Tunable.** Operators can adjust aging per queue based on workload.
- **Observable.** The `priority` column reflects the current effective priority
  at all times, making queue state inspectable without formula knowledge.

### Negative

- **Aging granularity is bounded by the maintenance interval.** A job's priority
  updates at most once per aging tick, unlike the continuous query-time formula.
  At the default 60s interval this is invisible in practice.
- **Mutates the priority column.** Operators inspecting the queue see the aged
  priority, not the original enqueue priority. The original priority is not
  separately preserved (it could be reconstructed from `run_at` and the aging
  interval if needed).
- **Leader dependency.** Aging only runs on the maintenance leader. If leader
  election stalls, aging stops. This is the same dependency as rescue and
  promotion — acceptable given the advisory-lock-based leader model.

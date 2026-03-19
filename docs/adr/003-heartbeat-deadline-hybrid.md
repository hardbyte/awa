# ADR-003: Heartbeat + Deadline Hybrid Crash Recovery

## Status

Accepted

## Context

A Postgres-based job queue must handle the case where a worker claims a job and then fails to complete it. Without recovery, the job stays in `running` state forever -- it is effectively lost. The two canonical approaches are:

1. **Heartbeat-based:** Workers periodically update a timestamp. A separate process detects stale timestamps and rescues the job.
2. **Deadline-based:** A hard deadline is set at claim time. If the job is still running past its deadline, it is rescued.

Each approach has a blind spot:

- **Heartbeat alone** cannot catch a job that is still running but stuck. If the worker process is alive and heartbeating but the handler is in an infinite loop, the heartbeat keeps updating and the job is never rescued.
- **Deadline alone** requires setting the deadline conservatively. Jobs with variable execution times (e.g., "usually 10 seconds, sometimes 10 minutes") either get false rescues (deadline too short) or delayed recovery (deadline too long). A worker crash is not detected until the deadline expires, which might be 30 minutes or more.

Awa must also handle Python workers where the GIL can block in unexpected ways. A CPU-bound Python handler (or a C extension holding the GIL) prevents asyncio from running, which could interfere with heartbeat coroutines -- but Awa's heartbeat runs on a dedicated Rust tokio task that never touches the GIL, so this specific failure mode is already handled. However, a truly hung Python process (e.g., segfault in a C extension that doesn't kill the process) could heartbeat via the Rust side while the handler never returns.

## Decision

Awa uses both heartbeat and deadline crash recovery, running as independent maintenance tasks on the leader node (PRD section 6.3).

### Heartbeat Recovery

- The `HeartbeatService` runs on every worker instance (not leader-elected).
- Every 30 seconds (configurable via `ClientBuilder::heartbeat_interval`), it batch-updates `heartbeat_at = now()` for all in-flight `(job_id, run_lease)` pairs:
  ```sql
  UPDATE awa.jobs_hot AS jobs SET heartbeat_at = now()
  FROM unnest($1::bigint[], $2::bigint[]) AS inflight(id, run_lease)
  WHERE jobs.id = inflight.id
    AND jobs.run_lease = inflight.run_lease
    AND jobs.state = 'running'
  ```
- Batches are chunked at 500 in-flight attempts to avoid oversized queries.
- The maintenance leader scans every 30 seconds for running jobs with `heartbeat_at < now() - 90s` and transitions them to `retryable`.
- **Catches:** Process crash, OOM kill, network partition, pod eviction -- any failure where the worker process stops entirely.

### Deadline Recovery

- At claim time, `deadline_at = now() + deadline_duration` is set (default: 5 minutes, configurable per queue via `QueueConfig::deadline_duration`).
- The maintenance leader scans every 30 seconds for running jobs with `deadline_at IS NOT NULL AND deadline_at < now()` and transitions them to `retryable`.
- **Catches:** Infinite loops, hung I/O, deadlocks, any case where the handler does not return within the expected time, even if the worker process is alive and heartbeating.

### Error Attribution

Each rescue writes a distinct error entry to the job's `errors` array so operators can distinguish the failure mode:

- Heartbeat rescue: `"heartbeat stale: worker presumed dead"`
- Deadline rescue: `"hard deadline exceeded"`

### Timing Relationship

```
Time ──────────────────────────────────────────────────────►

Job claimed (t=0)
│
├── heartbeat_at = now()
├── deadline_at = now() + 5m
│
├── Heartbeat tick (t=30s): heartbeat_at updated
├── Heartbeat tick (t=60s): heartbeat_at updated
│
├── Worker crashes at t=65s
│   ├── No more heartbeat updates
│   ├── heartbeat_at = t=60s
│   │
│   ├── t=90s+60s = t=150s: heartbeat_at is 90s stale → RESCUED by heartbeat
│   │   (Recovery time: ~85s after crash)
│   │
│   └── t=300s: deadline_at reached → would rescue if heartbeat didn't catch it
│
├── Handler stuck in infinite loop at t=10s (heartbeats still running):
│   ├── heartbeat_at keeps updating (worker is alive)
│   ├── t=300s: deadline_at reached → RESCUED by deadline
│   │   (Recovery time: ~290s after hang)
```

For a worker crash, heartbeat recovery kicks in within ~90-120 seconds (staleness threshold + scan interval). For a runaway handler, deadline recovery kicks in at the configured deadline (default: 5 minutes). Neither mechanism alone covers both cases.

## Consequences

### Positive

- **No blind spots:** Every failure mode that results in a job stuck in `running` state is eventually caught.
- **Fast crash recovery:** Heartbeat staleness detection recovers crashed jobs in ~90-120 seconds, much faster than waiting for a conservative deadline.
- **Configurable per queue:** Queues with long-running jobs can set longer deadlines without slowing crash recovery.
- **Independent mechanisms:** Each recovery path works correctly even if the other is temporarily unavailable (e.g., maintenance leader transition).

### Negative

- **Write amplification:** Heartbeat updates generate write I/O proportional to the number of in-flight jobs. At 30-second intervals with 500 in-flight jobs, this is ~17 rows/second of UPDATE traffic -- negligible for Postgres. At 50,000 in-flight jobs, it becomes 100 batch updates per tick (chunked at 500), still manageable but worth monitoring.
- **Two scan queries per maintenance cycle:** The leader runs two separate rescue queries (heartbeat staleness and deadline expiry). Both are indexed (`idx_awa_jobs_heartbeat` and `idx_awa_jobs_deadline`) and limited to 500 rows per sweep, so the cost is minimal.
- **Complexity:** Two recovery mechanisms to understand, configure, and test. The trade-off is justified by the elimination of blind spots.

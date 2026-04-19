# ADR-014: Structured Progress and Metadata Persistence

## Status

Accepted

## Note

ADR-019 changes the physical storage of progress. The user-facing progress
model in this ADR stays the same, but queue storage persists progress inside
`attempt_state` and `active_leases` rather than dedicated `progress` columns
on `jobs_hot` and `scheduled_jobs`.

## Context

Long-running handlers had no way to report progress or update checkpoint
metadata during execution. The only mid-execution signal was the binary
heartbeat (alive/dead). This made it impossible to:

- Show users what percentage of a batch job is complete
- Checkpoint work so a retried job can resume rather than restart
- Attach handler-specific metadata (e.g., last processed ID) that survives
  across attempts

## Decision

Add a nullable `progress JSONB` column to both `jobs_hot` and
`scheduled_jobs`. The column stores a structured object:

```json
{
  "percent": 50,
  "message": "Processing batch 5 of 10",
  "metadata": {"last_processed_id": 1234}
}
```

### Why JSONB instead of separate columns

A single JSONB column avoids namespace collision with the user-owned
`metadata` column (which is free-form, set at insert time via multiple paths
including COPY ingestion and cron metadata). It also allows the progress
schema to evolve without migrations.

### Write path: generation-buffered, three flush modes

Handlers write to an in-memory `ProgressState` buffer shared with the
heartbeat service via `Arc<std::sync::Mutex<...>>`. The buffer uses a
generation counter for change tracking:

1. **Heartbeat piggyback** — on every heartbeat cycle, jobs with pending
   progress updates get a combined `SET heartbeat_at = now(), progress =
   v.progress` query. Jobs without changes get the original heartbeat-only
   query. At most two queries per cycle regardless of job count.

2. **State-transition atomic** — when `complete_job()` runs, the latest
   progress snapshot is included in the same UPDATE that transitions state.
   If the UPDATE fails (stale run_lease), the progress is lost with the
   transition — which is correct.

3. **Explicit flush** — `flush_progress()` performs a direct `UPDATE
   jobs_hot SET progress = $2 WHERE id = $1 AND run_lease = $3`. This is
   the reliable path for critical checkpoints and does not return until the
   write is confirmed or the job is no longer running.

### Why std::sync::Mutex, not tokio::Mutex

The critical section is pure in-memory work: JSON cloning, integer
increment, Option swap. No `.await` points are ever held under the lock.
`std::sync::Mutex` is cheaper and simpler for this pattern.

### Lifecycle semantics

Completed jobs clear progress to NULL (ephemeral — the job succeeded and
progress is no longer relevant). All other transitions preserve progress:
retries and snoozes preserve it for checkpoint resumption, failures and
cancellations preserve it for operator inspection. Rescue operations
(stale heartbeat, expired deadline, callback timeout) preserve progress
because the rescue queries in `maintenance.rs` target `awa.jobs_hot`
directly and don't include `progress` in the SET clause — PostgreSQL
leaves unmentioned columns unchanged.

### Shallow merge for metadata

`update_metadata` performs a shallow merge on the `metadata` sub-object:
top-level keys overwrite, nested objects are replaced (not deep-merged).
This is an in-memory operation — no `JSONB ||` at the database level. The
entire progress JSON is written as a unit on flush.

## Consequences

- **Zero overhead for jobs that don't use progress.** The heartbeat
  partitions jobs into two tiers; jobs without pending progress use the
  original query unchanged.
- **No additional round-trips.** Progress writes piggyback on existing
  heartbeats rather than requiring separate queries.
- **Checkpoint resumption is opt-in.** Handlers read
  `ctx.job.progress.metadata` to pick up where they left off. Nothing
  forces handlers to use progress.
- **Unbounded metadata growth risk.** There is no enforced size limit on
  the progress JSONB. A pathological handler could accumulate large values
  via repeated `update_metadata` calls. A size guard should be added if
  this becomes a production concern.

## Alternatives Considered

- **Separate `progress_percent` / `progress_message` columns.** Simpler
  schema but inflexible — no room for handler-specific checkpoint data
  without another column.
- **External progress store (Redis).** Violates the Postgres-only
  principle (ADR-001).
- **Write progress on every `set_progress` call.** Too many round-trips
  for high-frequency updates. The buffered approach coalesces writes
  naturally.

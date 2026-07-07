# Troubleshooting

This guide covers the operational issues that come up most often in practice: stuck `running` jobs, leader-election delays, and heartbeat timeouts.

## Quick Checks

Start with these:

```bash
awa --database-url "$DATABASE_URL" queue stats
awa --database-url "$DATABASE_URL" job list --state running
awa --database-url "$DATABASE_URL" serve
```

The web UI is usually the fastest way to see queue depth, runtime health, and individual job details.

## Jobs Stuck In `running`

### What It Usually Means

A job staying in `running` is not automatically a bug. It usually means one of these:

- the handler is still genuinely working
- the process is draining during shutdown
- the worker stopped heartbeating and rescue has not fired yet
- the job hit a long deadline and is waiting for deadline rescue

### Inspect Running Jobs

In 0.6 with the default queue-storage backend, running attempts live in `{schema}.leases` (lease-materialized attempts) and unmaterialized short-job claims live in `{schema}.lease_claims`. The `{schema}` is `awa` unless the operator chose a different name; replace below as needed.

```sql
-- Materialized leases. heartbeat_at and deadline_at both populated.
SELECT
    job_id,
    queue,
    attempt,
    heartbeat_at,
    deadline_at,
    EXTRACT(EPOCH FROM (now() - heartbeat_at)) AS heartbeat_age_s,
    EXTRACT(EPOCH FROM (deadline_at - now())) AS deadline_remaining_s
FROM awa.leases
WHERE state = 'running'
ORDER BY heartbeat_at ASC;

-- Receipt-mode short-job claims that haven't materialized yet. No
-- heartbeat row; deadline_at lives on the claim if deadline_duration > 0
-- and the receipt-side deadline rescue (`rescue_expired_receipt_deadlines_tx`)
-- is what kicks in here. Anti-join with closure evidence to filter
-- "still open".
SELECT
    c.job_id,
    c.queue,
    c.attempt,
    c.claimed_at,
    c.deadline_at,
    EXTRACT(EPOCH FROM (now() - c.claimed_at)) AS claimed_age_s,
    EXTRACT(EPOCH FROM (c.deadline_at - now())) AS deadline_remaining_s
FROM awa.lease_claims c
WHERE NOT EXISTS (
    SELECT 1 FROM awa.lease_claim_closures cx
    WHERE cx.claim_slot = c.claim_slot
      AND cx.job_id = c.job_id
      AND cx.run_lease = c.run_lease
)
AND NOT EXISTS (
    SELECT 1 FROM awa.lease_claim_closure_batches cb
    WHERE cb.receipt_ranges @> c.receipt_id
)
ORDER BY c.claimed_at ASC;
```

Pre-0.6 / canonical-only deployments still query `awa.jobs` directly:

```sql
SELECT id, kind, queue, attempt, heartbeat_at, deadline_at
FROM awa.jobs
WHERE state = 'running'
ORDER BY heartbeat_at ASC;
```

Interpretation:

- low `heartbeat_age_s`: the worker is still alive
- `heartbeat_age_s` well above `heartbeat_staleness` (default `90s`): stale-heartbeat rescue should pick it up on the next scan
- very negative `deadline_remaining_s`: deadline rescue should pick it up soon (lease side or receipt side, depending on which table the row lives in)

### Inspect Runtime Health

```sql
SELECT
    instance_id,
    hostname,
    pid,
    last_seen_at,
    healthy,
    postgres_connected,
    poll_loop_alive,
    heartbeat_alive,
    maintenance_alive,
    leader,
    shutting_down
FROM awa.runtime_instances
ORDER BY leader DESC, last_seen_at DESC;
```

If `heartbeat_alive` or `maintenance_alive` is false for the active runtime, fix that before manually changing job state.

### Recovery Options

- if the job is still making progress, leave it alone
- if the worker is gone, wait for rescue to move it back to `retryable`
- if you need operator intervention now, cancel it:

```bash
awa --database-url "$DATABASE_URL" job cancel <job-id>
```

Then investigate why the worker stopped.

### Admin Cancel Doesn't Wake A Running Worker

`awa job cancel <id>` writes the cancellation to the database and emits `pg_notify('awa:cancel', {job_id, run_lease})`. Each worker runtime runs a `CancelListener` (PgListener-based) that picks up the notification and fires the matching `Arc<AtomicBool>` cancel flag the handler's `JobContext` holds — so the handler sees `ctx.is_cancelled() == true` on its next check and can stop cleanly.

If the listener fails to start or its connection drops, the listener logs a `warn!` and exits; admin cancels then silently fall back to heartbeat / deadline rescue for detection (i.e. the cancel does take effect on the next completion-time `run_lease` check, but the handler doesn't get the early wake-up). Symptoms: `awa job cancel` returns success and the database shows the job as `cancelled`, but a worker reports completing the cancelled `run_lease` long after the cancel was issued; the completion is rejected by `StaleCompleteRejected` so no harm done, just wasted work.

To diagnose:

- Search the worker logs for `Failed to create PG listener for admin cancel` or `Failed to LISTEN on cancel channel`. Either message means the listener started, hit the failure, and the runtime is now in fallback mode.
- Search for `PG cancel listener error; will retry` — the listener saw a transient error and is sleeping 1s before reconnecting; not a steady-state failure unless it spams the log.
- Confirm `pg_listener` works from outside the runtime: `psql -c "LISTEN \"awa:cancel\";"` and watch a separate session fire `SELECT pg_notify('awa:cancel', '{}');`.

To recover:

- Restart the worker process — the listener spawns at runtime startup, so restart re-creates the listener with a fresh connection.
- If the failure is steady (e.g. a Postgres pool sized so all connections are saturated by claim/complete traffic and `LISTEN` can't acquire one), increase the pool max or dedicate a connection to listening.

In all cases the cancel itself is durable — only the early wake-up is lost. Long-running handlers that don't poll `ctx.is_cancelled()` between heartbeats won't notice the cancel until they finish or heartbeat-rescue fires.

## Rescue Fails With `idx_awa_jobs_unique`

Symptom: maintenance repeats, every tick (canonical engine — the queue-storage
engine wedges identically but logs `error: unique conflict` instead):

```text
Failed to rescue stale heartbeat jobs, error: ... duplicate key value violates unique constraint "idx_awa_jobs_unique"
Failed to rescue deadline-expired jobs, error: ... duplicate key value violates unique constraint "idx_awa_jobs_unique"
```

Cause: a unique job whose `unique_states` mask excludes `running` was stuck running (holding no claim), a newer duplicate took the claim, and the rescue transition back into a claiming state conflicts. Since 0.6.1 both engines degrade row-at-a-time: the conflicted job is cancelled with a `rescued as duplicate` error entry (the claim holder wins) and everything else rescues normally, so the log appears at most once per conflicted job. On 0.6.0 the whole batched sweep aborted every tick — upgrade, or clear the conflict by hand:

```sql
SELECT j.id AS stuck_job, j.kind, j.queue, j.unique_key,
       c.job_id AS claim_holder, h.state AS holder_state
FROM awa.jobs_hot j
JOIN awa.job_unique_claims c ON c.unique_key = j.unique_key AND c.job_id <> j.id
LEFT JOIN awa.jobs h ON h.id = c.job_id
WHERE j.state = 'running';
```

Cancel the `stuck_job` (cancelled is outside the default mask, so the transition succeeds), or delete the claim row if its holder no longer exists.

Prevention: on the canonical engine, choose `unique_states` masks that are *closed under runtime transitions* — retry/rescue move `running -> retryable`, promotion moves `retryable -> available` and `scheduled -> available`. A mask a transition can enter from outside (e.g. `{scheduled, available, retryable}` without `running`) means any stuck-then-superseded job ends in a fallback cancellation. `{}` (no dedup) and the full non-terminal set `{scheduled, available, running, retryable}` are always safe; pair the full mask with a short `by_period` bucket if you need "a change during a run still gets a fresh run" semantics.

## Producer Enqueue Is Slower Than Expected

### What It Usually Means

Producers are enqueuing well below the rate the application offers, batches are taking longer than expected, or the consumer fleet is permanently undersaturated despite plenty of producer concurrency.

### Reading The Producer Histograms

The direct queue-storage COPY path (`QueueStorage::enqueue_params_copy` in Rust, `Client.enqueue_many_copy` in Python) records two histograms per batch:

| Metric                   | What it measures              |
| ------------------------ | ----------------------------- |
| `awa.enqueue.batch_size` | Rows per COPY call            |
| `awa.enqueue.duration`   | Wall-clock time per COPY call |

Reading them together separates the two usual failure modes:

- **Batches are tiny.** `batch_size` p50 sits at a handful of rows when the upstream flow is bursty or chunked too aggressively. Throughput is bounded by the per-batch round-trip cost, not the COPY itself. Increase chunk size or batch the upstream side.
- **Batches are slow.** `batch_size` p50 is reasonable (say ≥100) but `duration` p99 is high. The COPY itself is contended on the DB side; see the diagnoses below.

### Diagnoses

**1. Producer is going through the compatibility insert path.**

`insert_many_copy_from_pool` / `client.insert_many_copy` routes every row through `awa.insert_job_compat()` once per row. On any database with non-trivial per-statement latency (auth-proxy hop, managed Postgres, network) the per-row function call dominates — measured at ~100–150 ms per row through a Cloud SQL Auth Proxy in staging, which caps a single producer at ~7 rows/sec regardless of batch or chunk size.

Switch the producer to the direct queue-storage COPY entry point:

- Rust: `QueueStorage::enqueue_params_copy(pool, &jobs)`
- Python: `client.enqueue_many_copy(jobs)`

See [Producer path choice](configuration.md#producer-path-choice).

**2. `enqueue_shards` is `1` for the contended queue.**

The default `enqueue_shards = 1` means every producer contends on a single enqueue-head row per `(queue, priority)`. Multi-producer enqueue serialises through that row. Check the live value:

```sql
SELECT queue, enqueue_shards FROM awa.queue_meta WHERE queue = '<queue>';
```

If no row is returned the queue is running with the default `1`. Raise it explicitly (a 16-producer same-queue reference sweep measured 1.0× → 1.60× → 2.75× → 3.69× at `S = 1/2/4/8`):

```sql
INSERT INTO awa.queue_meta (queue, enqueue_shards)
VALUES ('<queue>', 4)
ON CONFLICT (queue)
DO UPDATE SET enqueue_shards = EXCLUDED.enqueue_shards;
```

Use an upsert: first-enqueue may create lane rows before any operator inserts a `queue_meta` row, so a plain `UPDATE` quietly affects zero rows. Raising `enqueue_shards` is a semantic switch from strict to partitioned FIFO; see [ADR-025](adr/025-sharded-enqueue-heads.md).

**3. WAL or commit pressure on the database.**

If batch size and shard count both look healthy, sample `pg_stat_activity` for `LWLock:WALWrite` / `LWLock:WALSync` waits and confirm the database is sized for the offered rate. The per-vCPU sustained-completion and burst-enqueue numbers in [`docs/deploying-on-managed-postgres.md`](deploying-on-managed-postgres.md#pick-a-vcpu-size) are useful reference points.

## Leader Election Delays

### Expected Behavior

Only one instance runs maintenance tasks at a time. Non-leaders retry every `10s` by default.

That means failover is not instant by default. A short delay after a pod restart or network blip is normal.

### What To Check

From the runtime snapshot table or your app health endpoint, confirm:

- at least one live instance exists
- in steady state, one live instance reports `leader = true`
- the leader also reports `maintenance_alive = true`

During failover there may be a brief window with no leader before the next election retry succeeds.

If no leader appears:

- verify Postgres connectivity from worker pods
- confirm the worker process actually called `start()`
- make sure the pool is not exhausted

### Tuning

If you want faster leader failover:

- Rust: lower `ClientBuilder::leader_election_interval(...)`
- Python: lower `leader_election_interval_ms=...` in `client.start(...)`

Use lower values carefully. Faster checks mean more advisory-lock traffic.

## Heartbeat Timeouts

### Defaults

Current runtime defaults:

- heartbeat interval: `30s`
- stale-heartbeat rescue tick: `30s`
- stale-heartbeat cutoff: about `90s`
- deadline duration: `5m`

### Common Causes

- the worker process crashed or was OOM-killed
- Postgres became unreachable
- the pod was terminated before graceful shutdown finished
- the pool was undersized and the runtime could not get connections reliably

For Python workers specifically, the heartbeat loop runs on the Rust runtime rather than the Python event loop. If heartbeats still stop, suspect process health or database connectivity first.

### Fixes

- increase container `terminationGracePeriodSeconds`
- make sure your app calls `shutdown(...)` on SIGTERM
- increase `deadline_duration` for legitimately long jobs
- increase pool size if the runtime is starved for DB connections

If long-running jobs are expected, remember:

- heartbeats keep the job alive
- they do not override `deadline_duration`

If a job really needs `20m`, set a longer deadline for that queue.

## Dead Tuples Growing In Queue Storage

### What It Usually Means

If queue throughput starts to sag while lease tables keep accumulating dead tuples, the usual cause is not duplicate dispatch. The more common pattern is:

- workers are still claiming and completing jobs
- the maintenance leader is still rotating and pruning
- one or more long-lived transactions on the primary are holding an old snapshot open or touching older segments
- prune cannot truncate old lease or terminal segments aggressively enough because the MVCC horizon is pinned or a reader still holds a lockable view of the segment

This is the same general failure mode described in PlanetScale's "Keeping a Postgres queue healthy" post.

### Find The Active Queue-Storage Schema

```sql
SELECT
    backend,
    schema_name,
    updated_at
FROM awa.runtime_storage_backends;
```

### Inspect Table Churn

Replace `<schema>` with the active `schema_name` from the previous query.

```sql
SELECT
    relname,
    n_live_tup,
    n_dead_tup,
    vacuum_count,
    autovacuum_count,
    last_vacuum,
    last_autovacuum
FROM pg_stat_user_tables
WHERE schemaname = '<schema>'
  AND (
      relname = 'attempt_state'
      OR relname = 'deferred_jobs'
      OR relname = 'dlq_entries'
      OR relname LIKE 'ready_segments_%'
      OR relname LIKE 'ready_entries%'
      OR relname LIKE 'ready_tombstones%'
      OR relname LIKE 'done_entries%'
      OR relname LIKE 'queue_terminal_count_deltas%'
      OR relname LIKE 'leases%'
  )
ORDER BY n_dead_tup DESC, relname;
```

Interpretation:

- `ready_entries%`, `ready_tombstones%`, and `queue_terminal_count_deltas%` should usually stay at or near zero dead tuples
- `ready_segments_*` is compact control-plane metadata; live rows should track retained ready lane ranges, and queue prune truncates reclaimed slot children with the rest of the ready family
- `leases%` can rise within the current rotation window, but should fall again after prune
- `attempt_state` should roughly match live long-running attempts, not total queue depth, and should return close to zero after drain
- `autovacuum_count` staying flat for a long time can indicate vacuum is not keeping up on churn-heavy lease partitions
- persistent dead tuples across many `leases%` tables usually means prune is blocked or the maintenance leader is unhealthy

### Inspect Long Transactions

```sql
SELECT
    pid,
    usename,
    application_name,
    client_addr,
    state,
    now() - xact_start AS xact_age,
    wait_event_type,
    query
FROM pg_stat_activity
WHERE datname = current_database()
  AND xact_start IS NOT NULL
ORDER BY xact_start ASC;
```

Pay particular attention to:

- sessions with large `xact_age`
- sessions in `idle in transaction`
- read-only / reporting tools that keep a snapshot open for a long time

### Recovery Options

- terminate or fix the long-lived transaction that is pinning the horizon
- move analytical reads to a replica
- shorten transaction scope in admin or reporting code
- confirm one maintenance leader is healthy; rotation and prune are leader-owned
- reduce terminal-row retention if the terminal history is much larger than needed; note that `failed_retention` is a floor — non-DLQ `failed` rows stay retryable for at least that long in both engines (queue storage carries in-floor failed rows forward at prune time), and the cumulative count of failed rows aged past the floor is visible as `QueueCounts.pruned_failed`
- review autovacuum settings if lease churn is expected continuously

If you want to reproduce the behavior locally before changing settings, run the MVCC benchmark documented in `docs/benchmarking.md`. Preventative guidance — reader placement, session timeouts, alerting, and autovacuum capacity flags — lives in [`deploying-on-managed-postgres.md`](deploying-on-managed-postgres.md#mvcc-discipline-long-running-readers-pin-the-whole-database).

## Something's In The DLQ

The Dead Letter Queue is where terminal failures land when a queue has DLQ enabled. These rows are never claimed. Operators retry them, purge them, or let retention remove them.

Quick checks:

```bash
awa --database-url "$DATABASE_URL" dlq depth
awa --database-url "$DATABASE_URL" dlq list --limit 20
awa --database-url "$DATABASE_URL" dlq list --queue email
```

`dlq_reason` tells you how the row got there:

- `terminal_error`
- `max_attempts_exhausted`
- `callback_timeout`
- an operator-supplied reason from `awa dlq move`

Useful actions:

```bash
awa --database-url "$DATABASE_URL" dlq retry <job-id>
awa --database-url "$DATABASE_URL" dlq retry-bulk --queue email
awa --database-url "$DATABASE_URL" dlq purge --queue email
awa --database-url "$DATABASE_URL" dlq move --queue email --reason backfill
```

If DLQ depth is growing quickly:

- inspect a few rows and compare `dlq_reason`
- sample `awa job dump <id>` for the full error/progress chain
- pause the upstream queue or producer if one failure mode is dominating
- tune `dlq_retention` or `RetentionPolicy.dlq` if forensic retention is too long for the incident volume

## Common Error Cases

### `SchemaNotMigrated`

Cause:

- application code is newer than the database schema

Fix:

```bash
awa --database-url "$DATABASE_URL" migrate
```

### `relation "awa.job_id_seq" does not exist`

Cause:

- A producer started before the queue-storage substrate had been materialised on a fresh database, or the schema was dropped under a running fleet.

`awa.job_id_seq` is part of the queue-storage substrate created by the runtime's `prepare_schema` step the first time a worker boots against a schema — not by `awa migrate` alone, which builds the canonical surface. Migrations + worker startup together produce a usable schema.

Fix:

- Wait for the first consumer pod to log `Awa worker runtime started` before scaling up producers.
- If the schema was deliberately reset, re-run `awa migrate` **and** let one worker pod boot to completion before resuming producer traffic.
- If the error persists after both, the runtime may be pointing at a different schema than the producer. Confirm with:
  ```sql
  SELECT awa.active_queue_storage_schema();
  ```

### `register at least one worker before starting the runtime`

Cause:

- `client.start()` was called before any worker registration

Fix:

- Rust: call `.register(...)` or `.register_worker(...)` before `.build()`
- Python: add `@client.worker(...)` handlers before `client.start(...)`

### `weighted mode requires explicit queue configs`

Cause:

- Python `client.start(global_max_workers=...)` was used without explicit queue config dicts

Fix:

```python
await client.start(
    [{"name": "email", "min_workers": 5, "weight": 2}],
    global_max_workers=20,
)
```

### Queue or job-kind shows "stale descriptor" in the UI

The descriptor catalog is refreshed by live workers on every runtime snapshot tick. A descriptor is flagged **stale** when no live runtime has touched `last_seen_at` within the snapshot window — typically because the code that declared it has been retired or no workers are currently running.

- If the queue/kind is genuinely retired, delete the row:
  ```sql
  DELETE FROM awa.queue_descriptors WHERE queue = 'retired-queue';
  DELETE FROM awa.job_kind_descriptors WHERE kind = 'retired_kind';
  ```
- If workers _should_ be running, check `/runtime` for missing instances.
- If the declaration moved to a different worker role that isn't deployed yet, the stale status will clear once that rollout completes.

### Queue or job-kind shows "descriptor drift" in the UI

Two or more live runtimes are reporting different BLAKE3 hashes for the same descriptor — i.e. they disagree on its fields.

- During a rolling deploy this is normal and clears when the old revision finishes draining.
- If it persists, two branches of your worker code are running simultaneously (e.g. a partial rollback or a split between the Rust and Python runtimes with different descriptor declarations). Reconcile the declaring code and re-deploy.

## When To Escalate

Escalate beyond normal operator actions when:

- `running` jobs are accumulating and no instance reports a healthy leader
- `last_seen_at` is stale for every runtime instance
- jobs repeatedly rescue and retry without making progress
- you need to change the schema state manually

At that point, inspect worker logs, Postgres health, and recent deployment events together.

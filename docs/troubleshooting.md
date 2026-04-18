# Troubleshooting

This guide focuses on the operational issues called out in issue `#16`: stuck `running` jobs, leader-election delays, and heartbeat timeouts.

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

```sql
SELECT
    id,
    kind,
    queue,
    attempt,
    heartbeat_at,
    deadline_at,
    EXTRACT(EPOCH FROM (now() - heartbeat_at)) AS heartbeat_age_s,
    EXTRACT(EPOCH FROM (deadline_at - now())) AS deadline_remaining_s
FROM awa.jobs
WHERE state = 'running'
ORDER BY heartbeat_at ASC;
```

Interpretation:

- low `heartbeat_age_s`: the worker is still alive
- `heartbeat_age_s` well above `90`: stale-heartbeat rescue should pick it up soon
- very negative `deadline_remaining_s`: deadline rescue should pick it up soon

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

## Dead Tuples Growing On `awa.jobs_hot`

### What It Usually Means

If queue throughput starts to sag while `awa.jobs_hot` keeps accumulating dead
tuples, the usual cause is not duplicate dispatch or a broken worker. The more
common pattern is:

- workers are still claiming and completing jobs
- the maintenance leader is still running cleanup
- one or more long-lived transactions on the primary are holding an old
  snapshot open
- vacuum cannot reclaim dead rows aggressively enough because the MVCC horizon
  is pinned

This is the same general failure mode described in PlanetScale's "Keeping a
Postgres queue healthy" post.

### Inspect Table Churn

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
WHERE schemaname = 'awa'
  AND relname IN ('jobs_hot', 'scheduled_jobs')
ORDER BY relname;
```

Interpretation:

- rising `n_dead_tup` on `jobs_hot` with steady worker activity means churn is happening
- `autovacuum_count` staying flat for a long time can indicate vacuum is not keeping up
- `scheduled_jobs` is usually not the problem here; `jobs_hot` is the churn table

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
- reduce terminal-row retention if the table is much larger than needed
- review autovacuum settings if high churn is expected continuously

If you want to reproduce the behavior locally before changing settings, run the
MVCC benchmark documented in `docs/benchmarking.md`.

## Common Error Cases

### `SchemaNotMigrated`

Cause:

- application code is newer than the database schema

Fix:

```bash
awa --database-url "$DATABASE_URL" migrate
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
- If workers *should* be running, check `/runtime` for missing instances.
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

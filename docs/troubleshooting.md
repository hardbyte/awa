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

## When To Escalate

Escalate beyond normal operator actions when:

- `running` jobs are accumulating and no instance reports a healthy leader
- `last_seen_at` is stale for every runtime instance
- jobs repeatedly rescue and retry without making progress
- you need to change the schema state manually

At that point, inspect worker logs, Postgres health, and recent deployment events together.

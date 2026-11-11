# Configuration

AWA has three configuration surfaces: the **Rust runtime** (`ClientBuilder` + `QueueConfig`), the **Python runtime** (`client.start()`), and the **CLI** (`awa serve`, `awa job`, etc). This guide explains how they work rather than listing every option — use `--help`, IDE autocomplete, or the source for exhaustive reference.

Terms used throughout this guide:

- A **queue** is the worker subscription and capacity boundary.
- A **claim** is the act of reserving a ready job for one attempt.
- A **claim lease** (`run_lease`) is the monotonically increasing attempt guard on a job; stale completions with an old value are rejected.
- A **scheduled job** has a future `run_at` and waits outside the ready claim path. Retry backoff and snooze use the same deferred backlog until their `run_at` is due.
- A **lane** is one ordered `(queue, priority, enqueue_shard)` stream.
- A **segment** is a rotating partition that Awa eventually truncates.
- A **receipt** is the lightweight short-attempt claim record; a **lease row** is materialized when an attempt needs mutable execution state.

## How configuration flows

```
┌────────────────────────────────────┐
│  Worker process (Rust or Python)   │
│  ─ QueueConfig per queue           │
│  ─ ClientBuilder for runtime knobs │
│  ─ Connects directly to Postgres   │
└──────────────┬─────────────────────┘
               │
          PostgreSQL
               │
┌──────────────┴─────────────────────┐
│  awa serve  (admin UI + API)       │
│  ─ CLI flags / AWA_* env vars      │
│  ─ Read-only safe (auto-detected)  │
└────────────────────────────────────┘
```

Workers and the UI server are separate processes. Workers own all queue machinery — the UI is a read-mostly dashboard with optional admin actions.

## Worker scope: which queues and which kinds

A single worker process handles work for the **set of queues it was configured with** and the **set of job kinds it registered handlers for**. There is no implicit fan-out across queues, and no implicit filter on kinds within a queue — these are two separate concerns.

### Targeting specific queues

A worker only claims jobs from queues passed to its builder; everything else on the database is invisible to it. To run a worker dedicated to one queue, only declare that queue:

```rust
// Rust: this worker only ever processes the "email" queue.
let client = Client::builder(pool)
    .queue("email", QueueConfig::default())
    .register::<SendEmail, _, _>(handle_email)
    .build()?;
client.start().await?;
```

```python
# Python: same idea — only "email" is in the start list.
@client.task(SendEmail, queue="email")
async def handle(job): ...

await client.start([("email", 8)])
```

Run separate worker processes (or separate fleets) per queue when you want **isolation**: a stuck `etl` queue can't starve `email`, deployment of a slow handler doesn't pause unrelated queues, and per-queue scaling is just a deployment knob. Run **one worker process across multiple queues** with `global_max_workers` and weighted mode when you want elastic capacity sharing — see [Weighted mode](#weighted-mode).

### Targeting specific job kinds within a queue

`register::<SomeJob, _, _>` (Rust) or `@client.task(SomeJob, ...)` (Python) tells the worker how to execute one specific job kind. At execute time the worker looks up the kind on the claimed job and runs the matching handler.

**Sharp edge:** the claim path is per-queue, not per-kind. If a queue holds jobs of kinds the worker didn't register, the worker still claims those jobs (it can't tell ahead of time) and then **fails them terminally** with `unknown job kind: <name>`. There is no soft re- enqueue. So the supported patterns for "this worker only handles a subset of kinds" are:

1. **Recommended: put each kind set on its own queue.** Queues are cheap; this is the operator-shaped boundary. Workers with different kind responsibilities subscribe to different queues, and the queue boundary becomes the routing decision.
2. **Acceptable: every worker on the queue registers handlers for every kind that lands there.** Heterogeneous workloads on a single queue work fine as long as no worker is missing a registration.
3. **Not supported: have some workers on a queue claim jobs and silently leave kinds they don't know for someone else.** This will terminal-fail or DLQ jobs.

If kinds drift out of sync (e.g. a deploy lags), the descriptor catalog in the admin UI flags missing handlers; see [Queue and job-kind descriptors](#queue-and-job-kind-descriptors) below.

## Queue configuration

Every queue needs a `QueueConfig`. The two fundamental choices are:

1. **Hard-reserved mode** (default) — each queue gets a fixed `max_workers` slot count
2. **Weighted mode** — call `global_max_workers(N)` to share a pool, with `min_workers` as a floor and `weight` for overflow

### Rust

```rust
let client = Client::builder(pool.clone())
    .queue("email", QueueConfig {
        max_workers: 20,
        rate_limit: Some(RateLimit { max_rate: 50.0, burst: 50 }),
        ..Default::default()
    })
    .queue("reports", QueueConfig {
        max_workers: 5,
        deadline_duration: Duration::from_secs(600),
        ..Default::default()
    })
    .register::<SendEmail, _, _>(handle_email)
    .register::<GenerateReport, _, _>(handle_report)
    .build()?;
```

The key `QueueConfig` fields:

| Field | Default | When you'd change it |
| --- | --- | --- |
| `max_workers` | `50` | Always — this is your concurrency cap per queue |
| `rate_limit` | `None` | External API rate limits, backpressure |
| `deadline_duration` | `5m` | Hard upper bound on a single attempt. Set to `Duration::ZERO` to skip the deadline rescue path; receipts mode (the 0.6 default storage) supports both shapes — the deadline lands on `lease_claims.deadline_at` and the maintenance rescue path force-closes expired claims. |
| `poll_interval` | `200ms` | Tune if NOTIFY latency matters (rare) |
| `min_workers` / `weight` | `0` / `1` | Only in weighted mode |
| `claimers` | `1` | Hot queue-storage queues that need more than one dispatcher/claimer loop inside a single runtime. Claimers share the queue's worker permits. |
| `claim_batch_size` | `512` | Maximum jobs each dispatcher tries to claim in one DB round-trip. Lower this for latency-sensitive small queues; benchmark before combining large batches with multiple claimers. |

Defaults intentionally favor the smallest blast radius: `enqueue_shards = 1`, `claimers = 1`, and `claim_batch_size = 512`. Raise `enqueue_shards` only when the queue can accept partitioned FIFO semantics. For a single hot queue, a larger claim batch usually helps before extra claimers: it reduces claim round-trips without adding more concurrent head coordinators. Benchmark `claimers = 2` or `4` only when a single claimer cannot keep worker permits full.

A runtime with many queues or claimers also bounds how many claim round-trips it runs at once: `ClientBuilder::max_concurrent_claims` (default: a quarter of the pool, clamped to `4..=64`). Claims that find the gate full wait for a slot rather than opening another transaction, which keeps a large pool from turning hundreds of concurrently polling queues into Postgres lock-manager contention.

With LISTEN available, consecutive empty polls back off to a 2 s cap (or `poll_interval` when larger). Notifications reset the backoff; periodic polling still recovers lost notifications. Poll-only runtimes retain `poll_interval`. The cap is internal, preserving existing `QueueConfig` struct literals. Shutdown bounds dispatcher exit and worker drain by the same timeout; a claim interrupted at expiry is recovered through the normal lease-rescue path.

### Partitioned Queues

A logical queue is the application concept: for example `email` or `customer-updates`. A physical queue is the queue name Awa stores in Postgres and workers claim from. Most applications use one physical queue per logical queue.

Very hot workloads can partition one logical queue over multiple physical queues when the ordering contract allows it. This creates independent durable claim and completion streams. It is the most direct way to reduce hot-head coordination without weakening durability: each job is still written, claimed, leased, completed, retried, and rescued through the normal Awa tables.

Rust producers and workers can share `PartitionedQueue`:

```rust
use awa::{Client, InsertOpts, QueueConfig, PartitionedQueue};

let customer_updates = PartitionedQueue::new("customer-updates", 4)?;

let client = Client::builder(pool.clone())
    .partitioned_queue(&customer_updates, QueueConfig {
        // Applied per physical queue: 4 × 32 hard-reserved workers.
        max_workers: 32,
        ..Default::default()
    })
    .register::<UpdateCustomer, _, _>(handle_update)
    .build()?;

let opts = customer_updates.route_opts_by_key(
    InsertOpts::default(),
    format!("customer-{customer_id}").into_bytes(),
);
```

With one partition, `PartitionedQueue::new("email", 1)` uses the plain `email` queue. With more than one partition, partition 0 remains the logical queue name and later partitions use stable suffixes: `email`, `email__p1`, `email__p2`, and so on. Key-based routing sets both the selected physical queue and `InsertOpts::ordering_key`, so related jobs keep per-key FIFO inside the selected partition even if each physical queue later raises `enqueue_shards`.

`partitioned_queue` applies the `QueueConfig` to each physical queue. In hard-reserved mode, total logical capacity is roughly `partitioned_queue.partitions() * max_workers`; `rate_limit` is also per physical queue. Divide those values yourself, or use `global_max_workers`, when you need a logical total cap across the partitioned queue.

Python exposes the same deterministic router through the `awa.PartitionedQueue` class:

```python
queue = awa.PartitionedQueue("customer-updates", 4)

@client.task(UpdateCustomer, queue=queue.physical_queues[0])
async def update_customer(job):
    ...

await client.start(
    queue.queue_configs(
        max_workers_per_partition=16,
        claim_batch_size=512,
    )
)

await client.insert(
    UpdateCustomer(customer_id=customer_id, payload=payload),
    **queue.route_by_key(f"customer-{customer_id}"),
)
```

Register the handler once and pass explicit partition queue configs to `start()`. Python handlers are dispatched by job kind; the queue name on `@client.task` gives `start()` a declared queue to validate.

`route_by_key()` returns `{"queue": ..., "ordering_key": ...}` and can be passed directly to `insert()`. For `insert_many_copy()` and `enqueue_many_copy()`, pass `opts=[queue.route_by_key(key) for ...]` when a batch contains jobs for multiple partitions. `route_by_index()` returns only a queue for round-robin partitioning when per-key FIFO is not needed.

### Choosing a throughput lever

These knobs solve different bottlenecks:

| Knob | What changes | Use it when |
| --- | --- | --- |
| `PartitionedQueue` | Routes one logical workload across multiple physical queues. Each physical queue has its own queue-level capacity, rate limit, claim cursor, completion stream, and metrics. | The workload can be partitioned by key or round-robin, and you want more end-to-end throughput from independent queue coordination paths. |
| `enqueue_shards` | Keeps one physical queue name, but splits that queue into multiple ordered lanes. FIFO becomes per `(queue, priority, enqueue_shard)` instead of global per `(queue, priority)`. | Operators should still see one queue, but the workload can accept partitioned FIFO inside that queue. |
| `claimers` | Adds more dispatcher/claimer loops for the same physical queue inside one worker runtime. They share the queue's worker permits and rate limiter. | One claimer is leaving worker permits idle. This is not the first lever for a hot queue because extra claimers can add transaction pressure without creating independent capacity domains. |

For a hot workload that can be partitioned, start with `PartitionedQueue`. If the public queue should stay singular, consider `enqueue_shards`. Raise `claimers` only when measurements show the runtime is claim-starved rather than Postgres-bound.

### Python

Tuple form for simple cases, dict form for full control:

```python
# Hard-reserved — just (name, max_workers)
await client.start([("email", 10), ("reports", 5)])

# Dict form — rate limiting, deadlines, priority aging, retention
await client.start([
    {"name": "email", "max_workers": 10, "rate_limit": (50.0, 50)},
    {
        "name": "reports",
        "max_workers": 5,
        "deadline_duration_ms": 600_000,
        "priority_aging_interval_ms": 30_000,
        "retention": {"completed_hours": 12, "failed_hours": 168},
    },
])
```

Weighted mode requires dict form and `global_max_workers`:

```python
await client.start(
    [{"name": "email", "min_workers": 5, "weight": 2},
     {"name": "reports", "min_workers": 2, "weight": 1}],
    global_max_workers=20,
)
```

### Weighted mode

Enabled by `global_max_workers(N)` (Rust) or `global_max_workers=N` (Python). Each queue's `min_workers` is guaranteed; remaining capacity is distributed by `weight`. This is useful when queue load is unpredictable and you want elastic sharing rather than static partitioning.

### What can be tuned per queue vs per job

Queue configuration is the normal way to express operational policy. Job kinds are primarily handler registrations and descriptor/catalog entries; if two kinds need different runtime policy, put them on different queues.

| Scope | Knobs | Notes |
| --- | --- | --- |
| Per queue | `max_workers`, `rate_limit`, `deadline_duration`, `priority_aging_interval`, `poll_interval`, `min_workers`, `weight` | Runtime dispatch policy. `deadline_duration` is the hard per-attempt wall-clock timeout for every job claimed from that queue. |
| Per job enqueue | `queue`, `priority`, `max_attempts`, `run_at`, `tags`, `metadata`, `unique` | Stored with the job. Use this for routing, priority, retry budget, scheduling, identity, and operator context. |
| Per job kind | handler registration plus `JobKindDescriptor` | Descriptors cover display name, owner, docs link, tags, and extra metadata. They do not set dispatch limits or timeouts. |
| Per callback wait | callback `timeout` | Applies only to jobs that enter `wait_for_callback`; see [Callback timeout](#callback-timeout--bounding-wait_for_callback). |
| Per queue / global retention and DLQ policy | `completed_retention`, `failed_retention`, `queue_retention(...)`, `dlq_enabled_by_default`, `queue_dlq_enabled`, `dlq_retention` | Retention has global defaults with per-queue overrides. DLQ enablement is queue-scoped. Split job kinds across queues if only some kinds should DLQ. |
| Per runtime / fleet | heartbeat timings, rescue scan intervals, cleanup batch size, descriptor retention, queue-storage slots/stripes/rotation | Process or storage-engine policy, not job-kind policy. `queue_storage_queue_stripe_count` currently applies to the queue-storage engine rather than to one named queue. |

There is no active per-job-kind hard timeout today. The hard timeout is the queue's `deadline_duration`; moving long-running kinds onto their own queue is the intended way to give them a different timeout without making every job on a busy queue slower to rescue.

## Scheduled jobs and deferred promotion

Set `run_at` when enqueueing a job that should not be claimable yet. Awa stores that row in the deferred backlog and the maintenance leader promotes it to the ready ring once `run_at <= now()`.

Retry backoff and `Snooze` use the same mechanism: the current attempt closes, a deferred row is written with its next `run_at` (`retryable` for backoff and `RetryAfter`, `scheduled` for a snooze), and promotion makes it ready again later. Cron schedules are producers on top of the same enqueue path; a cron fire atomically records the schedule fire and inserts the resulting job.

Operational knobs:

- `promote_interval` controls how often maintenance checks for due deferred rows.
- A queue whose enqueue lane is kept busy by producers does not delay promotion of other queues or the leader's rescue: promotion waits 50 ms for a lane's lock, then hands the lane to a dedicated waiter that is served in turn. Each waiter holds one connection from the worker's pool while it waits, on top of the leader's own: its lock session, one connection for each of its two maintenance loops, and one for the cron evaluator. The leader runs at most one waiter per eight pool connections, never fewer than one or more than four, so a 10-connection pool gives up at most one extra connection to a busy lane. `awa_maintenance_promote_lane_contention_total` counts those hand-offs by queue. A steady `waiter_limit` rate means more lanes are saturated than the waiters cover, and a larger pool allows more waiters. `waiter_timed_out` means a waiter's promotion transaction waited longer than 30 s for one of its locks, usually the lane lock behind the producers queued ahead of it.
- Per-job `run_at` controls the due time.
- Periodic schedules are declared in worker code with `periodic()` and managed through the CLI/UI `cron` surface.

## Job priority and aging

Every job carries a **priority** (`i16`, lower number = higher priority). The default is `2`. Conventional usage:

| Priority | Typical meaning                         |
| -------- | --------------------------------------- |
| `1`      | Urgent / customer-facing / SLA-critical |
| `2`      | Default                                 |
| `3`      | Background work                         |
| `4`      | Batch / catch-up / bulk reprocess       |

Values outside `1..=4` are rejected by the insert path.

Priority enters the queue at insert time:

```rust
// Rust
awa::insert_with(
    &pool,
    &SendEmail { to: "alice@example.com".into(), subject: "Welcome".into() },
    awa::InsertOpts {
        queue: "email".into(),
        priority: 1, // urgent
        ..Default::default()
    },
).await?;
```

```python
# Python — kwargs on insert
await client.insert(
    SendEmail(to="alice@example.com", subject="Welcome"),
    queue="email",
    priority=1,
)
```

### Priority aging (escalation for fairness)

Without aging, a steady stream of priority-1 work can starve every priority-2 job behind it. AWA escalates priority over time — the longer a job has been waiting, the higher (numerically lower) its **effective priority** becomes at claim time. A priority-4 job that has waited `3 × aging_interval` ages all the way down to priority 1 and is no longer starvable.

The cadence is per-queue via `QueueConfig.priority_aging_interval` (default `60s`):

```rust
// Rust
.queue("etl", QueueConfig {
    max_workers: 8,
    // Drop one priority level every 30 seconds of waiting.
    priority_aging_interval: Duration::from_secs(30),
    ..Default::default()
})
```

```python
# Python — same semantics, dict form
await client.start([
    {
        "name": "etl",
        "max_workers": 8,
        "priority_aging_interval_ms": 30_000,
    }
])
```

Aging is computed at claim time on queue-storage runtimes: ready rows keep their stored lane priority, and the claim SQL compares candidate lanes by effective priority. When a job is claimed, the live attempt records the effective priority used for that claim. In canonical storage, the maintenance leader physically rewrites waiting rows from priority N to N-1 and preserves the enqueue priority in `metadata._awa_original_priority`.

The admin UI shows the priority on the current job snapshot and, when `_awa_original_priority` is present, the original enqueue priority as `(enqueued as N)`. Set the value to `Duration::ZERO` (Rust) or `priority_aging_interval_ms: 0` (Python) to disable escalation entirely (strict static priority).

There is a separate top-level `ClientBuilder::priority_aging_interval` that controls the legacy canonical-storage maintenance pass that physically rewrites stored priorities. With queue storage (the 0.6 default) it is a no-op; the per-queue setting above is the one to tune.

### Changing priority after enqueue

Queued jobs can be reprioritized through durable batch operations. In queue storage, priority is part of the physical lane key (`queue`, `priority`, `enqueue_shard`, `lane_seq`), so changing an already-queued job's priority means tombstoning the old lane and appending a replacement ready row with a fresh lane sequence. That is a semantic operation, not a metadata update.

Use one of these patterns:

- Choose the priority when inserting the job, periodic schedule, or direct-COPY batch.
- Tune `priority_aging_interval` when the goal is fairness under sustained high-priority load.
- For DLQ recovery, use `retry_from_dlq(..., priority=...)` (Python) or `RetryFromDlqOpts { priority: Some(...) }` (Rust/model API) to revive a DLQ row at a different priority.
- For pending `available` or `scheduled` jobs, submit `op_kind = "set_priority"` through `/api/batch-ops` or `awa batch-ops submit`. The operation snapshots matching job ids, previews/counts the eligible population, and then applies changes asynchronously from the maintenance leader.

Plain `retry` of a failed/cancelled/waiting job keeps the job's existing priority. Running, waiting, terminal, and DLQ rows are not reprioritized by batch operations; use cancel/retry semantics if an in-flight attempt must be interrupted.

```bash
awa batch-ops preview \
  --op-kind set_priority \
  --filter '{"queue":"default"}' \
  --spec '{"priority":1}'

awa batch-ops submit \
  --op-kind set_priority \
  --filter '{"queue":"default"}' \
  --spec '{"priority":1}'
```

Python clients expose the same durable control-plane operation. Async and sync clients use the same method names; omit `await` when using `awa.Client`:

```python
preview = await client.preview_set_priority(
    1,
    filter={"queue": "default", "state": "available"},
)
print(preview["total_matched"])

operation = await client.set_priority(
    1,
    filter={"queue": "default"},
    submitted_by="ops@example.com",
)

operation = await client.cancel_batch_operation(operation["id"])
```

The generic Python envelope is also available when you want exact parity with the HTTP API:

```python
operation = await client.submit_batch_operation(
    "move_queue",
    spec={"queue": "escalations", "priority": 1},
    filter={"tag": "incident-123"},
)
```

Batch-operation history is retained for `AWA_BATCH_OP_RETENTION_DAYS` days (default `90`). Use `awa batch-ops purge --before <timestamp>` for explicit cleanup of finalized operations.

## Queue and job-kind descriptors

Queues and job kinds can carry operator-facing metadata: display names, descriptions, owners, docs links, tags, and arbitrary JSON `extra`. This is separate from runtime scheduling config and drives the labels the admin UI / API surface.

The runtime catalogs and propagates these — see [Architecture → Descriptors And Runtime Liveness](architecture.md#descriptors-and-runtime-liveness) for how sync, staleness, and drift detection work.

### Rust

```rust
use awa::{Client, JobArgs, JobKindDescriptor, QueueConfig, QueueDescriptor};

let client = Client::builder(pool)
    .queue("email", QueueConfig::default())
    .queue_descriptor(
        "email",
        QueueDescriptor::new()
            .display_name("Email")
            .description("Transactional outbound email")
            .owner("messaging")
            .tag("customer-facing"),
    )
    .job_kind_descriptor::<SendEmail>(
        JobKindDescriptor::new()
            .display_name("Send email")
            .description("Deliver a single transactional email"),
    )
    .register::<SendEmail, _, _>(handle_email)
    .build()?;
```

### Python

```python
client = awa.AsyncClient(database_url)

@client.task(SendEmail, queue="email")
async def handle(job):
    ...

client.queue_descriptor(
    "email",
    display_name="Email",
    description="Transactional outbound email",
    owner="messaging",
    tags=["customer-facing"],
)
client.job_kind_descriptor(
    "send_email",
    display_name="Send email",
    description="Deliver a single transactional email",
)

await client.start([("email", 8)])
```

Both surfaces must be called before `start()` / `build()`. Declaring a descriptor for a queue the client doesn't run is an error, so dead references show up at startup instead of silently producing stale rows.

## Maintenance leader election { #leader-election }

One runtime at a time is the maintenance leader. It promotes scheduled and retryable jobs, rescues stale heartbeats and expired deadlines and callbacks, rotates and prunes queue storage, cleans up retained rows and evaluates cron schedules. Leadership is a PostgreSQL session advisory lock: every runtime tries to take it, the one that does leads until its session ends, and the others retry.

| Setting | Default | Effect |
| --- | --- | --- |
| `ClientBuilder::leader_election_interval` (Rust) / `leader_election_interval_ms` (Python) | `10s` | How often a runtime that does not lead retries the lock. Lower values fail over faster. |
| `ClientBuilder::leader_check_interval` (Rust) | `30s` | How often the leader checks that its lock session is still alive. |
| `ClientBuilder::maintenance_leader` (Rust) / `maintenance_leader` (Python) | `true` | Whether the runtime contends for leadership at all. |

The intervals change how quickly leadership moves, not where it lands: any runtime can win the lock. To keep leadership off some deployments, such as GPU or bulk workers whose pools are sized for their jobs, start them with `maintenance_leader(false)`:

```rust
let client = Client::builder(pool)
    .queue("bulk", QueueConfig::default())
    .register::<BulkExport, _, _>(handle_bulk_export)
    .maintenance_leader(false)
    .build()?;
```

```python
await client.start([("bulk", 8)], maintenance_leader=False)
```

A runtime started this way never tries the lock, even when it is the only runtime. Everything else it does is unchanged: it claims and executes jobs, heartbeats them, publishes runtime snapshots, reports runtime-control observations and syncs its own periodic job declarations to `awa.cron_jobs`, where whichever runtime leads evaluates them.

- **Keep at least one runtime that may lead.** With none live, nothing promotes scheduled or retryable jobs, rescues abandoned attempts or rotates queue storage; jobs that are already available still run. An opted-out runtime logs a warning when it sees no other live runtime that may lead, and records the count it sees as `awa.maintenance.leader_eligible_runtimes`. Every runtime records `awa.maintenance.leader_eligible` (`1` or `0`).
- **Register rescue follow-ups on the runtimes that may lead.** Rescue runs on the leader, so `Rescued` lifecycle hooks and `Rescued` enqueue specs fire from the leader's registrations. One registered only on an opted-out runtime never fires. This already holds for any runtime that is not leading; opting out makes it permanent.
- **Keep periodic job declarations identical across runtimes that declare the same name.** Every opted-out runtime writes its own declarations each `cron_sync_interval`, as the leader does. An unchanged declaration writes nothing, but two runtimes that declare one name differently, as during a rolling deploy that changes a schedule, overwrite each other until the old runtimes are gone.
- **Configure maintenance on the runtimes that may lead.** Settings that only the leader uses, such as retention, rescue intervals, per-queue retention overrides and the queue-storage prune lock timeout, have no effect on an opted-out runtime.
- **Reporting needs the `runtime_maintenance_eligibility` schema patch.** `awa migrate` adds the `awa.runtime_maintenance_eligibility` table, which opted-out runtimes write to and `awa control status`, the admin API (`leader_eligible_instances`, and `maintenance_leader` per instance) and the admin UI read. The runtime role needs `SELECT, INSERT, DELETE` on it, which the default privileges in [security](security.md) grant. Without the patch, or without the grants, an opted-out runtime still never leads but is shown as eligible, and it logs a warning at startup. Runtimes from before the option always contend and are shown as eligible.

## Reliability timings: heartbeat, deadline, rescue

These knobs control **how fast a stuck or crashed handler is noticed and rescued**. They live on `ClientBuilder` (Rust) and `client.start()` kwargs (Python). All of them have `_ms`-suffixed kwargs on the Python side (e.g. `heartbeat_interval_ms=15000`).

### Heartbeat — detecting crashed workers

A running job updates a heartbeat row periodically while its handler is alive. If the heartbeat goes stale, the maintenance leader rescues the job. The rescued attempt counts against `max_attempts`: the job is re-enqueued for another attempt, or fails (and moves to the DLQ when the queue has it enabled) if that was its last. Three knobs participate:

| Knob | Default | What it does |
| --- | --- | --- |
| `heartbeat_interval` | `30s` | How often each running handler refreshes its heartbeat row. |
| `heartbeat_staleness` | `90s` | How long the row may go un-refreshed before the maintenance leader treats the job as crashed. |
| `heartbeat_rescue_interval` | `30s` | How often the maintenance leader scans for stale heartbeats. |

Pick them in this order:

1. **Decide your detection target.** "Crashes should be noticed within X seconds." That target is roughly `heartbeat_staleness + heartbeat_rescue_interval` in the worst case.
2. **Set `heartbeat_staleness` to at least `3× heartbeat_interval`.** The 3× rule absorbs scheduler hiccups, GC pauses, and the rescue scan's own jitter; tighter ratios produce false rescues. The builder logs a warning if you violate it.
3. **`heartbeat_rescue_interval`** can match `heartbeat_interval` for low-latency rescue, or be higher to reduce maintenance load on big fleets.

For a 5-second crash detection target: `heartbeat_interval=1s`, `heartbeat_staleness=4s`, `heartbeat_rescue_interval=1s`. For a 1-minute target with the cheapest possible maintenance: keep all the defaults.

### Deadline — bounding a single attempt

Each queue has a `deadline_duration` (default `5m` on `QueueConfig`). At claim time the runtime stamps `now() + deadline_duration` onto the claim, and a maintenance scan force-closes attempts that pass it without completing. This bounds a single attempt's wall-clock time independently of heartbeats — if a handler is hanging, looping forever, or wedged in a sync wait, the deadline rescues it even if its heartbeat is fresh. A rescued attempt counts against `max_attempts` like a failed one, so a job that hangs every time ends in `failed` or the DLQ.

A handler that ignores cancellation and is still running two `deadline_rescue_interval`s (one minute by default) past its deadline is dropped at its next `.await`, which returns its worker slot so the rescued retry can run. Code that never yields, such as a blocking call or a CPU loop on the async runtime, cannot be dropped and keeps its slot until it returns. A Python handler's worker slot is freed the same way, but its coroutine is not cancelled: it keeps running on its event loop until it finishes on its own, so while it runs the process can have more handlers executing than `max_workers`. Python handlers should enforce their own timeouts, for example with `asyncio.timeout`.

| Knob | Default | What it does |
| --- | --- | --- |
| `QueueConfig.deadline_duration` (Rust) / `deadline_duration_ms` (Python dict form) | `5m` | Per-queue hard upper bound on one attempt. `Duration::ZERO` / `0` skips deadline rescue for that queue. |
| `ClientBuilder::deadline_rescue_interval` (Rust) / `deadline_rescue_interval_ms` (Python kwarg) | `30s` | How often the maintenance leader scans for expired deadlines. |

Receipts mode (the 0.6 default storage) supports both shapes: the deadline lands on `lease_claims.deadline_at` and is rescued there for short claims, or carried onto `leases.deadline_at` if the claim materializes for a long-running attempt. See [Queue storage tuning](#queue-storage-tuning) and ADR-023.

### Callback timeout — bounding `wait_for_callback`

If you suspend a handler with `wait_for_callback()` and the external system never resumes, a callback-timeout rescue brings the job back to ready (or DLQ if attempts are exhausted).

| Knob | Default | What it does |
| --- | --- | --- |
| `ClientBuilder::callback_rescue_interval` | `30s` | How often the maintenance leader scans for `callback_timeout_at < now()`. The per-callback timeout itself is set when registering the callback in the handler. |

### Draining a backlog of abandoned claims { #rescue-backlog }

A runtime that is killed (OOM, `SIGKILL`, a lost node) leaves every attempt it had claimed open. On queue storage those open claims also stop the claim ring from rotating back into their slot, so a large backlog has to be rescued quickly, not 500 attempts per rescue interval.

Each stale-heartbeat, deadline or callback-timeout rescue therefore runs as a pass of batches. A batch scans a bounded claim window and rescues up to 500 attempts per source (lease rows and receipt claims) in its own short transaction. The pass keeps starting batches while a scan window or rescue batch came back full, until its time budget is spent or it has rescued `rescue_pass_max_rows`. While a backlog remains, the next pass of that kind runs one second later instead of after its interval, so a drain proceeds at up to about `rescue_pass_max_rows` attempts per second per rescue kind.

| Knob | Default | What it does |
| --- | --- | --- |
| `ClientBuilder::rescue_pass_budget` (Rust) / `rescue_pass_budget_ms` (Python kwarg) | `1s` | How long one pass keeps starting batches. Capped at half the rescue interval. `Duration::ZERO` / `0` keeps one batch per rescue interval with no early passes, as in 0.6.15. |
| `ClientBuilder::rescue_pass_max_rows` (Rust) / `rescue_pass_max_rows` (Python kwarg) | `5000` | How many attempts one pass rescues before it stops. Bounds the write burst (closures, retry rows, WAL, replica and CDC traffic), with up to one final batch beyond the cap. `0` keeps one batch per rescue interval with no early passes, as in 0.6.15. |

In practice the database sets the rate, not the cap. On PostgreSQL 17, a batch of 500 row-local claims (claims with a deadline) spends about 0.1–0.2 s in its rescue statements and writes about 0.3 MB of WAL for its closures, so a default pass takes several batches before its second of rest. Rescue kinds run one after another on the leader, not in parallel. Measured with default intervals, 1.6 million abandoned row-local claims past their deadlines drained in about 17 minutes (1,600 a second), against about 33 a second (500 per kind per interval) without passes. Raising `rescue_pass_budget` towards its cap and `rescue_pass_max_rows` keeps rescue running for more of each second, but each pass holds the leader for its whole budget, so promotion, rotation and prune wait as long. After a large drain the rings take a minute or two to move past the slots the backlog pinned, while promotion admits the rescued retries.

How a pass chooses and paces its work:

- **Oldest claims first.** Batches visit claim slots from the oldest to the current one, because the ring cannot rotate back into the oldest slot until its claims are closed. Every fourth batch visits the other slots first, so a long drain of one slot does not starve the rest. A slot a batch found nothing in is skipped for the rest of the pass.
- **Stale-heartbeat acceleration uses runtime evidence.** A stale heartbeat cannot distinguish a dead runtime from a stalled one. Extra batches and early passes require every snapshot in `awa.runtime_instances` to be either fresh with its heartbeat loop alive or silent for at least `heartbeat_staleness`, with that condition observed for a full staleness window. Accelerated batches also require the claim's last heartbeat to precede the latest gone-runtime cutoff. The per-attempt staleness condition is unchanged.
  This is a bounded heuristic, not proof that an owner has exited. To expose an unconfirmed phase between fresh and gone, each runtime needs `3 × max(runtime_snapshot_interval, 1s) < heartbeat_staleness`. With 5-second snapshots and 12-second staleness, a stalled live runtime goes directly from fresh to gone; steady evidence may predate the stall, so acceleration can rescue its stale claims faster than the ordinary one-batch cadence. The leader must also sample frequently enough: gaps longer than half the staleness reset the evidence window, and short stalls between samples can be missed. The defaults (10-second snapshots, 90-second staleness, 30-second rescue interval) provide an unconfirmed phase; after a runtime stops reporting, acceleration usually begins after about 3–3.5 minutes. A new maintenance leader resets the evidence window. Deadline and callback rescues use their expired deadline and do not need owner evidence.
- **Retries retain their backoff.** Rescue bounds closures and retry writes per pass. Backoffs and promotion delays can make retries become due together; `rescue_pass_max_rows` does not cap the rate at which promotion admits them to the ready lanes.
- **Exhausted attempts are dead-lettered in the same batches.** On a DLQ-enabled queue an attempt rescued on its last try goes to the DLQ in its batch's transaction (reason `heartbeat_stale`, `deadline_exceeded` or `callback_timeout`), so a drain can move many jobs to the DLQ at once. `awa.job.dlq_moved` counts them by reason.

A pass holds the leader's recovery loop for up to its budget plus one batch, so promotion and the other rescues can wait about that long while a backlog drains. Cleanup, rotation and the other hygiene branches run in a separate loop alongside it ([ADR-051](adr/051-maintenance-recovery-loop.md)). Claim prune still waits for the rescue batch holding its slot. Watch a drain with `awa.maintenance.rescue.pass_size` (attempts per pass, by `awa.rescue.kind` and `awa.rescue.pass_end`: `drained`, `time_budget`, `row_limit`, `owners_unconfirmed` or `error`) and `awa.maintenance.rescue.oldest_attempt_age` (the age of the oldest attempt the latest pass rescued, `0` when it rescued nothing). An `oldest_attempt_age` far above `heartbeat_staleness` or the queue's deadline means rescue is behind.

### Retention and cleanup

Retention depends on the storage path.

Queue storage is the worker engine in `0.6`: ordinary terminal history (`completed`, non-DLQ `failed`, and `cancelled`) is reclaimed by queue-ring prune after the matching ready segment is no longer live. Successful receipt completions can live in compact `receipt_completion_batches_*` rows; failed, cancelled, non-receipt, and wide terminal snapshots live in `done_entries_*`. Custom metadata, tags, errors, and unique-key state make a successful completion wide; Awa-owned provenance metadata from priority aging or queue/priority moves stays eligible for compact receipt completion. Both physical histories hydrate through `{schema}.terminal_jobs` while immutable job-body fields remain in the retained `ready_entries_*` row until queue prune reclaims the segment. Direct SQL against `done_entries` should therefore expect nullable body columns and should not assume all completed jobs are present there. Use `{schema}.terminal_jobs` for read-only SQL inspection.

Ready rows are not deleted for unclaimed cancellation, priority aging, or SQL compatibility deletes through `awa.jobs`. Those paths append to `{schema}.ready_tombstones_*`; claim treats tombstoned lanes as spent evidence, exact-count queries skip the tombstone ledger, and queue prune truncates it with the matching ready/done segment.

`failed_retention` is a retention floor for non-DLQ `failed` terminal rows in **both** storage engines: a failed terminal row stays retryable for at least `failed_retention` past the moment it failed. On the canonical compatibility path the floor is enforced by row cleanup leaving recent `failed` rows in place. In queue storage the floor is enforced by carry-forward at prune time: when a queue slot is reclaimed, `failed` rows still inside the floor are widened into self-contained terminal rows and re-inserted into the live segment before the old slot is truncated, so they survive rotation regardless of how fast the ring turns over. Failed rows that have aged past the floor are pruned; their count is recorded in `queue_terminal_rollups.pruned_failed_count` and surfaced as `QueueCounts.pruned_failed`, a cumulative, monotonic count of failed rows no longer retryable. See [ADR-032](adr/032-failed-terminal-retention.md). `cancelled` rows are not held by the floor — cancellation is an explicit decision and cancelled rows are reclaimed with their segment.

`completed_retention` is canonical-only: it bounds how long completed jobs stay queryable on the compatibility path. Queue storage keeps ordinary completed terminal history in the rotating ring and reclaims it by queue-ring prune; use the queue-ring sizing and rotation knobs below to control how much remains queryable.

DLQ rows are different: `dlq_entries` is a separate hold table and the maintenance leader deletes rows older than `dlq_retention` in bounded cleanup passes.

| Knob | Default | What it does |
| --- | --- | --- |
| `completed_retention` | `24h` | Canonical compatibility path only: how long completed jobs stay queryable before row cleanup deletes them. Queue storage reclaims completed history by queue-ring prune instead. |
| `failed_retention` | `72h` | Both engines: a non-DLQ `failed` terminal row stays retryable for at least this long past failure. Canonical enforces it by leaving recent failed rows in place; queue storage enforces it by carry-forward at prune time. Does not apply to `cancelled` rows or to the DLQ. |
| `dlq_retention` | `30d` | How long DLQ rows stay in `dlq_entries` before bounded cleanup deletes them. |
| `descriptor_retention` | `30d` | How long stale queue/kind descriptor catalog rows survive. |
| `cleanup_interval` | `60s` | How often the cleanup pass runs. |
| `cleanup_batch_size` | `1000` | Max rows deleted per pass. Raise for very high throughput; lower if you want gentler IO. |
| `dlq_cleanup_batch_size` | `1000` | DLQ-specific batch size. |
| `ClientBuilder::terminal_count_rollup_interval` (Rust) / `terminal_count_rollup_interval_ms` (Python kwarg) | `30s` | Queue storage only: how often maintenance folds pending `done_entries` terminal-count deltas for sealed queue slots into compact live counters. Exact reads include pending deltas and retained compact receipt batches, so this affects compaction/WAL pressure rather than correctness. |

## Queue storage tuning

Queue storage is the runtime engine in `0.6`, and most deployments can keep the defaults. Queue-storage tables live in the canonical `awa` schema; the main knobs are there for large fleets, very bursty queues, or operators who want to trade off the partition-reclaim window against rotation churn.

### Producer path choice

For bulk producers on queue storage, prefer direct queue-storage COPY: Python `enqueue_many_copy()` or Rust `QueueStorage::enqueue_params_copy()`. Those paths stream rows straight into `ready_entries` / `deferred_jobs` and use the queue-storage enqueue heads directly.

Direct-copy producers must use the same queue-storage routing config as the worker fleet. This matters most for `queue_stripe_count` / `queue_storage_queue_stripe_count`: a producer using the default unstriped config can write to `queue` while striped workers claim from `queue#0`, `queue#1`, etc. Rust producers should construct `QueueStorage` with the same `QueueStorageConfig` used by workers. Python producers should pass the same `queue_storage_queue_stripe_count` to `enqueue_many_copy()` when the fleet uses non-default striping.

`insert_many_copy()` is the compatibility insert API. It is still useful when a caller needs the canonical insert surface, but in queue-storage mode it routes through the compatibility function rather than being the primary producer fast path.

### Terminal count rollup

Queue storage keeps exact terminal counts without updating mutable counter rows on the completion hot path. `done_entries` terminal inserts append positive rows to `queue_terminal_count_deltas_*`; retry, discard, DLQ move, and compatibility delete of `done_entries` rows append negative rows. Compact receipt completions do not write count deltas: exact count reads add retained `receipt_completion_batches_*` counts and subtract retained `receipt_completion_tombstones_*`.

A queue slot is one partition in queue storage's rotating ready/done ring. A sealed slot is no longer the current write slot, so maintenance can fold its terminal-count deltas without touching the active completion segment.

The maintenance leader folds pending `done_entries` deltas from sealed queue slots every `terminal_count_rollup_interval` (default `30s`) and processes at most four slots per tick. Rollup is MVCC-horizon aware: if another backend is holding a visible snapshot open, or is idle in a transaction with an assigned transaction id, maintenance leaves pending delta rows append-only and tries again on a later tick. Exact counts remain correct because they read folded counters, pending deltas, retained compact batches, compact tombstones, and permanent rollups.

Raising the interval reduces folded-counter churn but leaves more pending delta rows for exact reads to sum. Lowering it folds counters sooner, which can help if exact-count reads are frequent and the `done_entries` delta ledger is growing faster than maintenance drains it. Under long reader transactions, lowering the interval does not force rollup; the useful fix is to release the pinned snapshot or let queue prune reclaim the whole sealed segment. Compact receipt successes are already retained as compact batch rows, so this interval does not change their hot-path write count.

### Rust

```rust
let client = Client::builder(pool.clone())
    .queue("email", QueueConfig::default())
    .queue_storage(
        QueueStorageConfig {
            queue_slot_count: 16,
            lease_slot_count: 8,
            claim_slot_count: 8,
            queue_stripe_count: 1,
            ..Default::default()
        },
        Duration::from_millis(1_000),
        Duration::from_millis(1_000),
    )
    .claim_rotate_interval(Duration::from_millis(1_000))
    .prune_lock_timeout(Duration::from_millis(10))
    .build()?;
```

### Python

```python
await client.start(
    [("email", 8)],
    queue_storage_schema="awa",
    queue_storage_queue_slot_count=16,
    queue_storage_lease_slot_count=8,
    queue_storage_claim_slot_count=8,
    queue_storage_queue_stripe_count=1,
    queue_storage_queue_rotate_interval_ms=1000,
    queue_storage_lease_rotate_interval_ms=1000,
    queue_storage_claim_rotate_interval_ms=1000,
    queue_storage_prune_lock_timeout_ms=10,
)
```

### What the knobs mean

| Knob | Default | What it controls |
| --- | --- | --- |
| `queue_slot_count` | `16` | Number of rotating ready, claim-attempt, tombstone, and terminal queue partitions. Together with queue rotation cadence and how quickly segments become prunable, this bounds ordinary terminal-history visibility in queue storage. Ready-backed terminal rows and claim-attempt evidence rely on the matching ready partition until queue prune reclaims the segment. |
| `lease_slot_count` | `8` | Number of rotating lease partitions |
| `claim_slot_count` | `8` | Number of rotating ADR-023 claim-ring partitions (`lease_claims`, `lease_claim_closures`, and `lease_claim_closure_batches` children). All three tables share the same `claim_slot` so each partition's claims and closure evidence are reclaimed together by `TRUNCATE`; compact closure batches also carry ready segment metadata so prune can prove sealed segments by counts and skip conservatively when counts do not prove closure. Each slot has tiny stale-rescue and deadline-rescue cursors in `claim_ring_slots`. |
| `queue_stripe_count` / `queue_storage_queue_stripe_count` | `1` | Number of physical stripes behind each logical queue. `1` is the normal unstriped path. For a single very hot queue on many small replicas, `2` is the current tuned recommendation; higher values should be benchmarked before use. |
| `lease_claim_receipts` | `true` | Use the receipt-plane short path (claim writes compact attempt-emission range evidence into `ready_claim_attempt_batches` and receipt evidence into row-local `lease_claims` or compact `lease_claim_batches`; successful compact completion validates the returned receipt identity, writes `receipt_completion_batches` terminal history, and writes `lease_claim_closure_batches` closure evidence; non-success exits write explicit closures into `lease_claim_closures`; receipt tables are reclaimed by claim-ring prune, and attempt-emission evidence is reclaimed by queue-ring prune). Receipts mode supports per-claim deadlines: when `QueueConfig.deadline_duration > 0`, the claim writes `clock_timestamp() + interval` onto `lease_claims.deadline_at` and the maintenance rescue path force-closes expired claims with a `'deadline_expired'` closure. Set to `false` to force every claim through the legacy `leases` materialization path. See ADR-023. |
| `queue_rotate_interval` | `1000ms` | How often ready/terminal segments rotate |
| `lease_rotate_interval` | `1000ms` | How often lease segments rotate |
| `claim_rotate_interval` | matches `queue_rotate_interval` | How often the ADR-023 claim-ring rotates. Set with `ClientBuilder::claim_rotate_interval` (Rust) or `queue_storage_claim_rotate_interval_ms` (Python). Test harnesses sometimes set this to a long interval to pin claim-ring layout for deterministic count assertions. |
| `prune_lock_timeout` | `10ms` | The PostgreSQL `lock_timeout` the maintenance leader applies when it takes the exclusive locks to reclaim a queue, lease or claim ring partition (and when it folds terminal-count deltas). The timeout applies separately to each lock acquisition, not to the whole prune. Queue prune locks up to eight partitions, so its partition-lock waits alone can approach eight times this value. A lock timeout rolls the prune back, is counted as `blocked`, and is retried on a later maintenance tick. Set with `ClientBuilder::prune_lock_timeout` (Rust) or `queue_storage_prune_lock_timeout_ms` (Python). Must be greater than zero and at most `2147483647` ms; keep it well below `deadlock_timeout` and `statement_timeout`. The short default keeps prune's pending `ACCESS EXCLUSIVE` request from stalling claims and enqueues queued behind it. See below for when to raise it. |

The benchmark harness in [postgresql-job-queue-benchmarking](https://github.com/hardbyte/postgresql-job-queue-benchmarking) reads `QUEUE_SLOT_COUNT`, `LEASE_SLOT_COUNT`, `CLAIM_SLOT_COUNT`, `QUEUE_STRIPE_COUNT`, and `LEASE_CLAIM_RECEIPTS` from the environment. Those env vars are benchmark configuration, not general worker-runtime configuration.

Use the defaults unless you have a reason not to:

- Increase `queue_slot_count` if queue partitions stay unprunable for too long because readers, active leases, or pending ready rows keep old segments live.
- Increase `lease_slot_count` if lease churn is high enough that dead tuples in the lease ring stop collapsing promptly.
- Increase `claim_slot_count` if the rotation cadence (`claim_rotate_interval`) plus the slot count combine to a partition retention window shorter than your longest in-flight zero-deadline short job; running out of empty slots forces `rotate_claims` to return `SkippedBusy` and the receipt-plane churn falls back onto a smaller working set of partitions.
- Increase `queue_stripe_count` only for measured workloads where many small replicas contend on the same logical queue. This is queue-storage configuration, not a per-queue `QueueConfig` field: it gives each logical queue the same number of physical stripes. Striping spreads a hot logical queue over `queue#N` physical coordination paths, but it weakens perfect global ordering and can regress calmer shapes if overused. For single hot-queue workloads, benchmark `2` stripes first; higher values should be treated as workload-specific tuning rather than a general default.
- Increase `prune_lock_timeout` when `increase(awa_maintenance_prune_attempts_total{awa_ring_outcome="blocked"}[15m])` stays above zero and the rings stop recycling, typically because frequent short reads of the ring partitions (admin or dashboard queries over the queue-storage tables) hold a conflicting lock at the instant prune tries. A stalled ring grows its current slot, which makes those queries slower and the contention worse. Raise it in small steps (for example `50ms`, then `100ms`, then `250ms`) and keep it well below PostgreSQL's `deadlock_timeout` (default `1s`) and below any `statement_timeout` on the pool's connections. A single lock wait that reaches `deadlock_timeout` makes PostgreSQL run its deadlock detector, and if claims or enqueues are part of a lock cycle with it, the detector can cancel them rather than the prune; a short `lock_timeout` abandons the prune first, so the prune is always the one that gives way. A `statement_timeout` at or below the lock timeout cancels the prune statement before the lock timeout fires, so it fails with an error instead of reporting `blocked`. Startup logs a warning when the configured timeout is at or above either setting. While prune waits, new accesses to the same partition also queue behind its lock request, so a longer timeout trades claim latency for reclamation. Shortening the readers is the more durable fix when they run longer than a few hundred milliseconds. Watch `awa_maintenance_prune_attempts_total` by `awa_ring` and `awa_ring_outcome`: `pruned` should advance steadily, `blocked` means the lock timeout expired, `skipped_active` (with `awa_ring_reason`) means live rows kept the slot, `backed_off` means prune was skipped for up to 32 ticks after consecutive `skipped_active` or `blocked` results, so a persistently blocked ring is retried less often over time, and `error` is a database failure.
- Consider increasing rotation intervals before raising `prune_lock_timeout`: longer intervals can reduce how often prune requests exclusive locks, while a longer timeout lets each request wait longer. Confirm the effect with workload measurements.
- Decrease rotation intervals to tighten dead-tuple bounds at the cost of more frequent rotate/prune work.

Longer rotation intervals accumulate more work in each slot. At a steady enqueue rate, ready rows per queue slot are roughly the enqueue rate multiplied by `queue_rotate_interval`; retries and blocked recycling can increase the retained footprint. Budget for larger partitions and indexes. Queue-ring history becomes coarser and generally spans about `queue_slot_count × queue_rotate_interval`; active or blocked slots can retain it longer. The independent `claim_rotate_interval` changes claim-evidence retention, not completed-job history in the ready/terminal ring.

A longer interval does not remove the locks required by each prune. `prune_lock_timeout` still applies separately to each acquisition; queue prune can lock up to eight partitions before its later reads and writes.

### Sharding the enqueue head per queue

Each queue's `awa.queue_meta.enqueue_shards` (default `1`, range `1..=64`) governs how many independent enqueue head rows the queue has. With the default value, the queue uses a single head row and the ordering contract is strict FIFO per `(queue, priority)` — identical to v016.

Raising `enqueue_shards` is a **semantic mode switch**, not a hidden performance knob. The queue's contract becomes **partitioned FIFO** — strict order within `(queue, priority, enqueue_shard)`, no order promised across shards. It is the same kind of decision as choosing SQS Standard over SQS FIFO, raising Kafka partition count, or using Pub/Sub ordering keys.

```sql
-- Opt a contended queue into 4 shards.
INSERT INTO awa.queue_meta (queue, enqueue_shards)
VALUES ('my_hot_queue', 4)
ON CONFLICT (queue)
DO UPDATE SET enqueue_shards = EXCLUDED.enqueue_shards;
```

Use an upsert rather than a plain `UPDATE`: the first enqueue can create queue-storage lane rows before an operator-owned `queue_meta` row exists, so a plain `UPDATE` may quietly affect zero rows.

A 16-producer same-queue reference sweep measured 1.0× / 1.60× / 2.75× / 3.69× at S=1/2/4/8. Application authors who need per-key FIFO at S>1 (per-customer, per-order, per-account) pass `InsertOpts::ordering_key` (Rust) or `ordering_key=...` (Python) on insert — jobs sharing the key always land on the same shard regardless of which producer batch emitted them.

Observability: the `awa.job.claimed` OTel counter carries an `awa.enqueue.shard` attribute on the queue-storage claim path. Dashboards can sum by that attribute to confirm the claim ordering is rotating fairly across shards.

Lowering the value is safe at any time — see [`docs/upgrade-0.5-to-0.6.md`](upgrade-0.5-to-0.6.md#lowering-enqueue_shards). See [ADR-025](adr/025-sharded-enqueue-heads.md) for the full design and contract.

### Hot-Queue Claim Control

Queue storage also uses a bounded-claimer control plane (`queue_claimer_state` / `queue_claimer_leases`) so not every replica hammers a hot queue's claim path at once. For a single hot queue, raising `QueueConfig.claimers` lets one runtime run multiple dispatcher/claimer loops while still sharing the queue's `max_workers` / `min_workers` permits.

Keep `claimers` modest. In the hot-queue reference shape, `enqueue_shards = 4` is the change that turns overload into bounded end-to-end throughput; raising `claimers` to `4` increases transaction pressure and worsens tail latency in that shape. For extreme single-queue workloads, raise `enqueue_shards` first when the ordering contract allows partitioned FIFO. Benchmark `claimers = 2` or `4` only for a workload-specific reason, and judge the result by durable completion rate, p99 end-to-end latency, queue depth, WAL bytes per completed job, transaction commits per completed job, and dead tuples. Lower `claim_batch_size` from `512` for small or latency-sensitive queues; raise claimers only with workload-specific evidence.

Queue storage defaults `AWA_COMPLETION_SHARDS` to `1` for ordinary runtimes and `4` when the configured runtime worker capacity is at least `512` (`8` on canonical storage). Extra completion flushers can improve large single-process shapes, but they also multiply terminal-write contention across a worker fleet. Override this only after measuring the same north-star metrics for the target topology.

`AWA_COMPLETION_BATCH_SIZE` defaults to `512` and `AWA_COMPLETION_FLUSH_MS` defaults to `1`. The queue-storage short-job path fuses receipt closure and terminal insertion into one statement, so the lower flush interval reduces the worker capacity tied up waiting for durable completion while the batch size still amortises the claim/complete path under load. That pairing is what moves the lower WAL budget into durable throughput.

A runtime lets at most the larger of its worker capacity and `2 × AWA_COMPLETION_BATCH_SIZE × AWA_COMPLETION_SHARDS` finished attempts wait for their completion to commit: 1,024 with the queue-storage defaults below 512 workers. Each attempt's lifecycle hooks run within that allowance. A handler that finishes while it is used up keeps its worker slot until a completion commits, so a runtime whose completions or hooks fall behind claims more slowly instead of holding the backlog in memory. Each waiting attempt holds its job, payload included; for large payloads, lower `AWA_COMPLETION_BATCH_SIZE` to lower the bound in proportion.

For very high-throughput no-op queues, size `max_workers` for durable completion latency, not handler CPU alone. The `10k/s` offered-rate reference shape needs 1,024 in-flight worker permits to absorb the load consistently; real jobs with non-trivial handler time need workload-specific sizing.

The terminal write path is already narrow for running/waiting jobs: successful receipt completions use compact `receipt_completion_batches_*` rows, and other retained terminal facts use `done_entries_*` without duplicating immutable ready body fields when the ready row is still retained. Both surfaces hydrate through `terminal_jobs`, so the default completion settings stay conservative without giving up durable terminal history.

## Dead Letter Queue

The DLQ is the **separate, durable hold-table for jobs that exhausted retries or hit a non-retryable terminal failure**. Without it, terminal failures live in ordinary queue-storage `done_entries_*` partitions and are reclaimed when queue-ring prune can safely truncate their segment. With DLQ enabled, those rows land in `dlq_entries`, are visible to the admin UI / API as a discrete backlog, and can be retried or purged by an operator.

### When to enable

- **Enable DLQ for queues whose terminal failures need an operator decision.** Payment, notification, billing — anything where you'd rather a human triages a failure than have the job silently age out.
- **Leave DLQ disabled for high-throughput queues whose failures are fire-and-forget.** Logging, telemetry, ETL retries that get re-driven from upstream — accumulating dead rows here is just storage cost.
- **Default to disabled** unless you've decided either way; the builder's `dlq_enabled_by_default` is the global switch and `queue_dlq_enabled` is the per-queue override.

### Configuring

```rust
use std::time::Duration;

let client = Client::builder(pool.clone())
    .dlq_enabled_by_default(true)            // default for every queue
    .queue_dlq_enabled("metrics_flush", false) // exception: this one stays off
    .dlq_retention(Duration::from_secs(60 * 60 * 24 * 30))  // 30 days
    .dlq_cleanup_batch_size(1000)
    .build()
    .await?;
```

DLQ policy is per-queue, not per-job-kind. If you need a single queue to handle some kinds with DLQ and some without, split them onto two queues (the same shape recommended for [worker scope by kind](#targeting-specific-job-kinds-within-a-queue)).

Per-queue retention overrides go through `RetentionPolicy.dlq` on `queue_retention(queue, policy)` — handy if one queue's failures need to live longer than the global `dlq_retention`.

### Operator workflow

Once DLQ is on, operators interact with it through:

- **Web UI** — DLQ tab with retry, purge, and per-row failure detail.
- **CLI** — `awa dlq list`, `awa dlq retry <id>`, `awa dlq purge`.
- **REST** — `/api/dlq/*` endpoints (same actions as the UI).
- **Python / Rust** — `client.dlq_*` admin methods for programmatic retry / purge.

Queue-policy declaration is still a runtime-side concern; the UI / API report state but don't toggle DLQ on/off (that's a code-level decision so it doesn't drift between deployments). See [ADR-020](adr/020-dead-letter-queue.md).

## CLI and `awa serve`

The CLI reads `DATABASE_URL` from the environment or `--database-url`. All subcommands except `serve` use a single database connection.

`awa serve` starts the admin UI and API. It has its own connection pool and response cache, configurable via CLI flags or environment variables:

```
awa serve --pool-max 10 --cache-ttl 5
```

Every flag has a corresponding `AWA_*` environment variable (shown in `--help`):

| Flag | Env var | Default | Purpose |
| --- | --- | --- | --- |
| `--pool-max` | `AWA_POOL_MAX` | `10` | Max database connections |
| `--pool-min` | `AWA_POOL_MIN` | `2` | Min idle connections |
| `--pool-idle-timeout` | `AWA_POOL_IDLE_TIMEOUT` | `300` | Idle connection timeout (seconds) |
| `--pool-max-lifetime` | `AWA_POOL_MAX_LIFETIME` | `1800` | Max connection lifetime (seconds) |
| `--pool-acquire-timeout` | `AWA_POOL_ACQUIRE_TIMEOUT` | `10` | Connection acquire timeout (seconds) |
| `--statement-timeout` | `AWA_STATEMENT_TIMEOUT` | `60` | Longest any one UI statement may run (seconds); `0` leaves the server's setting |
| `--cache-ttl` | `AWA_CACHE_TTL` | `5` | Dashboard query cache TTL (seconds) |

### Abandoned requests

When a browser closes a tab or a proxy times out, the server drops the request, but Postgres keeps running its query and the pool keeps that query's connection until it ends. The statement timeout ends it, and the pool then reuses the connection. On Postgres 14 and later the pool also sets `client_connection_check_interval`, so a query stops within 5 s when its connection closes, for example when `awa serve` restarts. Raise `--statement-timeout` if a bulk action from the UI, such as purging a very large DLQ, needs longer.

### Dashboard cache

The cache deduplicates repeated poll requests — multiple browser tabs or rapid refresh cycles within the TTL window hit memory rather than the database. The frontend polling interval is derived from the cache TTL (minimum 5s) and served via `/api/capabilities`, so clients automatically back off to match the server's refresh rate.

If you're connecting to a read replica, increase `--cache-ttl` to reduce load. The dashboard will feel slightly less real-time but won't overwhelm the replica.

### Read-only mode

`awa serve` disables mutation endpoints (retry, cancel, pause, drain) whenever the server is running in read-only mode. `/api/capabilities` reports `read_only: true` and the frontend hides the corresponding buttons. Mutation requests against a read-only server return `503 Service Unavailable` with a clear error body.

There are two ways to opt in:

| Mode | Trigger | When to use |
| --- | --- | --- |
| Auto-detect (default) | Server probes `current_setting('transaction_read_only')` on startup | Pointed at a read replica or a Postgres role without write grants |
| Forced | `--read-only` flag or `AWA_READ_ONLY=1` env var | Writable DB but you want mutations off — incident read-outs, shared debugging instances, less-trusted public UI sessions |

```bash
# Auto-detect (current behaviour)
awa --database-url "$DATABASE_URL" serve

# Explicit — force read-only regardless of DB privileges
awa --database-url "$DATABASE_URL" serve --read-only

# Same via env var
AWA_READ_ONLY=1 awa --database-url "$DATABASE_URL" serve
```

Once forced, there is no way for a frontend user to flip back to writable without restarting the server — that's the whole point.

## Next

- [Deployment guide](deployment.md)
- [Migration guide](migrations.md)
- [Troubleshooting](troubleshooting.md)

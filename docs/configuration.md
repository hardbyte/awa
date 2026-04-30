# Configuration

AWA has three configuration surfaces: the **Rust runtime** (`ClientBuilder` + `QueueConfig`), the **Python runtime** (`client.start()`), and the **CLI** (`awa serve`, `awa job`, etc). This guide explains how they work rather than listing every option — use `--help`, IDE autocomplete, or the source for exhaustive reference.

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

A single worker process handles work for the **set of queues it was
configured with** and the **set of job kinds it registered handlers
for**. There is no implicit fan-out across queues, and no implicit
filter on kinds within a queue — these are two separate concerns.

### Targeting specific queues

A worker only claims jobs from queues passed to its builder; everything
else on the database is invisible to it. To run a worker dedicated to
one queue, only declare that queue:

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

Run separate worker processes (or separate fleets) per queue when you
want **isolation**: a stuck `etl` queue can't starve `email`,
deployment of a slow handler doesn't pause unrelated queues, and per-
queue scaling is just a deployment knob. Run **one worker process
across multiple queues** with `global_max_workers` and weighted mode
when you want elastic capacity sharing — see [Weighted
mode](#weighted-mode).

### Targeting specific job kinds within a queue

`register::<SomeJob, _, _>` (Rust) or `@client.task(SomeJob, ...)`
(Python) tells the worker how to execute one specific job kind. At
execute time the worker looks up the kind on the claimed job and runs
the matching handler.

**Sharp edge:** the claim path is per-queue, not per-kind. If a queue
holds jobs of kinds the worker didn't register, the worker still
claims those jobs (it can't tell ahead of time) and then **fails them
terminally** with `unknown job kind: <name>`. There is no soft re-
enqueue. So the supported patterns for "this worker only handles a
subset of kinds" are:

1. **Recommended: put each kind set on its own queue.** Queues are
   cheap; this is the operator-shaped boundary. Workers with
   different kind responsibilities subscribe to different queues, and
   the queue boundary becomes the routing decision.
2. **Acceptable: every worker on the queue registers handlers for
   every kind that lands there.** Heterogeneous workloads on a single
   queue work fine as long as no worker is missing a registration.
3. **Not supported: have some workers on a queue claim jobs and
   silently leave kinds they don't know for someone else.** This will
   terminal-fail or DLQ jobs.

If kinds drift out of sync (e.g. a deploy lags), the descriptor
catalog in the admin UI flags missing handlers; see [Queue and
job-kind descriptors](#queue-and-job-kind-descriptors) below.

## Queue configuration

Every queue needs a `QueueConfig`. The two fundamental choices are:

1. **Hard-reserved mode** (default) — each queue gets a fixed `max_workers` slot count
2. **Weighted mode** — call `global_max_workers(N)` to share a pool, with `min_workers` as a floor and `weight` for overflow

### Rust

```rust
let client = Client::builder()
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
    .build(&pool)
    .await?;
```

The key `QueueConfig` fields:

| Field | Default | When you'd change it |
|---|---|---|
| `max_workers` | `50` | Always — this is your concurrency cap per queue |
| `rate_limit` | `None` | External API rate limits, backpressure |
| `deadline_duration` | `5m` | Hard upper bound on a single attempt. Set to `Duration::ZERO` to skip the deadline rescue path; receipts mode (the 0.6 default storage) supports both shapes — the deadline lands on `lease_claims.deadline_at` and the maintenance rescue path force-closes expired claims. |
| `poll_interval` | `200ms` | Tune if NOTIFY latency matters (rare) |
| `min_workers` / `weight` | `0` / `1` | Only in weighted mode |

### Python

Tuple form for simple cases, dict form for full control:

```python
# Hard-reserved — just (name, max_workers)
await client.start([("email", 10), ("reports", 5)])

# Dict form — rate limiting, weighted mode, retention
await client.start([
    {"name": "email", "max_workers": 10, "rate_limit": (50.0, 50)},
    {"name": "reports", "max_workers": 5},
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

## Job priority and aging

Every job carries a **priority** (`i16`, lower number = higher
priority). The default is `2`. Conventional usage:

| Priority | Typical meaning |
|---|---|
| `1` | Urgent / customer-facing / SLA-critical |
| `2` | Default |
| `3` | Background work |
| `4`+ | Batch / catch-up / bulk reprocess |

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

Without aging, a steady stream of priority-1 work can starve every
priority-2 job behind it. AWA escalates priority over time — the
longer a job has been waiting, the higher (numerically lower) its
**effective priority** becomes at claim time. A priority-4 job that
has waited `4 × aging_interval` ages all the way down to priority 1
and is no longer starvable.

The cadence is per-queue via `QueueConfig.priority_aging_interval`
(default `60s`):

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

Aging is computed at claim time on queue-storage runtimes — the stored
priority does not change, only the effective priority used for
ordering. The admin UI surfaces both the original priority (so you can
still see "this was enqueued as priority 4") and the current effective
priority. Set the value to `Duration::ZERO` (Rust) or
`priority_aging_interval_ms: 0` (Python) to disable escalation
entirely (strict static priority).

There is a separate top-level `ClientBuilder::priority_aging_interval`
that controls the legacy canonical-storage maintenance pass that
physically rewrites stored priorities. With queue storage (the 0.6
default) it is a no-op; the per-queue setting above is the one to
tune.

## Queue and job-kind descriptors

Queues and job kinds can carry operator-facing metadata: display names, descriptions, owners, docs links, tags, and arbitrary JSON `extra`. This is separate from runtime scheduling config and drives the labels the admin UI / API surface.

The runtime catalogs and propagates these — see [Architecture → Control-plane descriptors](architecture.md#control-plane-descriptors) for how sync, staleness, and drift detection work.

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

## Reliability timings: heartbeat, deadline, rescue

These knobs control **how fast a stuck or crashed handler is noticed
and rescued**. They live on `ClientBuilder` (Rust) and `client.start()`
kwargs (Python). All of them have `_ms`-suffixed kwargs on the Python
side (e.g. `heartbeat_interval_ms=15000`).

### Heartbeat — detecting crashed workers

A running job updates a heartbeat row periodically while its handler
is alive. If the heartbeat goes stale, the maintenance leader rescues
the job (re-enqueues it for another attempt). Three knobs participate:

| Knob | Default | What it does |
|---|---|---|
| `heartbeat_interval` | `30s` | How often each running handler refreshes its heartbeat row. |
| `heartbeat_staleness` | `90s` | How long the row may go un-refreshed before the maintenance leader treats the job as crashed. |
| `heartbeat_rescue_interval` | `30s` | How often the maintenance leader scans for stale heartbeats. |

Pick them in this order:

1. **Decide your detection target.** "Crashes should be noticed within
   X seconds." That target is roughly `heartbeat_staleness +
   heartbeat_rescue_interval` in the worst case.
2. **Set `heartbeat_staleness` to at least `3× heartbeat_interval`.**
   The 3× rule absorbs scheduler hiccups, GC pauses, and the rescue
   scan's own jitter; tighter ratios produce false rescues. The
   builder logs a warning if you violate it.
3. **`heartbeat_rescue_interval`** can match `heartbeat_interval` for
   low-latency rescue, or be higher to reduce maintenance load on big
   fleets.

For a 5-second crash detection target: `heartbeat_interval=1s`,
`heartbeat_staleness=4s`, `heartbeat_rescue_interval=1s`. For a
1-minute target with the cheapest possible maintenance: keep all the
defaults.

### Deadline — bounding a single attempt

Each queue has a `deadline_duration` (default `5m` on
`QueueConfig`). At claim time the runtime stamps
`now() + deadline_duration` onto the claim, and a maintenance scan
force-closes attempts that pass it without completing. This bounds a
single attempt's wall-clock time independently of heartbeats — if a
handler is hanging, looping forever, or wedged in a sync wait,
the deadline rescues it even if its heartbeat is fresh.

| Knob | Default | What it does |
|---|---|---|
| `QueueConfig.deadline_duration` (Rust) / `deadline_duration_ms` (Python dict form) | `5m` | Per-queue hard upper bound on one attempt. `Duration::ZERO` / `0` skips deadline rescue for that queue. |
| `ClientBuilder::deadline_rescue_interval` (Rust) / `deadline_rescue_interval_ms` (Python kwarg) | `30s` | How often the maintenance leader scans for expired deadlines. |

Receipts mode (the 0.6 default storage) supports both shapes: the
deadline lands on `lease_claims.deadline_at` and is rescued there for
short claims, or carried onto `leases.deadline_at` if the claim
materializes for a long-running attempt. See [Queue storage
tuning](#queue-storage-tuning) and ADR-023.

### Callback timeout — bounding `wait_for_callback`

If you suspend a handler with `wait_for_callback()` and the external
system never resumes, a callback-timeout rescue brings the job back to
ready (or DLQ if attempts are exhausted).

| Knob | Default | What it does |
|---|---|---|
| `ClientBuilder::callback_rescue_interval` | `30s` | How often the maintenance leader scans for `callback_timeout_at < now()`. The per-callback timeout itself is set when registering the callback in the handler. |

### Retention and cleanup

Terminal jobs and DLQ rows aren't purged synchronously; the
maintenance leader sweeps them in batches.

| Knob | Default | What it does |
|---|---|---|
| `completed_retention` | `24h` | How long completed jobs stay queryable before the cleanup pass deletes them. |
| `failed_retention` | `72h` | Same for failed/cancelled jobs (excluding DLQ). |
| `dlq_retention` | none | If unset, DLQ rows live forever. Set a duration to age them out. |
| `descriptor_retention` | `30d` | How long stale queue/kind descriptor catalog rows survive. |
| `cleanup_interval` | `60s` | How often the cleanup pass runs. |
| `cleanup_batch_size` | `1000` | Max rows deleted per pass. Raise for very high throughput; lower if you want gentler IO. |
| `dlq_cleanup_batch_size` | `1000` | DLQ-specific batch size. |

## Queue storage tuning

Queue storage is the runtime engine in `0.6`, and most deployments can keep
the defaults. Queue-storage tables live in the canonical `awa` schema; the
main knobs are there for large fleets, very bursty queues, or operators who
want to trade off retention-window size against rotation churn.

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
        Duration::from_millis(50),
    )
    .claim_rotate_interval(Duration::from_millis(1_000))
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
    queue_storage_queue_rotate_interval_ms=1000,
    queue_storage_lease_rotate_interval_ms=50,
    queue_storage_claim_rotate_interval_ms=1000,
)
```

### What the knobs mean

| Knob | Default | What it controls |
|---|---|---|
| `queue_slot_count` | `16` | Number of rotating ready/terminal queue partitions |
| `lease_slot_count` | `8` | Number of rotating lease partitions |
| `claim_slot_count` | `8` | Number of rotating ADR-023 claim-ring partitions (`lease_claims` + `lease_claim_closures` children). Both tables share the same `claim_slot` so each partition's claims and closures are reclaimed together by `TRUNCATE`. |
| `queue_stripe_count` | `1` | Rust `QueueStorageConfig` only in the current API. Number of physical stripes behind each logical queue. `1` is the normal unstriped path. For a single very hot queue on many small replicas, `2` is the current release-shape candidate; higher values should be benchmarked before use. |
| `lease_claim_receipts` | `true` | Use the receipt-plane short path (claim writes a row into `lease_claims`; completion writes a closure tombstone into `lease_claim_closures`; both reclaimed by claim-ring rotation). Receipts mode supports per-claim deadlines: when `QueueConfig.deadline_duration > 0`, the claim writes `clock_timestamp() + interval` onto `lease_claims.deadline_at` and the maintenance rescue path force-closes expired claims with a `'deadline_expired'` closure. Set to `false` to force every claim through the legacy `leases` materialization path. See ADR-023. |
| `queue_rotate_interval` | `1000ms` | How often ready/terminal segments rotate |
| `lease_rotate_interval` | `50ms` | How often lease segments rotate |
| `claim_rotate_interval` | matches `queue_rotate_interval` | How often the ADR-023 claim-ring rotates. Set with `ClientBuilder::claim_rotate_interval` (Rust) or `queue_storage_claim_rotate_interval_ms` (Python). Tests that pin claim-ring layout for a deterministic count assertion can push this past their wall-clock window (see `queue_storage_runtime_test.rs::queue_storage_client` helper). |

The benchmark harness in
[postgresql-job-queue-benchmarking](https://github.com/hardbyte/postgresql-job-queue-benchmarking)
reads `QUEUE_SLOT_COUNT`, `LEASE_SLOT_COUNT`, `CLAIM_SLOT_COUNT`,
`QUEUE_STRIPE_COUNT`, and `LEASE_CLAIM_RECEIPTS` from the environment.
Those env vars are benchmark configuration, not general worker-runtime
configuration.

Use the defaults unless you have a reason not to:

- Increase `queue_slot_count` if queue partitions stay unprunable for too long because readers or retention keep old segments live.
- Increase `lease_slot_count` if lease churn is high enough that dead tuples in the lease ring stop collapsing promptly.
- Increase `claim_slot_count` if the rotation cadence (`claim_rotate_interval`) plus the slot count combine to a partition retention window shorter than your longest in-flight zero-deadline short job; running out of empty slots forces `rotate_claims` to return `SkippedBusy` and the receipt-plane churn falls back onto a smaller working set of partitions.
- Increase `queue_stripe_count` only for measured hot logical queues where many small replicas contend on the same queue. Striping spreads that one logical queue over `queue#N` physical coordination paths, but it weakens perfect global ordering and can regress calmer shapes if overused.
- Increase rotation intervals to reduce partition churn and metadata activity.
- Decrease rotation intervals to tighten dead-tuple bounds at the cost of more frequent rotate/prune work.

### Internal hot-queue claim control

Queue storage also uses an internal bounded-claimer control plane
(`queue_claimer_state` / `queue_claimer_leases`) so not every replica hammers a
hot queue's claim path at once. This is not a public `QueueConfig` knob in
0.6; tune queue pressure first with ordinary worker counts and, for extreme
single-queue workloads, `queue_stripe_count`.

## Dead Letter Queue

The DLQ is the **separate, durable hold-table for jobs that exhausted
retries or hit a non-retryable terminal failure**. Without it,
terminal failures live in ordinary `terminal_entries` rotation and
age out under the `failed_retention` window — they're observable
briefly, then gone. With DLQ enabled, those rows land in
`dlq_entries`, are visible to the admin UI / API as a discrete
backlog, and can be retried or purged by an operator.

### When to enable

- **Enable DLQ for queues whose terminal failures need an operator
  decision.** Payment, notification, billing — anything where you'd
  rather a human triages a failure than have the job silently age out.
- **Leave DLQ disabled for high-throughput queues whose failures are
  fire-and-forget.** Logging, telemetry, ETL retries that get
  re-driven from upstream — accumulating dead rows here is just
  storage cost.
- **Default to disabled** unless you've decided either way; the
  builder's `dlq_enabled_by_default` is the global switch and
  `queue_dlq_enabled` is the per-queue override.

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

DLQ policy is per-queue, not per-job-kind. If you need a single queue
to handle some kinds with DLQ and some without, split them onto two
queues (the same shape recommended for [worker scope by
kind](#targeting-specific-job-kinds-within-a-queue)).

Per-queue retention overrides go through `RetentionPolicy.dlq` on
`queue_retention(queue, policy)` — handy if one queue's failures need
to live longer than the global `dlq_retention`.

### Operator workflow

Once DLQ is on, operators interact with it through:

- **Web UI** — DLQ tab with retry, purge, and per-row failure detail.
- **CLI** — `awa dlq list`, `awa dlq retry <id>`, `awa dlq purge`.
- **REST** — `/api/dlq/*` endpoints (same actions as the UI).
- **Python / Rust** — `client.dlq_*` admin methods for programmatic
  retry / purge.

Queue-policy declaration is still a runtime-side concern; the UI /
API report state but don't toggle DLQ on/off (that's a code-level
decision so it doesn't drift between deployments). See
[ADR-020](adr/020-dead-letter-queue.md).

## CLI and `awa serve`

The CLI reads `DATABASE_URL` from the environment or `--database-url`. All subcommands except `serve` use a single database connection.

`awa serve` starts the admin UI and API. It has its own connection pool and response cache, configurable via CLI flags or environment variables:

```
awa serve --pool-max 10 --cache-ttl 5
```

Every flag has a corresponding `AWA_*` environment variable (shown in `--help`):

| Flag | Env var | Default | Purpose |
|---|---|---|---|
| `--pool-max` | `AWA_POOL_MAX` | `10` | Max database connections |
| `--pool-min` | `AWA_POOL_MIN` | `2` | Min idle connections |
| `--pool-idle-timeout` | `AWA_POOL_IDLE_TIMEOUT` | `300` | Idle connection timeout (seconds) |
| `--pool-max-lifetime` | `AWA_POOL_MAX_LIFETIME` | `1800` | Max connection lifetime (seconds) |
| `--pool-acquire-timeout` | `AWA_POOL_ACQUIRE_TIMEOUT` | `10` | Connection acquire timeout (seconds) |
| `--cache-ttl` | `AWA_CACHE_TTL` | `5` | Dashboard query cache TTL (seconds) |

### Dashboard cache

The cache deduplicates repeated poll requests — multiple browser tabs or rapid refresh cycles within the TTL window hit memory rather than the database. The frontend polling interval is derived from the cache TTL (minimum 5s) and served via `/api/capabilities`, so clients automatically back off to match the server's refresh rate.

If you're connecting to a read replica, increase `--cache-ttl` to reduce load. The dashboard will feel slightly less real-time but won't overwhelm the replica.

### Read-only mode

`awa serve` disables mutation endpoints (retry, cancel, pause, drain) whenever the server is running in read-only mode. `/api/capabilities` reports `read_only: true` and the frontend hides the corresponding buttons. Mutation requests against a read-only server return `503 Service Unavailable` with a clear error body.

There are two ways to opt in:

| Mode | Trigger | When to use |
|---|---|---|
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

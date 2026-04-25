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
| `deadline_duration` | `5m` | Long-running jobs that need more time |
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

## Runtime tuning

`ClientBuilder` (Rust) and `client.start()` kwargs (Python) control maintenance loop intervals. The defaults are sensible for most workloads — you'd typically only touch these for:

- **Heartbeat interval** (`30s`) — lower if you need faster crash detection
- **Retention** (`24h` completed, `72h` failed) — raise if you need longer history, lower to reduce table size
- **Cleanup batch size** (`1000`) — raise for high-throughput systems to avoid frequent cleanup passes

All intervals have `_ms` suffixed kwargs in Python (e.g. `heartbeat_interval_ms=15000`).

## Queue storage tuning

Queue storage is the runtime engine in `0.6`, and most deployments can keep
the defaults. The main knobs are there for large fleets, very bursty queues, or
operators who want to trade off retention-window size against rotation churn.

### Rust

```rust
let client = Client::builder(pool.clone())
    .queue("email", QueueConfig::default())
    .queue_storage(
        QueueStorageConfig {
            queue_slot_count: 16,
            lease_slot_count: 8,
            claim_slot_count: 8,
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
    queue_storage_schema="awa_exp",
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
| `queue_rotate_interval` | `1000ms` | How often ready/terminal segments rotate |
| `lease_rotate_interval` | `50ms` | How often lease segments rotate |
| `claim_rotate_interval` | matches `queue_rotate_interval` | How often the ADR-023 claim-ring rotates. Set with `ClientBuilder::claim_rotate_interval` (Rust) or `queue_storage_claim_rotate_interval_ms` (Python). Tests that pin claim-ring layout for a deterministic count assertion can push this past their wall-clock window (see `queue_storage_runtime_test.rs::queue_storage_client` helper). |

The corresponding env vars (`QUEUE_SLOT_COUNT`, `LEASE_SLOT_COUNT`,
`CLAIM_SLOT_COUNT`, `EXPERIMENTAL_LEASE_CLAIM_RECEIPTS`) override
the same fields when set; useful for benchmark adapters that pull
configuration from the environment.

Use the defaults unless you have a reason not to:

- Increase `queue_slot_count` if queue partitions stay unprunable for too long because readers or retention keep old segments live.
- Increase `lease_slot_count` if lease churn is high enough that dead tuples in the lease ring stop collapsing promptly.
- Increase `claim_slot_count` if the rotation cadence (`claim_rotate_interval`) plus the slot count combine to a partition retention window shorter than your longest in-flight zero-deadline short job; running out of empty slots forces `rotate_claims` to return `SkippedBusy` and the receipt-plane churn falls back onto a smaller working set of partitions.
- Increase rotation intervals to reduce partition churn and metadata activity.
- Decrease rotation intervals to tighten dead-tuple bounds at the cost of more frequent rotate/prune work.

## Dead Letter Queue

Queue-storage deployments can route terminal failures into the DLQ instead of
leaving them in ordinary terminal history. The policy knobs currently live on
the Rust runtime builder:

```rust
use std::time::Duration;

let client = Client::builder(pool.clone())
    .dlq_enabled_by_default(true)
    .queue_dlq_enabled("metrics_flush", false)
    .dlq_retention(Duration::from_secs(60 * 60 * 24 * 30))
    .dlq_cleanup_batch_size(1000)
    .build()
    .await?;
```

Per-queue retention overrides still use `RetentionPolicy.dlq`.

Python, CLI, REST, and Web UI surfaces can inspect and operate on DLQ rows once
the deployment is using queue storage, but queue-policy declaration is still a
runtime-side concern. See [ADR-020](adr/020-dead-letter-queue.md).

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

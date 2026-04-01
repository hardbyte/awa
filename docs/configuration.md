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

## Runtime tuning

`ClientBuilder` (Rust) and `client.start()` kwargs (Python) control maintenance loop intervals. The defaults are sensible for most workloads — you'd typically only touch these for:

- **Heartbeat interval** (`30s`) — lower if you need faster crash detection
- **Retention** (`24h` completed, `72h` failed) — raise if you need longer history, lower to reduce table size
- **Cleanup batch size** (`1000`) — raise for high-throughput systems to avoid frequent cleanup passes

All intervals have `_ms` suffixed kwargs in Python (e.g. `heartbeat_interval_ms=15000`).

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

`awa serve` auto-detects read-only databases (replicas, read-only transactions) and disables mutation endpoints (retry, cancel, pause, drain). The frontend hides the corresponding buttons. No configuration needed.

## Next

- [Deployment guide](deployment.md)
- [Migration guide](migrations.md)
- [Troubleshooting](troubleshooting.md)

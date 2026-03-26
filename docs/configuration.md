# Configuration Reference

This document covers the public runtime knobs for Rust, Python, and the CLI.

## QueueConfig

Rust workers configure each queue with `QueueConfig`.

| Field | Default | Meaning |
|---|---|---|
| `max_workers` | `50` | Maximum concurrent jobs for the queue in hard-reserved mode |
| `poll_interval` | `200ms` | Poll fallback interval when no `NOTIFY` arrives |
| `deadline_duration` | `5m` | Hard limit for a running attempt before deadline rescue |
| `priority_aging_interval` | `60s` | How quickly lower-priority jobs age toward fairness |
| `rate_limit` | `None` | Optional token bucket: `RateLimit { max_rate, burst }` |
| `min_workers` | `0` | Guaranteed capacity in weighted mode |
| `weight` | `1` | Overflow share in weighted mode |

### RateLimit

| Field | Default | Meaning |
|---|---|---|
| `max_rate` | none | Sustained jobs/sec allowed for the queue |
| `burst` | `ceil(max_rate)` when `0` | Maximum burst size |

Validation:

- `max_rate` must be `> 0`
- `weight` must be `> 0`

## ClientBuilder

Rust worker runtime configuration lives on `ClientBuilder`.

| Method | Effective default | Meaning |
|---|---|---|
| `queue(name, config)` | required | Adds a queue; at least one queue must be configured |
| `register::<T, _, _>(handler)` | none | Registers a typed Rust worker |
| `register_worker(worker)` | none | Registers a raw `Worker` implementation |
| `state(value)` | none | Adds shared typed state for `ctx.extract::<T>()` |
| `heartbeat_interval(duration)` | `30s` | Heartbeat write interval |
| `promote_interval(duration)` | `250ms` | Scheduled/retryable promotion tick |
| `heartbeat_rescue_interval(duration)` | `30s` | Stale-heartbeat rescue tick |
| `deadline_rescue_interval(duration)` | `30s` | Deadline rescue tick |
| `callback_rescue_interval(duration)` | `30s` | Waiting-callback timeout rescue tick |
| `leader_election_interval(duration)` | `10s` | Retry interval for non-leaders |
| `leader_check_interval(duration)` | `30s` | Health check for the leader lock connection |
| `global_max_workers(n)` | disabled | Enables weighted mode with a shared overflow pool |
| `completed_retention(duration)` | `24h` | Retention for completed jobs |
| `failed_retention(duration)` | `72h` | Retention for failed/cancelled jobs |
| `cleanup_batch_size(n)` | `1000` | Max rows deleted per cleanup pass |
| `cleanup_interval(duration)` | `60s` | Cleanup tick |
| `queue_retention(queue, policy)` | none | Per-queue retention override |
| `runtime_snapshot_interval(duration)` | `10s` | How often runtime health snapshots are published |
| `periodic(job)` | none | Registers a cron schedule |
| `build()` | none | Validates and constructs the runtime |

Build validation:

- at least one queue must exist
- `cleanup_batch_size` must be `> 0`
- in weighted mode, `sum(min_workers) <= global_max_workers`

## Weighted Mode

Weighted mode is enabled only when you call:

```rust
.global_max_workers(N)
```

Then:

- `min_workers` becomes the guaranteed floor
- `weight` controls overflow share
- `max_workers` is no longer the primary capacity knob

## Python Runtime Configuration

`awa.Client(database_url, max_connections=10)` creates a synchronous client.
`awa.AsyncClient(database_url, max_connections=10)` has the same constructor for async use.

`client.start(...)` controls worker runtime settings.

### Queue Config Shapes

Hard-reserved mode supports tuple form:

```python
client.start([("email", 10)])
```

Dict form is also supported:

```python
client.start([
    {"name": "email", "max_workers": 10, "rate_limit": (100.0, 100)}
])
```

Weighted mode requires dict form:

```python
client.start(
    [{"name": "email", "min_workers": 5, "weight": 2}],
    global_max_workers=20,
)
```

### Python `start()` kwargs

| Kwarg | Default | Meaning |
|---|---|---|
| `poll_interval_ms` | `200` | Poll fallback interval |
| `global_max_workers` | `None` | Enables weighted mode |
| `completed_retention_hours` | runtime default | Completed retention |
| `failed_retention_hours` | runtime default | Failed/cancelled retention |
| `cleanup_batch_size` | runtime default | Cleanup batch size |
| `leader_election_interval_ms` | runtime default | Leader retry interval |
| `heartbeat_interval_ms` | runtime default | Heartbeat interval |
| `promote_interval_ms` | runtime default | Scheduled promotion interval |
| `heartbeat_rescue_interval_ms` | runtime default | Stale-heartbeat rescue interval |
| `deadline_rescue_interval_ms` | runtime default | Deadline rescue interval |
| `callback_rescue_interval_ms` | runtime default | Callback-timeout rescue interval |

### Python Queue Dict Keys

| Key | Required | Meaning |
|---|---|---|
| `name` | yes | Queue name |
| `max_workers` | hard-reserved mode | Queue concurrency cap |
| `min_workers` | weighted mode | Guaranteed floor |
| `weight` | no, default `1` | Overflow share |
| `rate_limit` | no | `(max_rate: float, burst: int)` |
| `retention` | no | `{"completed_hours": ..., "failed_hours": ...}` |

Validation rules:

- tuple form is not allowed with `global_max_workers`
- weighted mode requires explicit queue configs
- a dict cannot contain both `max_workers` and `min_workers`
- every worker queue declared with `@client.task(..., queue=...)` must be configured in `start()`

## CLI Configuration

The `awa` CLI currently supports these global flags:

| Flag | Default | Meaning |
|---|---|---|
| `--database-url` | `DATABASE_URL` env var | Postgres connection string |

`serve` adds:

| Flag | Env var | Default | Meaning |
|---|---|---|---|
| `--host` | | `127.0.0.1` | Bind host |
| `--port` | | `3000` | Bind port |
| `--pool-size` | `AWA_POOL_MAX` | `10` | Maximum database connections |
| `--pool-min` | `AWA_POOL_MIN` | `2` | Minimum idle connections kept open |
| `--pool-idle-timeout` | `AWA_POOL_IDLE_TIMEOUT` | `300` | Seconds before an idle connection is closed |
| `--pool-max-lifetime` | `AWA_POOL_MAX_LIFETIME` | `1800` | Maximum lifetime of a connection in seconds |
| `--pool-acquire-timeout` | `AWA_POOL_ACQUIRE_TIMEOUT` | `10` | Seconds to wait when acquiring a connection |
| `--cache-ttl` | `AWA_CACHE_TTL` | `5` | Server-side cache TTL for dashboard queries in seconds |

The cache deduplicates repeated poll requests from the frontend — multiple
browser tabs or rapid polling cycles within the TTL window hit memory rather
than the database. The frontend polling interval is derived from the cache TTL
(minimum 5 s) and served via the `/api/capabilities` endpoint.

## Environment Variables

These are the direct environment-variable integrations in the current codebase:

| Variable | Consumer | Meaning |
|---|---|---|
| `DATABASE_URL` | `awa` CLI | Default value for `--database-url` |
| `AWA_POOL_MAX` | `awa serve` | Maximum database connections (default `10`) |
| `AWA_POOL_MIN` | `awa serve` | Minimum idle connections (default `2`) |
| `AWA_POOL_IDLE_TIMEOUT` | `awa serve` | Idle connection timeout in seconds (default `300`) |
| `AWA_POOL_MAX_LIFETIME` | `awa serve` | Max connection lifetime in seconds (default `1800`) |
| `AWA_POOL_ACQUIRE_TIMEOUT` | `awa serve` | Connection acquire timeout in seconds (default `10`) |
| `AWA_CACHE_TTL` | `awa serve` | Dashboard query cache TTL in seconds (default `5`) |
| `RUST_LOG` | `awa` CLI and any app using `tracing_subscriber` env filters | Log filter, for example `RUST_LOG=info` |
| `HOSTNAME` | Rust worker runtime | Optional hostname recorded in runtime snapshots |

Notes:

- the Rust and Python libraries do not require `DATABASE_URL`; your code can pass the URL directly
- all `AWA_*` env vars can also be set via the corresponding `--flag` on the `serve` subcommand

## Defaults Worth Remembering

- heartbeat interval: `30s`
- stale-heartbeat rescue tick: `30s`
- stale-heartbeat cutoff: about `90s`
- deadline duration: `5m`
- leader retry interval: `10s`
- promotion tick: `250ms`

## Next

- [Deployment guide](deployment.md)
- [Migration guide](migrations.md)
- [Troubleshooting](troubleshooting.md)

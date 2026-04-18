# Grafana Dashboards for Awa

Two dashboards are provided:

- **`awa-dashboard.json`** — Prometheus / OTel metrics dashboard. Requires an OTLP collector (e.g., Grafana LGTM, Prometheus + OTLP receiver). Shows time-series metrics: throughput, latency, queue depth, rescues, completion flush performance.
- **`awa-dashboard-postgres.json`** — SQL dashboard querying Postgres directly. No collector needed. Shows queue depth, lag, descriptor health, recent failures, cron schedules, and runtime instances. Queue depth and stat panels read from `queue_state_counts` (cache table, eventually consistent within the ~2s dirty-key recompute window). Lag and recent failure panels use targeted queries on `jobs_hot` with appropriate indexes. Queue-related tables LEFT JOIN `queue_descriptors` so declared display names and owners appear alongside raw queue names, and declared-but-idle queues stay visible. The **Descriptor Health** panel surfaces stale and drifted descriptors (see `docs/architecture.md#control-plane-descriptors` for the model); an empty table = healthy fleet.

### Descriptor metrics on the OTel dashboard

The Prometheus / OTel dashboard surfaces descriptors via two info-style gauges the runtime emits every snapshot tick:

- `awa_queue_info{awa_job_queue, awa_queue_display_name, awa_queue_owner, awa_queue_tags, awa_queue_docs_url, awa_queue_description}` — always 1
- `awa_job_kind_info{awa_job_kind, awa_job_kind_display_name, awa_job_kind_owner, ...}` — always 1

This is the idiomatic Prometheus/OTel pattern (same shape as `kube-state-metrics` `kube_deployment_labels`): the value is a constant and the descriptor fields live in the label set. That keeps cardinality under control — you don't want `display_name` as a label on every `awa_job_completed_total` sample, because a rolling descriptor change would split every metric into new time series. Dashboards lift descriptor fields into panels at query time via a `group_left` join:

```promql
sum by (awa_job_queue, awa_queue_display_name) (
    rate(awa_job_completed_total[$__rate_interval])
    * on(awa_job_queue) group_left(awa_queue_display_name) awa_queue_info
)
```

The dashboard ships three example panels — **Queue Descriptor Catalog**, **Job Kind Descriptor Catalog** (both instant-query tables), and **Throughput by Queue (with display names)** (a live timeseries demonstrating the join). Descriptor drift / stale detection still lives on the Postgres dashboard because deriving it from metrics alone would require emitting per-runtime hash gauges, which the Postgres catalog does more cleanly.

## Prometheus / OTel Dashboard

### Panels

| Panel | Type | What it shows |
|-------|------|---------------|
| **Queue Lag** | Time series | Age of the oldest available job per queue |
| **Queue Depth** | Stacked time series | Current jobs by queue and state (available, running, failed, scheduled, retryable, waiting external) |
| **Job Wait Time (p50/p95/p99)** | Time series | Time from job creation to claim |
| **Job Throughput** | Time series | Completed, failed, retried, cancelled jobs/sec |
| **In-Flight Jobs** | Time series | Currently executing jobs by queue (stacked) |
| **Job Duration (p50/p95/p99)** | Time series | Execution time percentiles by queue |
| **Throughput by Kind (top 10)** | Stacked bars | Completed jobs/sec by job kind (capped at 10) |
| **Claim Latency** | Time series | Postgres dequeue query time (p50/p95) |
| **Claim Batch Size** | Time series | Average jobs claimed per poll cycle |
| **Maintenance Rescues** | Bars | Heartbeat, deadline, callback_timeout rescues |
| **Completion Flush Performance** | Time series | Batch completion write latency |
| **Promotion Throughput** | Time series | Scheduled/retryable jobs promoted per second |
| **Claims / Waiting External** | Time series | Queue claim rate and callback-parked job rate |
| **Error Rate** | Stat | Failed / (completed + failed) percentage |
| **Jobs In Flight** | Stat | Total executing jobs with threshold colours |
| **Throughput** | Stat | Total completed/sec (5m average) |
| **Rescues (5m)** | Stat | Recent rescue count with threshold colours |

Color semantics are consistent across panels: green for healthy/fast paths, orange for warning/tail latency or retries, red for failures/high tail latency, blue for queue intake/backlog, and yellow for callback/external wait states.

## Setup

### 1. Configure OTLP export in your worker

```rust
// In your worker binary, before starting the client:
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::SdkMeterProvider;

let exporter = opentelemetry_otlp::MetricExporter::builder()
    .with_tonic()
    .with_endpoint("http://localhost:4317")
    .build()?;

let provider = SdkMeterProvider::builder()
    .with_periodic_exporter(exporter)
    .build();

opentelemetry::global::set_meter_provider(provider);
```

### 2. Import the dashboard

**Option A: Grafana UI**
1. Open Grafana (default: http://localhost:3000)
2. Go to Dashboards → Import
3. Upload `awa-dashboard.json`
4. Select your Prometheus datasource

**Option B: Provisioning**
Copy `awa-dashboard.json` to your Grafana provisioning directory:
```
/etc/grafana/provisioning/dashboards/awa-dashboard.json
```

**Option C: API**
```bash
# The Grafana API requires a wrapper object around the dashboard JSON
curl -X POST "http://admin:admin@localhost:3000/api/dashboards/db" \
  -H "Content-Type: application/json" \
  -d "{\"dashboard\": $(cat awa-dashboard.json), \"overwrite\": true}"
```

### 3. Verify metrics are flowing

Check Prometheus has awa metrics:
```
curl -s "http://localhost:9090/api/v1/label/__name__/values" | grep awa
```

Expected metrics: `awa_job_completed_total`, `awa_job_in_flight`, `awa_job_duration_seconds_*`, etc.

## Metrics Reference

All metrics use the `awa` OTel meter name and are exported via OTLP to your configured collector.

### Job lifecycle
| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `awa.job.completed` | Counter | kind, queue | Jobs completed |
| `awa.job.failed` | Counter | kind, queue, terminal | Jobs failed |
| `awa.job.retried` | Counter | kind, queue | Jobs retried |
| `awa.job.cancelled` | Counter | kind, queue | Jobs cancelled |
| `awa.job.claimed` | Counter | queue | Jobs claimed from DB |
| `awa.job.in_flight` | UpDownCounter | queue | Currently executing |
| `awa.job.duration` | Histogram (s) | kind, queue | Execution time |
| `awa.job.waiting_external` | Counter | kind, queue | Parked for callback |

### Dispatcher
| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `awa.dispatch.claim_batches` | Counter | queue | Claim queries |
| `awa.dispatch.claim_batch_size` | Histogram | queue | Jobs per claim |
| `awa.dispatch.claim_duration` | Histogram (s) | queue | Claim query time |

### Completion batcher
| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `awa.completion.flushes` | Counter | shard | Flush operations |
| `awa.completion.flush_batch_size` | Histogram | shard | Jobs per flush |
| `awa.completion.flush_duration` | Histogram (s) | shard | Flush time |

### Maintenance
| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `awa.maintenance.promote_batches` | Counter | state | Promotion batches |
| `awa.maintenance.promote_batch_size` | Histogram | state | Jobs promoted |
| `awa.maintenance.promote_duration` | Histogram (s) | state | Promotion time |
| `awa.maintenance.rescues` | Counter | rescue_kind | Jobs rescued |

### Heartbeat
| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `awa.heartbeat.batches` | Counter | — | Heartbeat updates |

### Descriptors (info-style gauges)

Emitted on every `runtime_snapshot_interval` tick. Value is always 1; the useful payload is the label set. Use with `group_left` joins to enrich other panels (see the "Descriptor metrics on the OTel dashboard" section above).

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `awa.queue.info` | Gauge | queue, display_name?, description?, owner?, docs_url?, tags? | Declared queue descriptor (label-join target) |
| `awa.job_kind.info` | Gauge | kind, display_name?, description?, owner?, docs_url?, tags? | Declared job-kind descriptor (label-join target) |

Optional labels are only emitted when the corresponding descriptor field is set (no empty-string series).

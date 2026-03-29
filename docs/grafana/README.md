# Grafana Dashboard for Awa

A pre-built Grafana dashboard for monitoring Awa job queue metrics via Prometheus.

## Panels

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

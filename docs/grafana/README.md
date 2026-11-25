# Grafana Dashboards for Awa

Two dashboards are provided:

- **`awa-dashboard.json`** — Prometheus / OTel metrics dashboard. Requires an OTLP collector (e.g., Grafana LGTM, Prometheus + OTLP receiver). Shows time-series metrics: throughput, latency, queue depth, rescues, completion flush performance.
- **`awa-dashboard-postgres.json`** — SQL dashboard querying Postgres directly. No collector needed. Shows queue depth, lag, descriptor health, recent failures, cron schedules, and runtime instances. Queue depth and backlog panels read from `queue_state_counts` (cache table, eventually consistent within the ~2s dirty-key recompute window). Lag and recent-failure panels read through the `awa.jobs` compatibility view, so they follow the active queue-storage schema instead of assuming `jobs_hot` is the live worker engine. Queue-related tables LEFT JOIN `queue_descriptors` so declared display names and owners appear alongside raw queue names, and declared-but-idle queues stay visible. The **Descriptor Health** panel surfaces stale and drifted descriptors (see `docs/architecture.md#descriptors-and-runtime-liveness` for the model); an empty table = healthy fleet.

Previews (live demo workload, both dashboards rendered against a local `grafana/otel-lgtm` collector + Postgres):

| Postgres                          | OTel / Prometheus             |
| --------------------------------- | ----------------------------- |
| ![](screenshots/awa-postgres.png) | ![](screenshots/awa-otel.png) |

Alert rules (see [`alerts/`](alerts/)) import as Grafana unified-alerting file provisioning. Here's the rule browser after provisioning both variants, with the "no active runtime (Postgres)" rule firing after we stopped the demo worker:

![](screenshots/awa-alerts.png)

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
| --- | --- | --- |
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

**Option B: Provisioning** Copy `awa-dashboard.json` to your Grafana provisioning directory:

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
| --- | --- | --- | --- |
| `awa.job.completed` | Counter | kind, queue | Jobs completed |
| `awa.job.failed` | Counter | kind, queue, terminal | Jobs failed |
| `awa.job.retried` | Counter | kind, queue | Jobs retried |
| `awa.completion.deadlock_retry` | Counter | kind, queue | Finalize transactions re-run after Postgres aborted them as deadlock victims (SQLSTATE 40P01); non-zero means job transitions are waiting on each other |
| `awa.job.cancelled` | Counter | kind, queue | Jobs cancelled |
| `awa.job.claimed` | Counter | queue | Jobs claimed from DB |
| `awa.job.in_flight` | UpDownCounter | queue | Currently executing |
| `awa.job.duration` | Histogram (s) | kind, queue | Execution time |
| `awa.job.wait_duration` | Histogram (s) | kind, queue | Time from creation to claim; explicit buckets include 1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, and 1s |
| `awa.job.waiting_external` | Counter | kind, queue | Parked for callback |

### Dispatcher

| Metric                          | Type          | Labels | Description      |
| ------------------------------- | ------------- | ------ | ---------------- |
| `awa.dispatch.claim_batches`    | Counter       | queue  | Claim queries    |
| `awa.dispatch.claim_batch_size` | Histogram     | queue  | Jobs per claim   |
| `awa.dispatch.claim_duration`   | Histogram (s) | queue  | Claim query time |

### Completion batcher

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `awa.completion.flushes` | Counter | shard | Flush operations |
| `awa.completion.flush_batch_size` | Histogram | shard | Jobs per flush |
| `awa.completion.flush_duration` | Histogram (s) | shard | Flush time |

### Maintenance

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `awa.maintenance.promote_batches` | Counter | state | Promotion batches |
| `awa.maintenance.promote_batch_size` | Histogram | state | Jobs promoted |
| `awa.maintenance.promote_duration` | Histogram (s) | state, error.type | Promotion time. A batch that failed is recorded with `error.type`; the batch counter and size count successful batches only |
| `awa.maintenance.branch.duration` | Histogram (s) | branch, error.type | Wall-clock time of each run of a maintenance leader branch (`promote_scheduled`, `rescue_heartbeat`, `rotate_claim`, …). A run that hit an error carries `error.type`, so a branch failing on every tick is `rate(awa_maintenance_branch_duration_seconds_count{error_type!=""}[5m]) > 0`, whichever runtime leads |
| `awa.maintenance.promote.lane_contention` | Counter | state, queue, outcome | Promotion lanes whose lock was busy: `handed_off` to a dedicated waiter, `waiter_limit` when every waiter was busy, `waiter_timed_out` when a waiter's 30 s wait ran out |
| `awa.maintenance.rescues` | Counter | rescue_kind | Jobs rescued |
| `awa.maintenance.rescue.pass_size` | Histogram | rescue_kind, rescue_pass_end | Attempts rescued per queue-storage rescue pass, and why the pass stopped (`drained`, `time_budget`, `row_limit`, `owners_unconfirmed`, `error`) |
| `awa.maintenance.rescue.oldest_attempt_age` | Gauge (s) | rescue_kind | Age of the oldest attempt the latest queue-storage rescue pass on this process rescued; `0` when it rescued none. A former leader retains its last value until it runs another pass; filter to the current leader |
| `awa.maintenance.prune.pinned_ready_age` | Gauge (s) | ring, queue, priority, enqueue_shard | Age of the first pending ready row of each lane keeping queue prune from reclaiming the oldest sealed queue ring slot (prune reason `queue.pending_ready`); `0` once the lane no longer pins it. See [queue ring stops wrapping behind an unclaimed lane](../troubleshooting.md#queue-ring-stops-wrapping-behind-an-unclaimed-lane) |
| `awa.maintenance.prune.claim_cursor_advances` | Counter | ring, outcome, queue, priority, enqueue_shard | Claim-cursor advances the maintenance leader made (`outcome` `advanced`), or failed to make (`failed`), over the moved, reprioritised, cancelled or already-claimed rows at the head of a lane pinning the oldest sealed queue ring slot; one per lane per attempt. See [queue ring stops wrapping behind an unclaimed lane](../troubleshooting.md#queue-ring-stops-wrapping-behind-an-unclaimed-lane) |
| `awa.maintenance.leader` | Gauge | runtime.instance_id | `1` on the runtime holding the maintenance leader lock, `0` on every other runtime, one series per runtime even when several clients share a process. Summed over a fleet it is `1`: `0` means nothing promotes, rescues or prunes, and more than `1` past a handover means two leaders |
| `awa.maintenance.leader_eligible` | Gauge | — | `1` when this runtime contends for maintenance leadership, `0` when it was started with `maintenance_leader(false)` |
| `awa.maintenance.leader_eligible_runtimes` | Gauge | — | Recorded only by runtimes started with `maintenance_leader(false)`: how many other live runtimes may take leadership. `0` means none will |

`error.type` is a low-cardinality class, never an error message: `bind_limit` (more bind parameters than Postgres accepts), `timeout` (`statement_timeout`, a cancelled statement, or no pool connection in time), `lock_not_available` (`lock_timeout` or `NOWAIT`), `deadlock`, `serialization_failure`, `connection`, `database` for any other database error, or the `AwaError` variant (`validation`, `serialization`, …).

### Queue health

Published by the maintenance leader every `queue_stats_interval`.

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `awa.queue.depth` | Gauge | queue, state | Jobs per queue and state |
| `awa.queue.lag` | Gauge (s) | queue | Age of the oldest available job |
| `awa.queue.due.depth` | Gauge | queue, state (`scheduled`, `retryable`) | Deferred jobs whose `run_at` has passed but that are not yet promoted |
| `awa.queue.due.lag` | Gauge (s) | queue, state (`scheduled`, `retryable`) | Age of the oldest of those, `0` when none is due: the deferred counterpart of `awa.queue.lag`. It grows when promotion stops, which `awa.queue.depth{state="scheduled"}` can't tell from work scheduled for later |

### Heartbeat

| Metric                  | Type    | Labels | Description       |
| ----------------------- | ------- | ------ | ----------------- |
| `awa.heartbeat.batches` | Counter | —      | Heartbeat updates |

### Descriptors (info-style gauges)

Emitted on every `runtime_snapshot_interval` tick. Value is always 1; the useful payload is the label set. Use with `group_left` joins to enrich other panels (see the "Descriptor metrics on the OTel dashboard" section above).

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `awa.queue.info` | Gauge | queue, display_name?, description?, owner?, docs_url?, tags? | Declared queue descriptor (label-join target) |
| `awa.job_kind.info` | Gauge | kind, display_name?, description?, owner?, docs_url?, tags? | Declared job-kind descriptor (label-join target) |

Optional labels are only emitted when the corresponding descriptor field is set (no empty-string series).

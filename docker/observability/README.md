# Observability side-stack

OTel collector + Prometheus + Grafana for visualising metrics from the
long-horizon bench harness. Independent of `benchmarks/portable/docker-compose.yml`,
so it can be brought up and down without disturbing the bench's postgres.

## Quick start

```bash
cd docker/observability
./render-dashboard.sh                # bake the awa dashboard JSON
docker compose up -d
```

Then run the bench with the OTLP endpoint set:

```bash
cd ../../benchmarks/portable
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
  OTEL_SERVICE_NAME=awa-portable-bench \
  uv run python long_horizon.py \
    --systems awa --replicas 4 --worker-count 8 --producer-rate 200 \
    --phase warmup=warmup:5m --phase clean_1=clean:25m \
    --skip-build
```

Grafana lives at <http://localhost:13000> (admin/admin). The Awa dashboard
is auto-loaded under the **Awa** folder.

Prometheus is at <http://localhost:19090>; the OTel collector's scrape
endpoint is at <http://localhost:8889/metrics>.

## What it captures

Every metric `awa-worker/src/metrics.rs` declares — including the ring-rotation
and prune panels added in commit `3e8fb46`:

- `awa_maintenance_rotate_attempts_total{awa_ring,awa_ring_outcome,awa_ring_blocker}`
- `awa_maintenance_rotate_skipped_rows_bucket` (histogram)
- `awa_maintenance_prune_attempts_total{awa_ring,awa_ring_outcome,awa_ring_reason}`
- `awa_maintenance_prune_skipped_rows_bucket` (histogram)
- `awa_ring_current_slot{awa_ring}`
- `awa_ring_generation{awa_ring}`

Plus the existing job/queue/dispatch metrics: `awa_job_*`, `awa_dispatch_*`,
`awa_queue_*`, `awa_completion_*`, `awa_jobs_in_flight`, `awa_dlq_*`,
`awa_storage_*`.

## Resource budget

| service | RSS limit | what it stores |
|---------|-----------|----------------|
| otel-collector | 512 MB | none (forwarding) |
| prometheus | 1 GB | 24h retention |
| grafana | 512 MB | dashboard cache |

A 12 h overnight bench at the 4×8×200/s profile produces ≈70 MB of
prometheus tsdb data. The retention is set to 24 h so back-to-back
overnight runs don't trip the disk pressure auto-purge.

## Tearing down

```bash
docker compose down       # keeps tsdb volume for back-to-back runs
docker compose down -v    # discards tsdb volume too
```

## Cross-references

- [`../../docs/grafana/awa-dashboard.json`](../../docs/grafana/awa-dashboard.json) — canonical dashboard (source of truth)
- [`../../awa-worker/src/metrics.rs`](../../awa-worker/src/metrics.rs) — metric declarations
- [`../../docs/upgrade-0.5-to-0.6.md`](../../docs/upgrade-0.5-to-0.6.md) — operator checklist with watch-list panel references

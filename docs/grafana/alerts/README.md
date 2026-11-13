# Awa Grafana alert rules

Two rule sets, pick whichever matches your observability stack:

- **`awa-alerts-prometheus.yaml`** — reads OTLP metrics exported by awa workers (`awa_queue_lag_seconds`, `awa_job_completed_total`, …). Richer and cheaper to evaluate. Use this if you're already running a Prometheus-compatible datasource.
- **`awa-alerts-postgres.yaml`** — reads directly from the awa Postgres schema. Use this when you can't (or don't want to) stand up an OTel collector. Queries are slightly heavier than their Prometheus equivalents; the "throughput collapsed" rule is omitted because a 1-hour trailing baseline in raw SQL every minute is too expensive — "queue lag high" + "error rate elevated" together cover the same ground at steady state.

## What fires, and when

| Rule | Severity | Fires when | Clears |
| --- | --- | --- | --- |
| `awa_queue_lag_high` | warning | oldest `available` job > 5m for 2m | lag drops below 5m |
| `awa_error_rate_elevated` | warning | failed / (completed + failed) > 5% for 5m | ratio drops below 5% |
| `awa_rescues_spiking` _(Prom only)_ | warning | rescues > 1/s for 5m | rescue rate drops |
| `awa_no_active_runtime` | critical | active runtimes = 0 for 5m | at least one runtime reporting |
| `awa_throughput_collapsed` _(Prom only)_ | warning | completion rate < 10% of 1h baseline for 10m | rate recovers |
| `awa_descriptor_drift_persistent` | info | descriptor drift detected for 15m | all live runtimes agree |
| `awa_job_output_schema_missing` _(Prom only)_ | warning | a queue drops completion outputs because its schema predates migration v052, for 10m | `awa migrate` applies v052 |
| `awa_staged_follow_ups_discarded` _(Prom only)_ | info | a kind discarded staged follow-ups (returned `WaitForCallback` after staging) in each of the last 15m | no discards for 15m |
| `awa_ring_rotation_stalled` _(Prom only)_ | warning | a ring skipped every rotation as busy for 15m, for 5m | the ring rotates |
| `awa_ring_prune_blocked` _(Prom only)_ | warning | every prune of a ring in 30m was blocked by a lock and none succeeded, for 5m | a prune succeeds |
| `awa_maintenance_branch_overrunning` _(Prom only)_ | info | a maintenance branch overran its interval in every 10m window, for 15m | no overruns for 10m |
| `awa_dlq_growing` _(Prom only)_ | info | a queue's DLQ depth rose over the last hour, for 30m | depth stops rising |
| `awa_pg_long_transaction` _(Postgres only)_ | warning | the oldest open transaction on the database is older than 5m, for 2m | it commits or aborts |

The ring rules read awa's maintenance counters. A lease or claim ring cannot rotate past an attempt that is still running, so workloads whose jobs routinely run longer than 15 minutes should lengthen `awa_ring_rotation_stalled`'s window or its `for`.

Transaction age comes from `pg_stat_activity`, which awa doesn't export, so only the Postgres rule set has it. With postgres_exporter, `pg_stat_activity_max_tx_duration` gives the same signal in Prometheus.

Every rule's `description` annotation names the dashboard panel an operator should look at and links to the relevant awa-ui page. The 2–15 minute `for` windows deliberately skip transient spikes (rolling deploys, CI runs).

## Importing

### Option A — Grafana provisioning (recommended)

Drop the yaml file into Grafana's provisioning directory, substituting your datasource uid:

```bash
# Replace DS_PROMETHEUS with your actual uid (check /datasources in Grafana UI)
sed 's/DS_PROMETHEUS/prom/' docs/grafana/alerts/awa-alerts-prometheus.yaml \
  | sudo tee /etc/grafana/provisioning/alerting/awa.yaml
# Or for Postgres-only:
sed 's/DS_POSTGRES/awa-pg/' docs/grafana/alerts/awa-alerts-postgres.yaml \
  | sudo tee /etc/grafana/provisioning/alerting/awa.yaml
# Restart Grafana so it picks up the new provisioning file:
systemctl reload grafana-server
```

### Option B — Grafana HTTP API

```bash
# Rewrite the datasource placeholder, then POST per rule group.
sed 's/DS_PROMETHEUS/prom/' docs/grafana/alerts/awa-alerts-prometheus.yaml \
  > /tmp/awa-alerts.yaml

# Grafana's alerting provisioning API expects JSON per rule; use
# `grafana-cli` or the yaml-to-JSON conversion of your choice. The
# simplest path in production is provisioning (Option A) — the API is
# most useful for mutating rules from automation.
```

## Validating the rules locally

For the Prometheus rule set, run the observability side-stack in `docker/observability/` and point a local worker or benchmark at `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317`. For the Postgres rule set, provision a Postgres datasource that can read the Awa schema and use Grafana's rule test UI after replacing `DS_POSTGRES` with that datasource UID.

## Extending

If you need an alert that's not in the shipped set:

1. Prefer the Prometheus variant — alert evaluation is much cheaper there.
2. Use a `for:` window ≥ twice the runtime's `runtime_snapshot_interval` (10s default) so a brief OTLP export hiccup doesn't page on-call.
3. Include a pointer to the dashboard panel and the relevant awa-ui page in the `description` annotation — operators responding to alerts at 03:00 will thank you.

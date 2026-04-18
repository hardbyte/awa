# Awa Grafana alert rules

Two rule sets, pick whichever matches your observability stack:

- **`awa-alerts-prometheus.yaml`** — reads OTLP metrics exported by awa workers (`awa_queue_lag_seconds`, `awa_job_completed_total`, …). Richer and cheaper to evaluate. Use this if you're already running a Prometheus-compatible datasource.
- **`awa-alerts-postgres.yaml`** — reads directly from the awa Postgres schema. Use this when you can't (or don't want to) stand up an OTel collector. Queries are slightly heavier than their Prometheus equivalents; the "throughput collapsed" rule is omitted because a 1-hour trailing baseline in raw SQL every minute is too expensive — "queue lag high" + "error rate elevated" together cover the same ground at steady state.

## What fires, and when

| Rule | Severity | Fires when | Clears |
|------|----------|-----------|--------|
| `awa_queue_lag_high` | warning | oldest `available` job > 5m for 2m | lag drops below 5m |
| `awa_error_rate_elevated` | warning | failed / (completed + failed) > 5% for 5m | ratio drops below 5% |
| `awa_rescues_spiking` *(Prom only)* | warning | rescues > 1/s for 5m | rescue rate drops |
| `awa_no_active_runtime` | critical | active runtimes = 0 for 5m | at least one runtime reporting |
| `awa_throughput_collapsed` *(Prom only)* | warning | completion rate < 10% of 1h baseline for 10m | rate recovers |
| `awa_descriptor_drift_persistent` | info | descriptor drift detected for 15m | all live runtimes agree |

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

`tmp/test_alerts.sh` (not committed) can be used to exercise these rules against a local Grafana + otel-lgtm stack; see the "Dashboard screenshots" section of `docs/grafana/README.md` for the demo-data runner that makes them fire.

## Extending

If you need an alert that's not in the shipped set:

1. Prefer the Prometheus variant — alert evaluation is much cheaper there.
2. Use a `for:` window ≥ twice the runtime's `runtime_snapshot_interval` (10s default) so a brief OTLP export hiccup doesn't page on-call.
3. Include a pointer to the dashboard panel and the relevant awa-ui page in the `description` annotation — operators responding to alerts at 03:00 will thank you.

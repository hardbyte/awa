#!/usr/bin/env bash
# Run the full telemetry e2e locally: metrics + traces + dashboards, same
# assertions CI runs in the telemetry-validation job.
#
#   1. Starts grafana/otel-lgtm (collector + Prometheus + Tempo + Grafana)
#      unless OTLP :4317 is already serving.
#   2. Runs the Rust telemetry suite (metrics→Prometheus, spans→Tempo with
#      ADR-039 topology, dashboard panel data).
#   3. Runs the cross-language Python leg (python producer → rust execute →
#      python handler as one trace). Requires the awa-python dev venv:
#      `cd awa-python && uv sync && uv run maturin develop`.
#   4. Validates the published Grafana assets against the live stack.
#
# Leaves the stack running so you can eyeball Grafana at
# http://localhost:3000 (Explore → Tempo for traces). Stop it with:
#   docker stop awa-otel-lgtm
#
# Requires a Postgres for DATABASE_URL (default localhost:15432/awa_test).
set -euo pipefail
cd "$(dirname "$0")/.."

export DATABASE_URL="${DATABASE_URL:-postgres://postgres:test@localhost:15432/awa_test}"
export OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT:-http://localhost:4317}"
export PROMETHEUS_URL="${PROMETHEUS_URL:-http://localhost:9090}"
export TEMPO_URL="${TEMPO_URL:-http://localhost:3200}"
export GRAFANA_URL="${GRAFANA_URL:-http://localhost:3000}"

if ! curl -sf "$PROMETHEUS_URL/api/v1/status/runtimeinfo" >/dev/null 2>&1; then
  echo "── starting grafana/otel-lgtm"
  docker rm -f awa-otel-lgtm >/dev/null 2>&1 || true
  docker run -d --name awa-otel-lgtm \
    -p 4317:4317 -p 4318:4318 -p 9090:9090 -p 3000:3000 -p 3200:3200 \
    grafana/otel-lgtm:0.22.0 >/dev/null
  for i in $(seq 1 60); do
    curl -sf "$PROMETHEUS_URL/api/v1/status/runtimeinfo" >/dev/null 2>&1 && break
    sleep 2
  done
fi
echo "── LGTM stack up (Grafana: $GRAFANA_URL, Tempo: $TEMPO_URL)"

echo "── Rust telemetry suite (metrics, traces, dashboard panels)"
SQLX_OFFLINE=true cargo test -p awa --test telemetry_test -- --ignored --nocapture

echo "── Python cross-language trace leg"
if [ -x awa-python/.venv/bin/python ]; then
  (cd awa-python && uv run --with opentelemetry-exporter-otlp-proto-grpc \
    python ../scripts/trace-e2e-python.py)
else
  echo "  SKIPPED: awa-python venv missing (cd awa-python && uv sync && uv run maturin develop)"
fi

echo "── Grafana dashboard/alert validation"
scripts/validate-grafana.sh

echo
echo "All telemetry e2e checks passed. Grafana: $GRAFANA_URL (anonymous admin),"
echo "traces in Explore → Tempo, dashboards imported as 'AWA Job Queue'."

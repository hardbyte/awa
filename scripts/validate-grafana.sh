#!/usr/bin/env bash
# Validate the Grafana assets we publish (docs/grafana/) against a LIVE
# stack — run AFTER a telemetry workload has exported metrics, so drift
# between dashboards/alerts and what awa actually emits fails loudly:
#
#   1. Every awa_* identifier referenced by the OTel dashboard's PromQL and
#      the Prometheus alert rules must exist in Prometheus as a metric name
#      or a label name.
#   2. Both dashboard JSONs must import cleanly through the Grafana API
#      (schema validity, not just JSON well-formedness).
#   3. Every rawSql panel on the Postgres dashboard must EXPLAIN against a
#      live awa schema (catches schema drift — views/columns the dashboard
#      still references).
#
# Environment: PROMETHEUS_URL (default :9090), GRAFANA_URL (default :3000),
# DATABASE_URL (default local test instance). Requires: jq, curl, psql.
set -euo pipefail

cd "$(dirname "$0")/.."
PROMETHEUS_URL="${PROMETHEUS_URL:-http://localhost:9090}"
GRAFANA_URL="${GRAFANA_URL:-http://localhost:3000}"
DATABASE_URL="${DATABASE_URL:-postgres://postgres:test@localhost:15432/awa_test}"
GRAFANA_AUTH="${GRAFANA_AUTH:-admin:admin}"
DASHBOARD_OTEL=docs/grafana/awa-dashboard.json
DASHBOARD_PG=docs/grafana/awa-dashboard-postgres.json
ALERTS_PROM=docs/grafana/alerts/awa-alerts-prometheus.yaml
failures=0

# ── 1. Metric/label coverage ─────────────────────────────────────────
echo "── validating awa_* references against live Prometheus"
referenced=$(
  {
    jq -r '[.. | .expr? // empty] | .[]' "$DASHBOARD_OTEL"
    grep -E '^\s*(expr|summary|description):' "$ALERTS_PROM" || true
  } | grep -oE 'awa_[a-z0-9_]+' | sort -u
)
prom_names=$(curl -sf "$PROMETHEUS_URL/api/v1/label/__name__/values" | jq -r '.data[]')
prom_labels=$(curl -sf "$PROMETHEUS_URL/api/v1/labels" | jq -r '.data[]')
known=$(printf '%s\n%s\n' "$prom_names" "$prom_labels" | sort -u)
# Fallback set for condition-gated metrics the validation workload never
# fires (e.g. a SkippedBusy ring rotation): every metric name string
# defined in awa-metrics, normalised to Prometheus form (dots →
# underscores). A dashboard reference must exist in Prometheus OR still be
# defined in source — renames and removals fail either way.
source_names=$(grep -oE '"awa\.[a-z0-9_.]+"' awa-metrics/src/lib.rs | tr -d '"' | tr '.' '_' | sort -u)

missing=0
while IFS= read -r name; do
  if grep -qxF "$name" <<<"$known"; then
    continue
  fi
  base=$(sed -E 's/_(bucket|sum|count|total)$//; s/_seconds$//' <<<"$name")
  if grep -qxF "$base" <<<"$source_names"; then
    echo "  conditional: $name (defined in awa-metrics, not observed in this workload)"
    continue
  fi
  echo "  MISSING: $name (referenced by dashboard/alerts, absent from Prometheus AND awa-metrics)"
  missing=$((missing + 1))
done <<<"$referenced"
if [ "$missing" -gt 0 ]; then
  echo "  $missing referenced awa_* identifiers not found — dashboard/alert drift"
  failures=$((failures + 1))
else
  echo "  OK: $(wc -l <<<"$referenced") awa_* identifiers all present"
fi

# ── 2. Grafana API import ────────────────────────────────────────────
echo "── importing dashboards through the Grafana API"
for dashboard in "$DASHBOARD_OTEL" "$DASHBOARD_PG"; do
  payload=$(jq '{dashboard: (. | .id = null), overwrite: true}' "$dashboard")
  status=$(curl -s -o /tmp/grafana-import-response.json -w '%{http_code}' \
    -u "$GRAFANA_AUTH" -H 'Content-Type: application/json' \
    -X POST "$GRAFANA_URL/api/dashboards/db" -d "$payload")
  if [ "$status" = "200" ]; then
    echo "  OK: $dashboard imported ($(jq -r .url /tmp/grafana-import-response.json))"
  else
    echo "  FAILED: $dashboard import returned $status:"
    sed 's/^/    /' /tmp/grafana-import-response.json
    failures=$((failures + 1))
  fi
done

# ── 3. Postgres dashboard SQL against the live schema ────────────────
echo "── EXPLAINing Postgres dashboard rawSql against live schema"
sql_failures=0
count=0
while IFS= read -r encoded; do
  sql=$(jq -rn --arg s "$encoded" '$s | @base64d')
  count=$((count + 1))
  if ! psql "$DATABASE_URL" -X -q -v ON_ERROR_STOP=1 -c "EXPLAIN $sql" >/dev/null 2>/tmp/explain-err.txt; then
    echo "  FAILED panel SQL #$count:"
    sed 's/^/    /' /tmp/explain-err.txt
    echo "$sql" | head -3 | sed 's/^/    | /'
    sql_failures=$((sql_failures + 1))
  fi
done < <(jq -r '[.. | .rawSql? // empty] | .[] | @base64' "$DASHBOARD_PG")
if [ "$sql_failures" -gt 0 ]; then
  echo "  $sql_failures of $count rawSql panels failed EXPLAIN — schema drift"
  failures=$((failures + 1))
else
  echo "  OK: all $count rawSql panels EXPLAIN cleanly"
fi

# ── 4. Traces readable through Grafana's Tempo datasource ───────────
# Proves the wiring a user actually clicks through (Explore → Tempo), not
# just Tempo's own API. Skips gracefully if no e2e trace has been exported
# yet (validator run standalone).
echo "── reading a trace through Grafana's Tempo datasource"
tempo_uid=$(curl -sf -u "$GRAFANA_AUTH" "$GRAFANA_URL/api/datasources" | jq -r '.[] | select(.type=="tempo") | .uid' | head -1)
if [ -z "$tempo_uid" ]; then
  echo "  FAILED: no Tempo datasource provisioned in Grafana"
  failures=$((failures + 1))
else
  search_q=$(jq -rn '"{ resource.service.name = \"awa-trace-e2e\" }" | @uri')
  trace_id=$(curl -sf -u "$GRAFANA_AUTH" \
    "$GRAFANA_URL/api/datasources/proxy/uid/$tempo_uid/api/search?q=$search_q&limit=1" \
    | jq -r '.traces[0].traceID // empty')
  if [ -z "$trace_id" ]; then
    echo "  SKIPPED: no awa-trace-e2e traces found (run the telemetry suite first)"
  elif curl -sf -u "$GRAFANA_AUTH" \
    "$GRAFANA_URL/api/datasources/proxy/uid/$tempo_uid/api/traces/$trace_id" \
    | jq -e '.batches | length > 0' >/dev/null; then
    echo "  OK: trace $trace_id readable through Grafana → Tempo"
  else
    echo "  FAILED: trace $trace_id not readable through the Grafana Tempo datasource"
    failures=$((failures + 1))
  fi
fi

if [ "$failures" -gt 0 ]; then
  echo "grafana validation FAILED ($failures section(s))"
  exit 1
fi
echo "grafana validation OK"

#!/usr/bin/env bash
# Validate the Grafana assets we publish (docs/grafana/) against a LIVE
# stack — run AFTER a telemetry workload has exported metrics, so drift
# between dashboards/alerts and what awa actually emits fails loudly:
#
#   1. Every awa_* identifier referenced by the OTel dashboards' PromQL and
#      the Prometheus alert rules must be a name awa exports under the
#      OpenTelemetry OTLP-to-Prometheus translation, and every awa_* name
#      Prometheus holds must be one that translation predicts.
#   2. Every dashboard JSON must import cleanly through the Grafana API
#      (schema validity, not just JSON well-formedness).
#   3. Every rawSql panel on the Postgres dashboard must EXPLAIN against a
#      live awa schema (catches schema drift — views/columns the dashboard
#      still references).
#
# Environment: PROMETHEUS_URL (default :9090), GRAFANA_URL (default :3000),
# DATABASE_URL (default local test instance). Requires: jq, curl, psql,
# python3 (with PyYAML, or uv to fetch it).
set -euo pipefail

cd "$(dirname "$0")/.."
PROMETHEUS_URL="${PROMETHEUS_URL:-http://localhost:9090}"
GRAFANA_URL="${GRAFANA_URL:-http://localhost:3000}"
DATABASE_URL="${DATABASE_URL:-postgres://postgres:test@localhost:15432/awa_test}"
GRAFANA_AUTH="${GRAFANA_AUTH:-admin:admin}"
DASHBOARD_OTEL=docs/grafana/awa-dashboard.json
DASHBOARD_QUEUE=docs/grafana/awa-queue-detail.json
DASHBOARD_PG=docs/grafana/awa-dashboard-postgres.json
ALERTS_PROM=docs/grafana/alerts/awa-alerts-prometheus.yaml
failures=0

# ── 1. Metric/label coverage ─────────────────────────────────────────
# Expected names come from the instruments awa-metrics defines, translated
# by the OpenTelemetry OTLP-to-Prometheus rules the collector applies
# (unit suffixes, `_total` on counters, histogram series, `_` for invalid
# characters): see scripts/otel-prometheus-names.py.
echo "── validating awa_* references against the OTel→Prometheus names and live Prometheus"
expected=$(python3 scripts/otel-prometheus-names.py)
referenced=$(
  {
    jq -r '[.. | .expr? // empty] | .[]' "$DASHBOARD_OTEL" "$DASHBOARD_QUEUE"
    grep -E '^\s*(expr|summary|description):' "$ALERTS_PROM" || true
  } | grep -oE 'awa_[a-z0-9_]+' | sort -u
)
prom_names=$(curl -sf "$PROMETHEUS_URL/api/v1/label/__name__/values" | jq -r '.data[]')
prom_labels=$(curl -sf "$PROMETHEUS_URL/api/v1/labels" | jq -r '.data[]')
known=$(printf '%s\n%s\n' "$prom_names" "$prom_labels" | sort -u)

# a) Every reference is a name awa can export. A metric the workload never
#    fires (e.g. a SkippedBusy ring rotation) is still checked this way.
missing=0
while IFS= read -r name; do
  if ! grep -qxF "$name" <<<"$expected"; then
    echo "  MISSING: $name (referenced by dashboard/alerts; no awa-metrics instrument or attribute exports it)"
    missing=$((missing + 1))
  elif ! grep -qxF "$name" <<<"$known"; then
    echo "  conditional: $name (defined in awa-metrics, not observed in this workload)"
  fi
done <<<"$referenced"

# b) Every awa_* series and label the collector exported is one the
#    translation predicts, so the expected names match what Prometheus sees.
unexpected=0
while IFS= read -r name; do
  [ -n "$name" ] || continue
  if ! grep -qxF "$name" <<<"$expected"; then
    echo "  UNEXPECTED: $name (exported to Prometheus, but not the OTel→Prometheus name of any awa-metrics instrument or attribute)"
    unexpected=$((unexpected + 1))
  fi
done < <(grep -E '^awa_' <<<"$known" || true)

if [ "$missing" -gt 0 ] || [ "$unexpected" -gt 0 ]; then
  echo "  $missing missing, $unexpected unexpected awa_* identifiers — dashboard/alert or naming drift"
  failures=$((failures + 1))
else
  echo "  OK: $(wc -l <<<"$referenced") awa_* references all exportable"
fi

# ── 2. Grafana API import ────────────────────────────────────────────
echo "── importing dashboards through the Grafana API"
for dashboard in "$DASHBOARD_OTEL" "$DASHBOARD_QUEUE" "$DASHBOARD_PG"; do
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

# ── 3b. Alert rule queries against the live sources ──────────────────
# Each Prometheus rule's expression must evaluate (a typo in PromQL or a
# label matcher returns an error, not an empty result), and each Postgres
# rule's SQL must EXPLAIN against the live schema.
echo "── evaluating alert rule queries"
ALERTS_PG=docs/grafana/alerts/awa-alerts-postgres.yaml
if python3 -c 'import yaml' 2>/dev/null; then
  yaml_python=(python3)
else
  yaml_python=(uv run --quiet --no-project --with pyyaml python3)
fi
alert_failures=0
alert_count=0
# Every query needs a datasource and a model, a datasource query needs a
# PromQL or SQL body, and the rule's condition must name one of its queries.
# Otherwise Grafana refuses the rule or the checks below skip its query.
if ! "${yaml_python[@]}" -c 'import sys, yaml
bad = []
for path in sys.argv[1:]:
    for group in yaml.safe_load(open(path))["groups"]:
        for rule in group["rules"]:
            refs = [query.get("refId") for query in rule["data"]]
            incomplete = any(
                "model" not in query
                or "datasourceUid" not in query
                or (
                    query["datasourceUid"] != "__expr__"
                    and not (query["model"].get("expr") or query["model"].get("rawSql") or "").strip()
                )
                for query in rule["data"]
            )
            if rule.get("condition") not in refs or incomplete:
                bad.append(rule["uid"])
for uid in bad:
    print(f"  FAILED: alert rule {uid} has an incomplete query or condition")
sys.exit(1 if bad else 0)' "$ALERTS_PROM" "$ALERTS_PG"; then
  alert_failures=$((alert_failures + 1))
fi
while IFS= read -r encoded; do
  expr=$(jq -rn --arg s "$encoded" '$s | @base64d')
  alert_count=$((alert_count + 1))
  status=$(curl -s -G "$PROMETHEUS_URL/api/v1/query" --data-urlencode "query=$expr" | jq -r '.status // "error"')
  if [ "$status" != "success" ]; then
    echo "  FAILED Prometheus alert expr: $expr"
    alert_failures=$((alert_failures + 1))
  fi
done < <("${yaml_python[@]}" -c 'import base64, sys, yaml
for group in yaml.safe_load(open(sys.argv[1]))["groups"]:
    for rule in group["rules"]:
        for query in rule["data"]:
            expr = query["model"].get("expr")
            if expr:
                print(base64.b64encode(expr.encode()).decode())' "$ALERTS_PROM")
while IFS= read -r encoded; do
  sql=$(jq -rn --arg s "$encoded" '$s | @base64d')
  alert_count=$((alert_count + 1))
  if ! psql "$DATABASE_URL" -X -q -v ON_ERROR_STOP=1 -c "EXPLAIN $sql" >/dev/null 2>/tmp/explain-err.txt; then
    echo "  FAILED Postgres alert SQL:"
    sed 's/^/    /' /tmp/explain-err.txt
    alert_failures=$((alert_failures + 1))
  fi
done < <("${yaml_python[@]}" -c 'import base64, sys, yaml
for group in yaml.safe_load(open(sys.argv[1]))["groups"]:
    for rule in group["rules"]:
        for query in rule["data"]:
            sql = query["model"].get("rawSql")
            if sql:
                print(base64.b64encode(sql.encode()).decode())' "$ALERTS_PG")
if [ "$alert_failures" -gt 0 ]; then
  echo "  $alert_failures of $alert_count alert queries failed"
  failures=$((failures + 1))
else
  echo "  OK: all $alert_count alert queries evaluate"
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

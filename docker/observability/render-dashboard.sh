#!/bin/sh
# Render the canonical awa dashboard for the local Grafana provisioning
# directory.
#
# The dashboard at docs/grafana/awa-dashboard.json declares an importable
# datasource variable (`__inputs[].DS_PROMETHEUS`) and references it as
# `${DS_PROMETHEUS}` throughout the panel JSON. Grafana's file-based
# provisioning does not run the interactive import wizard, so the
# variable is never substituted unless we do it ourselves.
#
# We bake the same fixed datasource UID that
# `grafana/provisioning/datasources/prometheus.yml` declares
# (`prometheus_default`) into a copy of the dashboard at
# `grafana/dashboards/`. That copy is what Grafana actually reads.
#
# Run from this directory:
#   ./render-dashboard.sh
#
# Re-run whenever docs/grafana/awa-dashboard.json changes.

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
SRC="$REPO_ROOT/docs/grafana/awa-dashboard.json"
DST_DIR="$SCRIPT_DIR/grafana/dashboards"
DST="$DST_DIR/awa-dashboard.json"

if [ ! -f "$SRC" ]; then
    echo "render-dashboard: missing source $SRC" >&2
    exit 1
fi

mkdir -p "$DST_DIR"
sed 's/${DS_PROMETHEUS}/prometheus_default/g' "$SRC" > "$DST"
echo "rendered: $DST (datasource UID: prometheus_default)"

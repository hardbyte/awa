#!/bin/sh
# Copy the canonical awa dashboards into the local Grafana provisioning
# directory.
#
# The dashboards in docs/grafana/ select their Prometheus datasource with a
# `datasource` template variable, so Grafana's file provisioning loads them
# unmodified and the variable defaults to the datasource provisioned by
# `grafana/provisioning/datasources/prometheus.yml`.
#
# Run from this directory:
#   ./render-dashboard.sh
#
# Re-run whenever a dashboard in docs/grafana/ changes.

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
DST_DIR="$SCRIPT_DIR/grafana/dashboards"

mkdir -p "$DST_DIR"
for name in awa-dashboard awa-queue-detail; do
    SRC="$REPO_ROOT/docs/grafana/$name.json"
    if [ ! -f "$SRC" ]; then
        echo "render-dashboard: missing source $SRC" >&2
        exit 1
    fi
    cp "$SRC" "$DST_DIR/$name.json"
    echo "rendered: $DST_DIR/$name.json"
done

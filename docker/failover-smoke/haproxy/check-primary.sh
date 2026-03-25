#!/bin/sh
set -eu

export PGPASSWORD="${POSTGRES_PASSWORD:-test}"
export PGCONNECT_TIMEOUT=2

result="$(psql \
  -h "$HAPROXY_SERVER_ADDR" \
  -p "$HAPROXY_SERVER_PORT" \
  -U "${POSTGRES_USER:-postgres}" \
  -d "${POSTGRES_DB:-awa_failover_test}" \
  -Atqc "SELECT NOT pg_is_in_recovery()")"

[ "$result" = "t" ]

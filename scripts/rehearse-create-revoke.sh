#!/usr/bin/env bash
# A released awa-pg 0.6 producer and worker run through revoking their schema
# CREATE and `awa migrate`. `control` revokes without preparing; `prepared`
# follows the upgrade guide. Prints the workload's failure and stall summary.
# usage: ADMIN_URL=postgres://postgres:pw@host:port/postgres rehearse-create-revoke.sh control|prepared
set -euo pipefail
mode=${1:?control or prepared}
: "${ADMIN_URL:?set ADMIN_URL to a superuser URL on a disposable PostgreSQL server}"
admin=$ADMIN_URL
repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
dir=$(mktemp -d)
SQLX_OFFLINE=true cargo build --quiet --manifest-path "$repo/Cargo.toml" -p awa-cli
awa_new=$(cargo metadata --no-deps --format-version 1 --manifest-path "$repo/Cargo.toml" | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')/debug/awa
uv venv --quiet --clear "$dir/venv"
uv pip install --quiet --python "$dir/venv" "awa-pg==${AWA_RELEASED_VERSION:-0.6.9}"
py06=$dir/venv/bin/python
suffix=$(date +%s%N | tail -c 8)
owner=rh_owner_$suffix runtime=rh_runtime_$suffix db=rh_db_$suffix
host=${admin#*@}; host=${host%%/*}
owner_url=postgres://$owner:rh@$host/$db
runtime_url=postgres://$runtime:rh@$host/$db
log() { echo "$(date +%T) [$mode] $*" >&2; }

psql -q "$admin" -c "CREATE ROLE $owner LOGIN PASSWORD 'rh'" -c "CREATE ROLE $runtime LOGIN PASSWORD 'rh'" \
  -c "CREATE DATABASE $db OWNER $owner"
trap 'psql -q "$admin" -c "DROP DATABASE IF EXISTS $db WITH (FORCE)" -c "DROP ROLE IF EXISTS $runtime" -c "DROP ROLE IF EXISTS $owner" >/dev/null 2>&1 || true' EXIT

log "migrating with released 0.6: $("$py06" -c 'import importlib.metadata as m; print(m.version("awa-pg"))')"
"$py06" -m awa --database-url "$owner_url" migrate >/dev/null
psql -q "$owner_url" <<SQL
GRANT USAGE, CREATE ON SCHEMA awa TO $runtime;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA awa TO $runtime;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA awa TO $runtime;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA awa TO $runtime;
REVOKE EXECUTE ON FUNCTION awa.install_queue_storage_substrate(TEXT, INT, INT, INT, BOOLEAN) FROM $runtime;
ALTER DEFAULT PRIVILEGES IN SCHEMA awa GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO $runtime;
ALTER DEFAULT PRIVILEGES IN SCHEMA awa GRANT USAGE, SELECT ON SEQUENCES TO $runtime;
ALTER DEFAULT PRIVILEGES IN SCHEMA awa GRANT EXECUTE ON FUNCTIONS TO $runtime;
SQL
log "schema v$(psql -At "$owner_url" -c 'SELECT max(version) FROM awa.schema_version')"

REHEARSAL_DIR=$dir RUNTIME_DATABASE_URL=$runtime_url "$py06" "$repo/scripts/compat/create_revoke_workload.py" >"$dir/summary.json" 2>"$dir/workload.err" &
workload=$!
t0=$(date +%s.%N)
mark() { log "t=$(echo "$(date +%s.%N) - $t0" | bc | cut -c1-5) $*"; }
sleep 10

if [ "$mode" = prepared ]; then
  mark "prepare-create-revoke start"
  "$awa_new" --database-url "$owner_url" storage prepare-create-revoke >"$dir/prepare.json"
  jq -c '.[] | {schema, prepared, provisioned_lanes, revoke, grants: (.sequence_grants | length), blocking}' "$dir/prepare.json" >&2
  jq -r '.[].sequence_grants[] | select(.run_as == "'"$runtime"'") | .statement' "$dir/prepare.json" | psql -q "$runtime_url"
  jq -r '.[].revoke[]' "$dir/prepare.json" | psql -q "$owner_url"
  mark "grants and revokes done"
else
  log "revoking CREATE without preparing"
  psql -q "$owner_url" -c "REVOKE CREATE ON SCHEMA awa FROM $runtime"
fi
touch "$dir/all-priorities"
sleep 10

if [ "$mode" = prepared ]; then
  mark "awa migrate start"
  ( while [ ! -e "$dir/migrated" ]; do psql -At "$owner_url" -c "SELECT string_agg(DISTINCT format('%s waits %s on %s', w.pid, w.mode, COALESCE(w.relation::regclass::text, w.locktype)), '; ') FROM pg_locks w WHERE NOT w.granted" 2>/dev/null; sleep 0.1; done | grep -v '^$' | sort | uniq -c | sort -rn | head -8 >"$dir/waits.txt" ) &
  "$awa_new" --database-url "$owner_url" migrate >"$dir/migrate.log" 2>&1 || { log "migrate failed"; tail -5 "$dir/migrate.log" >&2; }
  touch "$dir/migrated"; sleep 0.3; cat "$dir/waits.txt" >&2
  mark "awa migrate done, schema v$(psql -At "$owner_url" -c 'SELECT max(version) FROM awa.schema_version')"
  sleep 10
fi
touch "$dir/stop"
wait "$workload" || true
tail -3 "$dir/workload.err" >&2 || true
echo "$mode $(cat "$dir/summary.json")"

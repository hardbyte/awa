#!/usr/bin/env bash
# Runs the persona soak (awa/tests/persona_soak_test.rs) against a matrix of
# throwaway PostgreSQL containers, one engine after another.
#
#   scripts/persona-soak.sh [engine ...]
#
# Engines: pg17 (postgres:17), pg18 (postgres:18) and pg18-cassert (an
# assert-enabled, -O0 build of PostgreSQL 18; set SOAK_CASSERT_IMAGE). With no
# arguments all three run in that order.
#
# Environment:
#   AWA_SOAK_SECONDS          load phase length per engine (default 900)
#   AWA_SOAK_PERSONAS         comma list, default all of mia,tom,priya,sam,alex,chaos,
#                             watchers,support,uiops,impatient,python
#   AWA_SOAK_SEED             fixes the workload mix and chaos schedule
#   AWA_SOAK_SCALE            multiplies rates and queue counts (default 1)
#   SOAK_RESULTS_DIR          output directory (default ./soak-results/<timestamp>)
#   SOAK_BASE_PORT            first host port; engines use consecutive ports (default 15471)
#   SOAK_MAX_CONNECTIONS      server max_connections (default 400)
#   SOAK_CASSERT_IMAGE        image for pg18-cassert (default awa-pg18-cassert:18.6-O0-epqlog5)
#   SOAK_SKIP_PREPARE         set to 1 to skip building the awa binary and the Python bindings
#   CARGO_TARGET_DIR          cargo target directory, as for any cargo build
#
# The admin UI personas drive the real `awa serve` binary and the python persona runs
# the awa-python bindings in a separate process; both are built once, before the first
# engine, with `cargo build --release -p awa-cli` and `uv` + `maturin develop --release`
# into awa-python/.venv.
#
# Every container is private to the run (name awa-soak-<engine>-<pid>) and is
# removed on exit, including on failure or interruption. Containers that already
# exist are never touched.
set -uo pipefail

cd "$(dirname "$0")/.."

SECONDS_PER_ENGINE="${AWA_SOAK_SECONDS:-900}"
BASE_PORT="${SOAK_BASE_PORT:-15471}"
MAX_CONNECTIONS="${SOAK_MAX_CONNECTIONS:-400}"
CASSERT_IMAGE="${SOAK_CASSERT_IMAGE:-awa-pg18-cassert:18.6-O0-epqlog5}"
RESULTS_DIR="${SOAK_RESULTS_DIR:-$PWD/soak-results/$(date +%Y%m%d-%H%M%S)}"
ENGINES=("$@")
if [ ${#ENGINES[@]} -eq 0 ]; then
  ENGINES=(pg17 pg18 pg18-cassert)
fi

mkdir -p "$RESULTS_DIR"

prepare() {
  echo "== building the awa binary and the Python bindings"
  cargo build --release -p awa-cli || return 1
  if [ ! -x awa-python/.venv/bin/python ]; then
    uv venv --quiet awa-python/.venv || return 1
    uv pip install --quiet --python awa-python/.venv maturin || return 1
  fi
  (cd awa-python && VIRTUAL_ENV="$PWD/.venv" PATH="$PWD/.venv/bin:$PATH" maturin develop --release) || return 1
}
if [ "${SOAK_SKIP_PREPARE:-0}" != 1 ]; then
  prepare >"$RESULTS_DIR/prepare.log" 2>&1 || { echo "prepare failed; see $RESULTS_DIR/prepare.log" >&2; exit 2; }
fi
export AWA_SOAK_PYTHON="${AWA_SOAK_PYTHON:-$PWD/awa-python/.venv/bin/python}"
export AWA_SOAK_AWA_BIN="${AWA_SOAK_AWA_BIN:-${CARGO_TARGET_DIR:-$PWD/target}/release/awa}"
CONTAINERS=()
STATS_PID=""

cleanup() {
  [ -n "$STATS_PID" ] && kill "$STATS_PID" 2>/dev/null
  for container in "${CONTAINERS[@]:-}"; do
    [ -n "$container" ] && docker rm -f "$container" >/dev/null 2>&1
  done
}
trap cleanup EXIT
trap 'exit 130' INT TERM

start_container() {
  local engine="$1" name="$2" port="$3"
  local common=(-c "max_connections=$MAX_CONNECTIONS" -c max_locks_per_transaction=256 -c shared_buffers=512MB)
  case "$engine" in
    pg17)
      docker run -d --name "$name" --shm-size=1g -e POSTGRES_PASSWORD=test -p "$port:5432" postgres:17 "${common[@]}" >/dev/null ;;
    pg18)
      docker run -d --name "$name" --shm-size=1g -e POSTGRES_PASSWORD=test -p "$port:5432" postgres:18 "${common[@]}" >/dev/null ;;
    pg18-cassert)
      # The image ships an initialised, trust-authenticated cluster in /data. Its server
      # logs an AWAEPQ line, with the full query text, for every partition-pruning
      # initialisation, which under this workload is tens of gigabytes per hour. The log
      # collector therefore writes into a FIFO whose reader drops those messages and keeps
      # the rest (assertion failures, crash notices, errors) in /data/log/pg-filtered.log.
      # Parallel query is off here: at -O0 a handful of admin reads would otherwise take every core.
      docker run -d --name "$name" --shm-size=1g -p "$port:5432" "$CASSERT_IMAGE" sh -c '
        mkdir -p /data/log
        rm -f /data/log/pg.log /data/log/pg-filtered.log
        mkfifo /data/log/pg.log
        awk -W interactive "
          /^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] /{
            pid=\"\"; if (match(\$0,/\\[[0-9]+\\]/)) pid=substr(\$0,RSTART,RLENGTH)
            if (\$0 ~ /AWAEPQ/) {skip=1; epq[pid]=1}
            else if (\$0 ~ /STATEMENT:/ && (pid in epq)) {skip=1; delete epq[pid]}
            else {skip=0; delete epq[pid]}
          }
          !skip{print; fflush()}" \
          </data/log/pg.log >>/data/log/pg-filtered.log &
        exec postgres -D /data -c listen_addresses="*" -c max_connections='"$MAX_CONNECTIONS"' \
          -c max_locks_per_transaction=256 -c shared_buffers=512MB -c track_activity_query_size=65536 \
          -c max_parallel_workers_per_gather=0 \
          -c logging_collector=on -c log_directory=/data/log -c log_filename=pg.log -c log_rotation_size=0' >/dev/null ;;
    *)
      echo "unknown engine: $engine (expected pg17, pg18 or pg18-cassert)" >&2
      return 1 ;;
  esac
}

wait_ready() {
  local name="$1" deadline=$((SECONDS + 120))
  until docker exec "$name" psql -U postgres -d postgres -Atc 'select 1' >/dev/null 2>&1; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "container $name did not become ready" >&2
      docker logs --tail 40 "$name" >&2
      return 1
    fi
    sleep 1
  done
  sleep 2
}

sample_stats() {
  local name="$1" out="$2"
  echo "time,cpu_percent,mem" >"$out"
  while true; do
    docker stats --no-stream --format '{{.CPUPerc}},{{.MemUsage}}' "$name" 2>/dev/null |
      sed "s/^/$(date +%H:%M:%S),/" >>"$out"
    sleep 15
  done
}

overall=0
for index in "${!ENGINES[@]}"; do
  engine="${ENGINES[$index]}"
  port=$((BASE_PORT + index))
  name="awa-soak-$engine-$$"
  out="$RESULTS_DIR/$engine"
  mkdir -p "$out"
  CONTAINERS+=("$name")

  echo "== $engine on port $port (container $name)"
  start_container "$engine" "$name" "$port" || exit 2
  wait_ready "$name" || exit 2

  log_env=(AWA_SOAK_PG_CONTAINER="$name")
  [ "$engine" = pg18-cassert ] && log_env+=(AWA_SOAK_PG_CONTAINER_LOG=/data/log/pg-filtered.log)

  sample_stats "$name" "$out/container-stats.csv" &
  STATS_PID=$!

  env AWA_SOAK_DATABASE_URL="postgres://postgres:test@localhost:$port/awa_soak_${engine//-/_}" \
    AWA_SOAK_SECONDS="$SECONDS_PER_ENGINE" \
    AWA_SOAK_REPORT_DIR="$out" \
    "${log_env[@]}" \
    cargo test --release -p awa --test persona_soak_test -- --ignored --nocapture \
    >"$out/run.log" 2>&1
  status=$?

  kill "$STATS_PID" 2>/dev/null
  STATS_PID=""
  if [ $status -ne 0 ]; then
    overall=1
    echo "   FAILED (exit $status); see $out/report.md and $out/reproducer.md"
    # Keep the server log with the reproducer when the run failed.
    docker logs "$name" >"$out/server.log" 2>&1 || true
    [ "$engine" = pg18-cassert ] && docker exec "$name" cat /data/log/pg-filtered.log >"$out/server.log" 2>/dev/null
  else
    echo "   passed; report in $out/report.md"
  fi
  docker rm -f "$name" >/dev/null 2>&1
done

echo "results: $RESULTS_DIR"
exit $overall

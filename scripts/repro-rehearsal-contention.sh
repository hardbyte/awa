#!/usr/bin/env bash
# Reproduce timing-sensitive rolling_upgrade_rehearsal_test.rs (#427) flakes
# locally by loop-running it under a constrained CPU set.
#
# The rehearsal races live traffic against a real schema migration and, in
# the mixed-fleet cells, against a hard-killed released worker. On a
# many-core dev box those races basically never land: everything finishes
# well inside the staleness/timeout margins. GitHub's hosted runners give
# the job 2 vCPUs, which widens the same windows enough to occasionally lose
# them — that gap is why flakes like the cron double-fire and the
# simple-queue terminal-count mismatch showed up in CI but not locally.
# `taskset -c 0,1` closes that gap by pinning this shell to 2 cores too.
#
# Usage:
#   scripts/repro-rehearsal-contention.sh [iterations] [test-name-filter]
#
#   iterations         default 20
#   test-name-filter   default: run every #[ignore]'d test in the file
#                       (pass a substring, e.g. "full_workload", to narrow it)
#
# Env:
#   DATABASE_URL          default postgres://postgres:test@localhost:15432/awa_test
#   AWA_N_MINUS_ONE_VER   released N-1 awa-pg version to rehearse against, default 0.6.3
#   AWA_N_MINUS_ONE_PYTHON  path to an existing interpreter; skips venv setup if set
#
# On a failure, the full test output is kept under
# ./rehearsal-repro-logs/run-<N>.log for post-mortem (grep for "panicked at",
# then cross-reference job ids / timestamps against the database — see the
# cron double-fire and simple-queue investigations for the pattern).
set -euo pipefail

ITERATIONS=${1:-20}
TEST_FILTER=${2:-}
DATABASE_URL=${DATABASE_URL:-postgres://postgres:test@localhost:15432/awa_test}
AWA_N_MINUS_ONE_VER=${AWA_N_MINUS_ONE_VER:-0.6.3}
LOG_DIR=rehearsal-repro-logs

if ! psql "$DATABASE_URL" -c "SELECT 1" >/dev/null 2>&1; then
  echo "error: cannot reach DATABASE_URL=$DATABASE_URL" >&2
  echo "  start it, e.g.: docker run -d --name awa-pg -e POSTGRES_PASSWORD=test -e POSTGRES_DB=awa_test -p 15432:5432 postgres:17-alpine" >&2
  exit 1
fi

if [ -z "${AWA_N_MINUS_ONE_PYTHON:-}" ]; then
  VENV=.compat-venv-repro
  echo "── setup: released awa-pg==${AWA_N_MINUS_ONE_VER} in ${VENV}"
  uv venv --quiet --clear "$VENV"
  uv pip install --quiet --python "$VENV" "awa-pg==${AWA_N_MINUS_ONE_VER}"
  AWA_N_MINUS_ONE_PYTHON="$(pwd)/${VENV}/bin/python"
fi
export AWA_N_MINUS_ONE_PYTHON
export DATABASE_URL
export SQLX_OFFLINE=true

TASKSET=()
if command -v taskset >/dev/null 2>&1; then
  TASKSET=(taskset -c 0,1)
else
  echo "warning: taskset not found — running unconstrained; reproduction is much less likely" >&2
fi

echo "── build"
cargo build --package awa --test rolling_upgrade_rehearsal_test

mkdir -p "$LOG_DIR"
rm -f "$LOG_DIR"/run-*.log

pass=0
fail=0
for i in $(seq 1 "$ITERATIONS"); do
  log="$LOG_DIR/run-${i}.log"
  echo "── iteration $i/$ITERATIONS"
  if "${TASKSET[@]}" cargo test --package awa --test rolling_upgrade_rehearsal_test \
      ${TEST_FILTER:+"$TEST_FILTER"} -- --ignored --nocapture >"$log" 2>&1; then
    pass=$((pass + 1))
    rm -f "$log"
  else
    fail=$((fail + 1))
    echo "   FAILED — see $log"
    grep -A2 "panicked at" "$log" | sed 's/^/   /' || true
  fi
done

echo
echo "── summary: ${pass} passed, ${fail} failed (of ${ITERATIONS})"
if [ "$fail" -gt 0 ]; then
  echo "   failing logs kept in ${LOG_DIR}/"
  exit 1
fi

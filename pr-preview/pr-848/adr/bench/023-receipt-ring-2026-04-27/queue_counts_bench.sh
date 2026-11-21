#!/usr/bin/env bash
# Targeted benchmark for queue_counts_exact under heavy backlog.
#
# Spins up a fresh `awa` schema in awa-wave1-test (port 15438), seeds N
# ready_entries for one queue, and runs EXPLAIN (ANALYZE, BUFFERS) on
# the exact-count CTE. Repeats for several backlog sizes so we can
# see how each sub-CTE scales.
#
# Output: one .explain file per (size, repeat) plus timings.csv.

set -euo pipefail

DSN="postgres://postgres:test@localhost:15438/awa_test"
PG="psql $DSN"
SCHEMA="awa"
QUEUE="bench_queue"
PRIORITY=2
SIZES=(100000 1000000 5000000 10000000)
REPEATS=3
OUT_DIR="$(cd "$(dirname "$0")" && pwd)"
TIMINGS_CSV="$OUT_DIR/timings.csv"

echo "size_rows,repeat,total_ms,planning_ms,execution_ms" > "$TIMINGS_CSV"

reset_schema() {
  echo "[setup] dropping awa schema"
  PGPASSWORD=test $PG -q -c "DROP SCHEMA IF EXISTS awa CASCADE" >/dev/null
}

apply_migrations() {
  echo "[setup] applying migrations + queue-storage prepare"
  local awa_bin="/home/brian/.cargo-target/release/awa"
  if [[ ! -x "$awa_bin" ]]; then
    echo "[setup] building awa-cli (release)"
    (cd "$(git rev-parse --show-toplevel)" && SQLX_OFFLINE=true cargo build --release --quiet --package awa-cli)
  fi
  "$awa_bin" --database-url "$DSN" migrate >/dev/null
  "$awa_bin" --database-url "$DSN" storage prepare-queue-storage-schema --schema "$SCHEMA" >/dev/null

  # Activate queue_storage backend so the runtime path matches reality.
  PGPASSWORD=test $PG -q <<EOF >/dev/null
INSERT INTO awa.runtime_storage_backends (backend, schema_name, updated_at)
VALUES ('queue_storage', '$SCHEMA', now())
ON CONFLICT (backend) DO UPDATE
SET schema_name = EXCLUDED.schema_name, updated_at = EXCLUDED.updated_at;

UPDATE awa.storage_transition_state
SET state = 'active',
    current_engine = 'queue_storage',
    prepared_engine = NULL,
    details = '{"schema": "$SCHEMA"}'::jsonb,
    updated_at = now(),
    finalized_at = now()
WHERE singleton;
EOF
}

seed_backlog() {
  local size="$1"
  echo "[seed] inserting $size ready_entries (single ready_slot=0, queue=$QUEUE)"
  PGPASSWORD=test $PG -q <<EOF >/dev/null
-- Single ready_slot=0 partition. queue_claim_heads + queue_lanes seeded
-- so the lane_counts CTE has a real boundary to filter against.
INSERT INTO $SCHEMA.queue_claim_heads (queue, priority, claim_seq)
VALUES ('$QUEUE', $PRIORITY, 1)
ON CONFLICT (queue, priority) DO UPDATE SET claim_seq = 1;

INSERT INTO $SCHEMA.queue_lanes (queue, priority, next_seq, claim_seq, available_count, pruned_completed_count)
VALUES ('$QUEUE', $PRIORITY, $size + 1, 1, $size, 0)
ON CONFLICT (queue, priority) DO UPDATE
SET next_seq = EXCLUDED.next_seq,
    claim_seq = EXCLUDED.claim_seq,
    available_count = EXCLUDED.available_count;

INSERT INTO $SCHEMA.ready_entries (
  ready_slot, ready_generation, job_id, kind, queue, priority,
  attempt, run_lease, max_attempts, lane_seq, run_at, created_at
)
SELECT 0, 0, gs, 'bench_kind', '$QUEUE', $PRIORITY, 0, 0, 25, gs, now(), now()
FROM generate_series(1, $size) AS gs;

ANALYZE $SCHEMA.ready_entries;
ANALYZE $SCHEMA.queue_claim_heads;
ANALYZE $SCHEMA.queue_lanes;
EOF
}

run_explain() {
  local size="$1"
  local repeat="$2"
  local outfile="$OUT_DIR/explain_${size}_${repeat}.txt"
  echo "[explain] size=$size repeat=$repeat -> $(basename "$outfile")"
  PGPASSWORD=test $PG -q -X --pset=footer=off <<EOF > "$outfile" 2>&1
\timing on
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, FORMAT TEXT)
WITH lane_counts AS (
    SELECT count(*)::bigint AS available
    FROM $SCHEMA.ready_entries AS ready
    JOIN $SCHEMA.queue_claim_heads AS claims
      ON claims.queue = ready.queue
     AND claims.priority = ready.priority
    WHERE ready.queue = ANY(ARRAY['$QUEUE'])
      AND ready.lane_seq >= claims.claim_seq
),
pruned_terminal AS (
    SELECT COALESCE(
        sum(GREATEST(
            COALESCE(lanes.pruned_completed_count, 0),
            COALESCE(rollups.pruned_completed_count, 0)
        )),
        0
    )::bigint AS completed
    FROM (
        SELECT queue, priority, pruned_completed_count
        FROM $SCHEMA.queue_lanes
        WHERE queue = ANY(ARRAY['$QUEUE'])
    ) AS lanes
    FULL OUTER JOIN (
        SELECT queue, priority, pruned_completed_count
        FROM $SCHEMA.queue_terminal_rollups
        WHERE queue = ANY(ARRAY['$QUEUE'])
    ) AS rollups
    USING (queue, priority)
),
live_running AS (
    SELECT (
        COALESCE((
            SELECT count(*)::bigint
            FROM $SCHEMA.leases
            WHERE queue = ANY(ARRAY['$QUEUE'])
              AND state = 'running'
        ), 0)
        +
        COALESCE((
            SELECT count(*)::bigint
            FROM $SCHEMA.lease_claims AS claims
            WHERE claims.queue = ANY(ARRAY['$QUEUE'])
              AND NOT EXISTS (
                  SELECT 1 FROM $SCHEMA.lease_claim_closures AS closures
                  WHERE closures.claim_slot = claims.claim_slot
                    AND closures.job_id = claims.job_id
                    AND closures.run_lease = claims.run_lease
              )
        ), 0)
    )::bigint AS running
),
live_terminal AS (
    SELECT count(*)::bigint AS completed
    FROM $SCHEMA.done_entries
    WHERE queue = ANY(ARRAY['$QUEUE'])
)
SELECT
    lane_counts.available,
    live_running.running,
    pruned_terminal.completed + live_terminal.completed AS completed
FROM lane_counts
CROSS JOIN pruned_terminal
CROSS JOIN live_running
CROSS JOIN live_terminal;
EOF

  # Extract Planning + Execution time from the explain output.
  local planning=$(grep -oE 'Planning Time: [0-9.]+ ms' "$outfile" | grep -oE '[0-9.]+' | head -1)
  local execution=$(grep -oE 'Execution Time: [0-9.]+ ms' "$outfile" | grep -oE '[0-9.]+' | head -1)
  local total=$(grep -oE 'Time: [0-9.]+ ms' "$outfile" | grep -oE '[0-9.]+' | tail -1)
  echo "$size,$repeat,$total,$planning,$execution" >> "$TIMINGS_CSV"
}

reset_schema
apply_migrations
for size in "${SIZES[@]}"; do
  reset_schema
  apply_migrations
  seed_backlog "$size"
  for r in $(seq 1 $REPEATS); do
    run_explain "$size" "$r"
  done
done

echo
echo "[done] timings:"
cat "$TIMINGS_CSV"

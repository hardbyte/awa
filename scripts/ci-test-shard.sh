#!/usr/bin/env bash
# CI sharding for the `Rust tests` job (#335).
#
# The suite is split into parallel CI shards so wall-clock cost is the
# slowest shard, not the sum. Timing basis (main run 28783383104):
# migration_test alone was 1339s of the ~31-minute job, so it is
# partitioned per-test with cargo-nextest (each test in its own process;
# the suite's Postgres advisory locks already serialize schema access
# across processes). The other named binaries form the `heavy` shard;
# EVERYTHING ELSE is the `rest` shard, computed by subtraction — a new
# test file lands in `rest` automatically and can never be forgotten.
#
# `check` mode (run by the lint job) fails when a named binary disappears,
# so renames can't silently drop coverage.
set -euo pipefail

MIGRATION_PARTITIONS=4

HEAVY_TESTS=(
  executor_guard_test
  lifecycle_hook_test
  queue_storage_runtime_test
  external_wait_test
  cel_callback_test
)

ASSIGNED_ELSEWHERE=(
  migration_test
  "${HEAVY_TESTS[@]}"
)

list_awa_test_files() {
  for f in awa/tests/*.rs; do
    basename "$f" .rs
  done
}

rest_test_args() {
  local name assigned
  for name in $(list_awa_test_files); do
    assigned=false
    for a in "${ASSIGNED_ELSEWHERE[@]}"; do
      if [ "$name" = "$a" ]; then
        assigned=true
        break
      fi
    done
    $assigned || printf -- '--test %s ' "$name"
  done
}

check() {
  local missing=0
  for a in "${ASSIGNED_ELSEWHERE[@]}"; do
    if [ ! -f "awa/tests/${a}.rs" ]; then
      echo "ci-test-shard.sh: shard references missing test file awa/tests/${a}.rs" >&2
      missing=1
    fi
  done
  if [ "$missing" -ne 0 ]; then
    echo "Update HEAVY_TESTS / ASSIGNED_ELSEWHERE in scripts/ci-test-shard.sh" >&2
    exit 1
  fi
  echo "shard membership OK: $(list_awa_test_files | wc -l) awa test files," \
    "${#ASSIGNED_ELSEWHERE[@]} explicitly assigned, rest-shard covers the remainder"
}

shard="${1:?usage: ci-test-shard.sh <check|migrations-K|heavy|rest>}"

case "$shard" in
  check)
    check
    ;;
  migrations-*)
    part="${shard#migrations-}"
    cargo nextest run -p awa --test migration_test \
      --partition "count:${part}/${MIGRATION_PARTITIONS}"
    ;;
  heavy)
    args=()
    for t in "${HEAVY_TESTS[@]}"; do args+=(--test "$t"); done
    cargo test -p awa "${args[@]}"
    ;;
  rest)
    # awa unit tests + every test binary not assigned elsewhere, then the
    # rest of the workspace (tests + doctests), then awa's own doctests.
    # Together with the other shards this mirrors `cargo test --workspace`.
    # shellcheck disable=SC2046
    cargo test -p awa --lib --bins $(rest_test_args)
    cargo test --workspace --exclude awa
    cargo test -p awa --doc
    ;;
  *)
    echo "unknown shard: $shard" >&2
    exit 2
    ;;
esac

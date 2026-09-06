#!/bin/sh
# Actual crates.io 0.6.7 cron paths against the candidate schema.
set -eu
: "${DATABASE_URL:?set DATABASE_URL to a disposable PostgreSQL database}"
repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo"
SQLX_OFFLINE=true cargo build --locked --manifest-path correctness/cron-compat/Cargo.toml
target=$(cargo metadata --no-deps --format-version 1 --manifest-path correctness/cron-compat/Cargo.toml | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
export AWA_CRON_N_MINUS_ONE_PROBE="$target/debug/awa-cron-compat-probe"
SQLX_OFFLINE=true cargo test -p awa --test cron_reconciliation_test -- --ignored --nocapture

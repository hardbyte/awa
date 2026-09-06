#!/bin/sh
set -eu
repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo"
./correctness/run-tlc.sh races/AwaCron.tla
./correctness/run-tlc.sh races/AwaCron.tla races/AwaCronLiveness.cfg
./correctness/run-tlc.sh races/AwaCronOwnership.tla
./correctness/run-tlc.sh races/AwaCronOwnership.tla races/AwaCronOwnershipLiveness.cfg
witness=$(mktemp)
if ./correctness/run-tlc.sh races/AwaCronOwnership.tla races/AwaCronOwnershipNoFence.cfg > "$witness" 2>&1; then
    cat "$witness"
    rm -f "$witness"
    exit 1
fi
cat "$witness"
if ! grep -q 'Action property RetiredCannotFire is violated' "$witness"; then
    rm -f "$witness"
    exit 1
fi
rm -f "$witness"

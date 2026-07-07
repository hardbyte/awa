#!/bin/sh
# Run the full TLC model suite with per-config expectations.
#
# The suite mixes three kinds of checks and each entry says which it is:
#   pass          TLC must complete with no error.
#   <pattern>     TLC output must match the (extended-regex) pattern — used
#                 for expected-counterexample regression witnesses and for
#                 trace configs, whose *Incomplete invariants are positive
#                 witnesses: a valid trace violates them after TLC consumes
#                 every event, and a broken trace deadlocks instead.
#
# Add a model by appending a line to the table below. Fields are separated
# by ';;' so grep alternation ('|') stays available inside patterns.
# Everything runs; failures are collected and reported together.
set -u

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

SUITE='
AwaSegmentedStorage.tla        ;; -                                       ;; pass ;; segmented storage, 1 worker
AwaSegmentedStorage.tla        ;; AwaSegmentedStorageInterleavings.cfg    ;; pass ;; segmented storage, 2-worker interleavings
AwaSegmentedStorageRaces.tla   ;; AwaSegmentedStorageRaces.cfg            ;; PrunedLeaseSegmentsAreEmpty is violated|PrunedClaimSegmentsAreEmpty is violated ;; claim/rotate/prune race witness
AwaSegmentedStorageRaces.tla   ;; AwaSegmentedStorageRacesSafe.cfg        ;; pass ;; safe commit
AwaSegmentedStorageRaces.tla   ;; AwaSegmentedStorageRacesMultiWorker.cfg ;; pass ;; safe commit, 3 workers
AwaSegmentedStorageTrace.tla   ;; AwaSegmentedStorageTrace.cfg            ;; SnoozeTraceIncomplete is violated ;; snooze trace consumed
AwaSegmentedStorageTrace.tla   ;; AwaSegmentedStorageTraceReceiptRescue.cfg ;; ReceiptRescueTraceIncomplete is violated ;; receipt-rescue trace consumed
AwaSegmentedStorageTrace.tla   ;; AwaSegmentedStorageTraceRunningCancel.cfg ;; RunningCancelTraceIncomplete is violated ;; running-cancel trace consumed
AwaSegmentedStorageTrace.tla   ;; AwaSegmentedStorageTraceReceiptOnlyCancel.cfg ;; ReceiptOnlyCancelTraceIncomplete is violated ;; receipt-only-cancel trace consumed
AwaSegmentedStorageTrace.tla   ;; AwaSegmentedStorageTraceCallbackWait.cfg ;; CallbackWaitTraceIncomplete is violated ;; callback wait/resume trace consumed
AwaSegmentedStorageTrace.tla   ;; AwaSegmentedStorageTraceDlqRetry.cfg    ;; DlqRetryTraceIncomplete is violated ;; DLQ retry trace consumed
AwaSegmentedStorageTrace.tla   ;; AwaSegmentedStorageTraceDlqPurge.cfg    ;; DlqPurgeTraceIncomplete is violated ;; DLQ purge trace consumed
AwaSegmentedStorageTrace.tla   ;; AwaSegmentedStorageTraceBroken.cfg      ;; Deadlock reached ;; broken trace rejected
AwaStorageLockOrder.tla        ;; AwaStorageLockOrder.cfg                 ;; pass ;; lock ordering
AwaStorageLockOrder.tla        ;; AwaStorageLockOrderDeadlockDemo.cfg     ;; NoDeadlock is violated ;; deadlock detector witness
AwaDeadTupleContract.tla       ;; AwaDeadTupleContract.cfg                ;; pass ;; architectural reclaim contract
AwaCanonicalUniqueRescue.tla   ;; -                                       ;; pass ;; per-row rescue fallback converges
AwaCanonicalUniqueRescue.tla   ;; AwaCanonicalUniqueRescueBatchOnly.cfg   ;; Temporal property Convergence was violated ;; batch-only rescue starvation witness
'

failures=0
total=0

run_entry() {
    spec=$1
    cfg=$2
    expect=$3
    desc=$4
    total=$((total + 1))

    label="$spec"
    [ "$cfg" != "-" ] && label="$spec / $cfg"
    log=$(mktemp)

    if [ "$cfg" = "-" ]; then
        "$SCRIPT_DIR/run-tlc.sh" "storage/$spec" >"$log" 2>&1
    else
        "$SCRIPT_DIR/run-tlc.sh" "storage/$spec" "storage/$cfg" >"$log" 2>&1
    fi
    rc=$?

    if [ "$expect" = "pass" ]; then
        if [ $rc -eq 0 ]; then
            echo "ok   $label — $desc"
        else
            echo "FAIL $label — $desc (expected clean pass, rc=$rc)"
            cat "$log"
            failures=$((failures + 1))
        fi
    elif grep -Eq "$expect" "$log"; then
        echo "ok   $label — $desc (expected: $expect)"
    else
        echo "FAIL $label — $desc (expected output matching: $expect)"
        cat "$log"
        failures=$((failures + 1))
    fi
    rm -f "$log"
}

# Parse the table. Using a temp file keeps run_entry in the current shell so
# the failure counter survives (a pipe would fork a subshell).
table=$(mktemp)
printf '%s\n' "$SUITE" | grep -v '^[[:space:]]*$' | grep -v '^[[:space:]]*#' >"$table"
while IFS= read -r line; do
    spec=$(printf '%s' "$line" | awk -F' *;; *' '{print $1}' | tr -d ' ')
    cfg=$(printf '%s' "$line" | awk -F' *;; *' '{print $2}' | tr -d ' ')
    expect=$(printf '%s' "$line" | awk -F' *;; *' '{print $3}')
    desc=$(printf '%s' "$line" | awk -F' *;; *' '{print $4}')
    run_entry "$spec" "$cfg" "$expect" "$desc"
done <"$table"
rm -f "$table"

echo
if [ "$failures" -ne 0 ]; then
    echo "TLC suite: $failures of $total checks failed"
    exit 1
fi
echo "TLC suite: all $total checks behaved as expected"

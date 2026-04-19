# ADR-019 Validation Artifact (2026-04-19)

This file records the exact commands and raw output used for the current
validation numbers in ADR-019.

All commands were run from the repository root on branch
`feature/vacuum-aware-storage-redesign` with:

```text
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test
```

## Final Runtime Comparison

### Canonical throughput

Command:

```text
env DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
  AWA_RUNTIME_TOTAL_JOBS=5000 \
  cargo test -p awa test_throughput_rust_workers --test benchmark_test \
  -- --ignored --exact --nocapture --test-threads=1
```

Output:

```text
[bench] Inserted 5000 jobs in 0.47s (10714 inserts/sec)
[bench] All 5000 jobs completed in 0.52s
[bench] Throughput: 9686 jobs/sec
ok
```

### Queue storage throughput

Command:

```text
env DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
  AWA_VA_RUNTIME_TOTAL_JOBS=5000 \
  cargo test -p awa test_throughput_rust_workers_queue_storage --test benchmark_test \
  -- --ignored --exact --nocapture --test-threads=1
```

Output:

```text
[bench-va] Inserted 5000 jobs in 0.26s (18921 inserts/sec)
[bench-va] All 5000 jobs completed in 0.52s
[bench-va] Throughput: 9537 jobs/sec
[bench-va] exact_dead_tuples queue_lanes=41 ready=0 done=0 leases=376 attempt_state=0 total=417
ok
```

### Canonical pickup latency

Command:

```text
env DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
  cargo test -p awa test_pickup_latency_listen_notify --test benchmark_test \
  -- --ignored --exact --nocapture --test-threads=1
```

Output:

```text
[bench] Pickup latency over 50 iterations:
[bench]   min:  3.792708ms
[bench]   p50:  4.995333ms
[bench]   p95:  38.998458ms
[bench]   p99:  217.351833ms
[bench]   max:  217.351833ms
ok
```

### Queue storage pickup latency

Command:

```text
env DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
  cargo test -p awa test_pickup_latency_listen_notify_queue_storage --test benchmark_test \
  -- --ignored --exact --nocapture --test-threads=1
```

Output:

```text
[bench-va] Pickup latency over 50 iterations:
[bench-va]   min:  2.87725ms
[bench-va]   p50:  3.671334ms
[bench-va]   p95:  22.013125ms
[bench-va]   p99:  87.7375ms
[bench-va]   max:  87.7375ms
ok
```

## Soak and Burst

### Mixed workload soak

Command:

```text
env DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
  AWA_QS_SOAK_TARGET_RATE=1000 \
  AWA_QS_SOAK_DURATION_SECS=10 \
  cargo test -p awa test_queue_storage_mixed_workload_soak --test queue_storage_soak_test \
  -- --ignored --exact --nocapture --test-threads=1
```

Summary output:

```text
[queue-storage-soak] summary duration=10s seeded=9988 produced_for=10.01s drained_in=12.79s handler=1016/s finalized=781/s peak_in_flight=334 peak_dead_total=10041 peak_attempt_state=0 exact_dead_total=276 exact_dead=(queue_lanes=19,ready=0,done=255,leases=2,attempt_state=0) final_counts={"completed": 8998, "failed": 990} seeded_by_mode={"terminal_fail": 495, "complete": 6000, "deadline_hang": 998, "snooze_once": 1000, "retry_once": 1000, "callback_timeout": 495} final_dlq=990
ok
```

### Terminal-failure burst

Command:

```text
env DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
  cargo test -p awa test_queue_storage_terminal_failure_burst --test queue_storage_soak_test \
  -- --ignored --exact --nocapture --test-threads=1
```

Summary output:

```text
[queue-storage-terminal-burst] summary total=10000 drain=5.10s handler=1962/s finalized=1962/s peak_in_flight=9936 peak_dead_total=2964 peak_attempt_state=0 exact_dead_total=61 exact_dead=(queue_lanes=61,ready=0,done=0,leases=0,attempt_state=0) final_counts={"failed": 10000} final_dlq=10000
ok
```

## Counter Lesson

One failed experiment is worth preserving because it changed the design:

When `queue_lanes` was updated on every hot-path completion to maintain live
`running` and `completed` counters, queue-storage throughput collapsed.

Command:

```text
env DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
  AWA_VA_RUNTIME_TOTAL_JOBS=5000 \
  cargo test -p awa test_throughput_rust_workers_queue_storage --test benchmark_test \
  -- --ignored --exact --nocapture --test-threads=1
```

Observed outputs during that experiment:

```text
[bench-va] All 5000 jobs completed in 8.87s
[bench-va] Throughput: 564 jobs/sec
```

and after a partial rollback:

```text
[bench-va] All 5000 jobs completed in 7.96s
[bench-va] Throughput: 628 jobs/sec
```

The recovered design keeps only claim/enqueue control in `lane_state`, derives
`running` from `active_leases`, and derives terminal counts from live
`terminal_entries` plus a prune-time rollup.

# Segmented Storage Model

`AwaSegmentedStorage` is a focused TLA+ model for the proposed 0.6+ segmented
runtime storage layout.

It uses the naming set discussed for the redesign:

- `ready_entries`
- `deferred_entries`
- `waiting_entries`
- `active_leases`
- `attempt_state`
- `terminal_entries`
- `lane_state`
- `ready_segments` / `ready_segment_cursor`
- `deferred_segments` / `deferred_segment_cursor`
- `waiting_segments` / `waiting_segment_cursor`
- `lease_segments` / `lease_segment_cursor`

What it models:

- append-only enqueue into `ready_entries`
- enqueue into `deferred_entries` plus promotion back to `ready_entries`
- parking into `waiting_entries` plus callback/timeout resume back to `ready_entries`
- claim into `active_leases`
- explicit `lane_state` append/claim cursors with gap-skipping claim advancement
- lazy materialization of `attempt_state`
- short-job completion without `attempt_state`
- stateful completion after heartbeat/progress/callback activity
- retry flow back into `deferred_entries`
- rescue flow that re-enqueues at the tail of `ready_entries`
- stale completion rejection via per-worker lease snapshots
- segment rotation and prune safety for ready, deferred, waiting, and lease segment families
- a second config with two workers to exercise interleavings on the same storage invariants
- a third config with two jobs to exercise lane ordering and uniqueness under enqueue/promote/resume churn

Key safety checks include:

- waiting jobs hold no active lease
- fresh heartbeat is attached to `active_leases`, not `attempt_state`, so short jobs remain rescuable
- deferred and waiting jobs have no runnable `laneSeq`
- `attempt_state` only exists for leased or waiting jobs
- claim cursors never move behind live runnable rows

What it intentionally does not model:

- SQL planner behavior
- MVCC horizons or autovacuum timing
- queue priorities and fairness
- liveness/fairness properties; this spec is still safety-oriented
- split-phase claim/rotate and prune/claim races; claim and prune are still modeled as atomic actions
- dedicated DLQ storage families; DLQ behavior is currently subsumed into `terminal_entries`
- terminal-family rotation/prune; `terminal_entries` is modeled as retained history only
- cancel-of-running in this storage model; see `AwaCore` for lease-guarded cancel/finalize races

Run it with:

```bash
./correctness/run-tlc.sh storage/AwaSegmentedStorage.tla
./correctness/run-tlc.sh storage/AwaSegmentedStorage.tla storage/AwaSegmentedStorageInterleavings.cfg
./correctness/run-tlc.sh storage/AwaSegmentedStorage.tla storage/AwaSegmentedStorageTwoJobs.cfg
```

The checked-in configs are intentionally small so TLC completes quickly in CI-like
environments:

- `AwaSegmentedStorage.cfg`: 1 job, 1 worker
- `AwaSegmentedStorageInterleavings.cfg`: 1 job, 2 workers
- `AwaSegmentedStorageTwoJobs.cfg`: 2 jobs, 1 worker

That is enough to exercise waiting/resume, stale completion rejection, retry,
rescue, and queue-family prune safety, but not multi-job fairness or priority-aging
liveness.

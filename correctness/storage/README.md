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

Key safety checks include:

- waiting jobs hold no active lease
- deferred and waiting jobs have no runnable `laneSeq`
- `attempt_state` only exists for leased or waiting jobs
- claim cursors never move behind live runnable rows

What it intentionally does not model:

- SQL planner behavior
- MVCC horizons or autovacuum timing
- queue priorities and fairness
- liveness/fairness properties; this spec is still safety-oriented

Run it with:

```bash
./correctness/run-tlc.sh storage/AwaSegmentedStorage.tla
./correctness/run-tlc.sh storage/AwaSegmentedStorage.tla storage/AwaSegmentedStorageInterleavings.cfg
```

The checked-in configs are intentionally small so TLC completes quickly in CI-like
environments:

- `AwaSegmentedStorage.cfg`: 1 job, 1 worker
- `AwaSegmentedStorageInterleavings.cfg`: 1 job, 2 workers

That is enough to exercise waiting/resume, stale completion rejection, retry,
rescue, and queue-family prune safety, but not multi-job fairness or priority-aging
liveness.

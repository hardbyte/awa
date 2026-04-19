# Segmented Storage Model

`AwaSegmentedStorage` is a focused TLA+ model for the proposed 0.6+ segmented
runtime storage layout.

It uses the naming set discussed for the redesign:

- `ready_entries`
- `deferred_entries`
- `active_leases`
- `attempt_state`
- `terminal_entries`
- `lane_state`
- `ready_segments` / `ready_segment_cursor`
- `deferred_segments` / `deferred_segment_cursor`
- `lease_segments` / `lease_segment_cursor`

What it models:

- append-only enqueue into `ready_entries`
- enqueue into `deferred_entries` plus promotion back to `ready_entries`
- claim into `active_leases`
- explicit `lane_state` append/claim cursors with gap-skipping claim advancement
- lazy materialization of `attempt_state`
- short-job completion without `attempt_state`
- stateful completion after heartbeat/progress/callback activity
- retry flow back into `deferred_entries`
- rescue flow that re-enqueues at the tail of `ready_entries`
- stale completion rejection via per-worker lease snapshots
- segment rotation and prune safety for ready, deferred, and lease segment families

What it intentionally does not model:

- SQL planner behavior
- MVCC horizons or autovacuum timing
- queue priorities and fairness
- a dedicated `waiting_entries` family; callback waiting is still abstracted as attempt-state metadata

Run it with:

```bash
./correctness/run-tlc.sh storage/AwaSegmentedStorage.tla
```

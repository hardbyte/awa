# Upgrade Checklist: 0.5.x → 0.6

This is the operator-facing rollout sheet for moving an existing 0.5.x
cluster to 0.6 (queue-storage-by-default). The companion long-form
explanation is in [migrations.md](migrations.md). This file is the
short version: one-screen pre-flight, two phases, an explicit rollback
boundary, and the health checks to run at each step.

> **Fresh installs do not need this file.** A new cluster runs
> `awa migrate` and starts workers; the first worker auto-finalizes
> via `awa.storage_auto_finalize_if_fresh()`. See
> migrations.md ["Fresh install"](migrations.md#fresh-install-no-prior-canonical-data).
> This checklist is for **upgrading existing 0.5.x clusters**, where
> canonical drain is unavoidable and auto-finalize correctly defers to
> the staged path.

## Pre-flight

- [ ] Cluster is on `0.5.latest` everywhere (no mixed older versions)
- [ ] `awa storage status` shows `state=canonical / current=canonical / active=canonical / prepared=NULL`
- [ ] Schema is at the latest 0.5.x migration: `SELECT MAX(version) FROM awa.schema_version`
- [ ] Backups taken (or backup tooling verified) — there's no `awa migrate` downgrade path
- [ ] Operator has access to:
  - Run `awa storage` commands against the production DSN
  - Inspect `awa.runtime_instances` for live capability reporting
  - Watch worker logs / Grafana dashboards during the cutover
- [ ] Confirmed every queue-storage-capable Python worker is on an `awa-pg` wheel that exposes the ADR-023 claim-ring knobs (the `queue_storage_claim_*` kwargs on `client.start()`); skip if running Rust-only

## Phase 1 — last 0.5.x everywhere (safe stop)

```bash
# 1. Apply the prep migration
awa --database-url "$DATABASE_URL" migrate

# 2. Verify cluster is still fully canonical
awa --database-url "$DATABASE_URL" storage status

# 3. Confirm runtime capability is canonical-only
psql "$DATABASE_URL" -c "
  SELECT instance_id, storage_capability, last_seen_at
  FROM awa.runtime_instances
  ORDER BY last_seen_at DESC;
"
```

**Expected:** `current_engine=canonical`, `active_engine=canonical`,
`prepared_engine=NULL`, `state=canonical`. Every live `runtime_instance`
reports `storage_capability=canonical` (not `queue_storage`).

This is a **safe stopping point**. The queue is fully canonical and
behavior is unchanged. You can sit here indefinitely.

## Phase 2 — 0.6 rollout

> **Crossing the line below is a one-way door once any queue-storage
> work has been accepted.** See [Rollback boundaries](#rollback-boundaries)
> first.

```bash
# 1. Roll out 0.6 binaries (rolling deploy). 0.5.x and 0.6 pods may
#    coexist; while state is still canonical or prepared, all writes
#    and execution stay canonical.

# 2. Materialize the queue-storage schema before the routing flip.
#    Skipping this means the first queue-storage write pays the
#    schema-install cost. The default is the `awa` schema (shared
#    with canonical metadata); if you want an isolated schema name
#    for queue-storage tables, pass it here AND in step 3's --details.
awa --database-url "$DATABASE_URL" storage prepare-queue-storage-schema --schema awa

# 3. Record the prepared engine. Default schema is `awa`; pass
#    --details '{"schema":"<name>"}' to record a different name.
awa --database-url "$DATABASE_URL" storage prepare --engine queue_storage

# 4. Verify prepared state.
awa --database-url "$DATABASE_URL" storage status
#    → current=canonical, active=canonical, prepared=queue_storage, state=prepared

# 5. Confirm every live runtime is queue-storage capable BEFORE the
#    flip. This is the operator-side pre-flight that prevents
#    canonical-only (0.5) workers from surviving into mixed_transition.
psql "$DATABASE_URL" -c "
  SELECT count(*) FILTER (WHERE storage_capability != 'queue_storage') AS canonical_only
  FROM awa.runtime_instances
  WHERE last_seen_at > now() - interval '90 seconds';
"
#    → canonical_only must be 0

# 6. Start at least one runtime with `transition_role=queue_storage_target`.
#    This is what the mixed-transition SQL gate actually requires. An auto-role
#    runtime started before mixed_transition resolves its effective
#    storage to canonical at startup and will downgrade to
#    `canonical_drain_only` immediately after routing flips, leaving
#    the cluster with no queue-storage executor. A queue-storage target
#    is the witness that someone will keep executing queue-storage work
#    once routing flips.
#
#    In Rust:
#      Client::builder(pool)
#          .queue_storage(...)
#          .transition_role(TransitionWorkerRole::QueueStorageTarget)
#
#    In Python:
#      client.start([(queue, n)],
#                   queue_storage_schema=schema,
#                   storage_transition_role="queue_storage_target")
#
#    Verify it has registered:
psql "$DATABASE_URL" -c "
  SELECT count(*) AS live_targets
  FROM awa.runtime_instances
  WHERE transition_role = 'queue_storage_target'
    AND storage_capability = 'queue_storage'
    AND last_seen_at > now() - interval '90 seconds';
"
#    → live_targets must be ≥ 1

# 7. Flip routing. New writes and cron enqueues go to queue storage.
awa --database-url "$DATABASE_URL" storage enter-mixed-transition

# 8. Watch canonical drain. `canonical_live_backlog()` sums non-terminal
#    `jobs_hot` rows plus everything still in `scheduled_jobs` — the
#    same number the gate uses internally.
watch -n 5 'psql "$DATABASE_URL" -c "SELECT awa.canonical_live_backlog() AS canonical_backlog;"'
#    → wait for canonical_backlog to reach 0

# 9. Finalize once canonical backlog is empty.
awa --database-url "$DATABASE_URL" storage finalize

# 10. Verify active state.
awa --database-url "$DATABASE_URL" storage status
#    → current=queue_storage, active=queue_storage, prepared=NULL, state=active

# 11. Once state=active, the queue-storage-target runtime is no longer
#     special — auto-role runtimes started from now on resolve to queue
#     storage at startup. Either keep the explicit target running or
#     redeploy it without --transition-role; behavior is identical
#     post-flip.
```

## Health checks per step

| After step | Watch for |
|------------|-----------|
| migrate | `SELECT MAX(version) FROM awa.schema_version` advances |
| prepare-queue-storage-schema | `\dt awa.ready_entries` (and other queue-storage tables) exist in the `awa` schema |
| prepare | `awa storage status` reports `state=prepared` |
| start queue-storage target | `awa.runtime_instances` shows `transition_role='queue_storage_target'` and `storage_capability='queue_storage'` for the new instance; `awa storage status` lists no `enter_mixed_transition_blockers` |
| enter-mixed-transition | `awa.maintenance.rotate.attempts{awa_ring="queue", awa_ring_outcome="rotated"}` is non-zero in Grafana; queue ring `current_slot` advancing |
| watch canonical drain | `awa.queue_depth{awa_job_state="available"}` on the canonical side trending to 0 |
| finalize | `awa storage status` reports `state=active`; no canonical-state runtime instances heartbeating |

## Rollback boundaries

- **canonical → prepared**: `awa storage abort` returns to canonical. Trivial.
- **prepared → canonical**: `awa storage abort` clears the prepared engine metadata. Trivial.
- **mixed_transition (no queue-storage work yet)**: `awa storage abort` rolls back. The interlock requires no live queue-storage runtimes AND no rows in queue-storage tables. Acceptable while the routing flip just happened and producers haven't enqueued yet.
- **mixed_transition (queue-storage rows exist) — ONE-WAY**: `awa storage abort` is rejected. From this point onward you must either finish the transition (`finalize`) or restore from backup. A pure fleet downgrade to 0.5 is **not supported** because 0.5 workers don't know how to claim queue-storage work.
- **active → anything**: not supported by `awa storage abort`. Use database restore.

## Watch list during the rollout

These are the metrics that distinguish "transition healthy" from "transition stuck", available on the OTel Prometheus dashboard (panel row "Ring rotation & prune (queue-storage)"):

- `awa.maintenance.rotate.attempts{awa_ring="queue", awa_ring_outcome="rotated"}` should advance steadily — this is the headline "ring is rotating" signal
- `awa.maintenance.rotate.attempts{awa_ring_outcome="skipped_busy"}` rate should stay flat — sustained `skipped_busy` traffic with `awa_ring_blocker="queue.ready_rows"` means producers are outpacing consumers
- `awa.queue_lag_seconds` p95 should not climb past your latency SLO
- `awa.maintenance.prune.attempts{awa_ring_outcome="blocked"}` rate should be near zero — non-zero `blocked` means a held-tx is preventing partition reclaim

If any of these go wrong **before** any queue-storage work is accepted, `awa storage abort` is still available. After, you commit to forward-only.

## Known issues

- **Python workers without claim-ring knobs.** Older Python wheels don't accept `queue_storage_claim_slot_count` / `queue_storage_claim_rotate_interval_ms` on `client.start()`. They'll still run with default values and the rollout will work, but operators wanting non-default ring sizing need a wheel that exposes those kwargs.
- **Held-tx during finalize.** A long-running canonical transaction (e.g., reporting query) blocks vacuum, which can stall partition prune in the queue-storage tables. `awa.maintenance.prune.attempts{awa_ring_outcome="blocked"}` will rise. Identify and terminate the held-tx; prune resumes on the next maintenance tick.
- **0.6 rollback after queue-storage work.** Not supported via `awa storage abort` once any rows exist in queue-storage tables. Plan accordingly: keep `0.6` workers available throughout the transition window. Only emergency rollback path is database restore.

## v017 sharded enqueue heads — operator notes

Migration `v017` adds an `enqueue_shard` column and extends the primary keys of `queue_enqueue_heads`, `queue_claim_heads`, `ready_entries`, `leases`, and `done_entries`. `lease_claims` carries the column as a regular column; its primary key stays `(claim_slot, job_id, run_lease)`. `awa.queue_meta.enqueue_shards` is the per-queue tunable (default 1, range 1..=64). The default value is observationally identical to the pre-v017 layout — shard 0 is the only shard, and FIFO-within-lane is preserved.

When the migration runs:

- **`canonical` state.** No queue-storage schema exists; the migration is a no-op for the queue-storage tables. `awa.queue_meta.enqueue_shards` is added with default 1 and the `BETWEEN 1 AND 64` constraint.
- **`prepared` state.** Queue-storage schema exists but receives no live traffic. The PK reshape takes an `ACCESS EXCLUSIVE` lock; the tables are empty so the lock acquires immediately and the migration completes in milliseconds.
- **`active` state.** Queue-storage is the live engine. The PK reshape still takes an `ACCESS EXCLUSIVE` lock, but on a live table it waits for in-flight enqueue and claim transactions to finish, then blocks new ones until the rewrite completes. On a quiet queue this is milliseconds; on a saturated queue with multi-million-row `ready_entries` it is longer. Run the migration during a low-traffic window.
- **`mixed_transition` state.** The migration refuses to run and raises a 55000 error. Finalize the transition (or abort and re-enter `prepared`) first.

### Raising `enqueue_shards`

Opt a contended queue into multiple shards with a single UPDATE on `awa.queue_meta`:

```sql
UPDATE awa.queue_meta SET enqueue_shards = 4 WHERE queue = 'my_hot_queue';
```

The producer-side rotor picks up the new shard count on the next enqueue. Existing in-flight rows on shard 0 continue to be claimed and drained; new rows fan out across shards 0..S-1.

Trade-offs:

- **FIFO ordering** is preserved within a shard. Cross-shard ordering at the lane level becomes approximate. Workloads that rely on strict per-lane FIFO pin `enqueue_shards = 1`.
- **Throughput** scales near-linearly with shard count up to roughly one shard per two concurrent producers on a contended queue; the next bottleneck is WAL bandwidth.
- **Claim cost** is `O(shard count)` per claim call: the claim path scans every shard's head row to pick a candidate. With `enqueue_shards = 64` and four priorities, that's 256 candidate rows — still trivial.

See [ADR-025](adr/025-sharded-enqueue-heads.md) for the full design and per-shard FIFO contract.

### Lowering `enqueue_shards`

Lowering is safe only after all `ready_entries` rows in the now-out-of-range shards have been claimed. The producer rotor stops emitting to those shards immediately, but existing rows still need a claimer. Confirm with:

```sql
SELECT enqueue_shard, count(*)
FROM <queue_storage_schema>.ready_entries
WHERE queue = 'my_hot_queue'
GROUP BY enqueue_shard;
```

before lowering the value.

## Cross-references

- [migrations.md](migrations.md) — full migration story including SQL-level identities
- [configuration.md](configuration.md) — claim-ring / lease-ring sizing knobs
- [`docs/adr/023-receipt-plane-ring-partitioning.md`](adr/023-receipt-plane-ring-partitioning.md) — receipt-plane partition design and reverse-migration recipe
- [`docs/adr/025-sharded-enqueue-heads.md`](adr/025-sharded-enqueue-heads.md) — enqueue-head sharding design and FIFO contract
- [`docs/grafana/awa-dashboard.json`](grafana/awa-dashboard.json) — Prometheus dashboard with the rotation/prune panels

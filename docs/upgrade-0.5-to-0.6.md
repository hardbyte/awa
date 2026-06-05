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

## Rollback boundaries

Read this before starting Phase 2. The rollout has one explicit one-way
door — knowing exactly where it sits is the difference between "abort"
and "restore from backup":

- **canonical → prepared**: `awa storage abort` returns to canonical. Trivial.
- **prepared → canonical**: `awa storage abort` clears the prepared engine metadata. Trivial.
- **mixed_transition (no queue-storage work yet)**: `awa storage abort` rolls back. The interlock requires no live queue-storage runtimes AND no rows in queue-storage tables. Acceptable while the routing flip just happened and producers haven't enqueued yet.
- **mixed_transition (queue-storage rows exist) — ONE-WAY**: `awa storage abort` is rejected. From this point onward you must either finish the transition (`finalize`) or restore from backup. A pure fleet downgrade to 0.5 is **not supported** because 0.5 workers don't know how to claim queue-storage work.
- **active → anything**: not supported by `awa storage abort`. Use database restore.

The `/api/storage` dashboard card surfaces the current boundary
explicitly: "Rollback is still available" on `canonical` / `prepared`,
"From here it's restore-only" once the cluster has crossed into
`mixed_transition`, and "Finalized" once the transition has completed.

### The Storage transition card

When a transition is in flight the runtime page renders a "Storage
transition" card with four panels operators reload during a cutover:

1. **Header row** — current engine, prepared engine, state, and the
   live canonical backlog. The state pill is paired with an
   `in <state> for Nh Mm` time-in-state stamp that ticks once per
   second, so a stuck transition is visually distinct from a slow one
   without having to compare timestamps by hand.
2. **Backlog sparkline** — appears next to the canonical-backlog number
   once the cluster enters `mixed_transition`. The series is anchored
   to the current `transition_epoch`, so it always shows the timeline
   of *this* cutover (an `abort`/re-`prepare` resets the line).
3. **Schema-readiness warning** — if `prepared_engine = queue_storage`
   but the queue-storage tables are missing, a yellow alert spells out
   the exact `awa storage prepare-queue-storage-schema --schema <name>`
   command to run before the routing flip.
4. **Rollback-boundary panel** — the wording above ("reversible via
   abort", "from here it's restore-only", "finalized") rendered as a
   coloured block so the operator never has to infer the boundary from
   the state string alone.

Samples for the sparkline are accumulated client-side from the existing
poll (no audit table, per the #180 decision); they're discarded on a
tab reload and that's fine — the operator only needs the trend since
they opened the page, not historical replay.

## Phase 2 — 0.6 rollout

> **Crossing the line below is a one-way door once any queue-storage
> work has been accepted.** Re-read [Rollback boundaries](#rollback-boundaries)
> above before kicking off Phase 2.

```bash
# 1. Roll out 0.6 binaries (rolling deploy). 0.5.x and 0.6 pods may
#    coexist; while state is still canonical or prepared, all writes
#    and execution stay canonical.

# 2. Optional: materialize a custom queue-storage schema before the
#    routing flip. The default `awa` schema is already materialized by
#    `awa migrate`; run this only if you want an isolated schema name or
#    non-default slot counts, and pass the same schema in step 3's --details.
# awa --database-url "$DATABASE_URL" storage prepare-queue-storage-schema --schema <custom_schema>

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

# 8. Wait for canonical drain and finalize. `awa storage finalize --wait`
#    polls every 5s and only invokes the SQL finalize once every
#    readiness gate (`canonical_live_backlog`, no canonical or
#    drain-only runtimes still heartbeating, etc.) has stayed clear
#    for two consecutive observations. Default wait is unbounded;
#    pass e.g. `--wait=2h` to cap. Polling progress is emitted via
#    structured `tracing` logs (set `RUST_LOG=info` to see it).
awa --database-url "$DATABASE_URL" storage finalize --wait
#    → exits 0 once state=active; exits 2 if the wait cap expires
#       while blockers remain.

# 8a. (Optional) Dry-run the readiness gates first without changing
#     state. `--check` prints the same JSON `awa storage status`
#     would, plus a one-line summary, and exits 2 if blocked.
awa --database-url "$DATABASE_URL" storage finalize --check

# 9. Verify active state.
awa --database-url "$DATABASE_URL" storage status
#    → current=queue_storage, active=queue_storage, prepared=NULL, state=active

# 10. Once state=active, the queue-storage-target runtime is no longer
#     special — auto-role runtimes started from now on resolve to queue
#     storage at startup. Either keep the explicit target running or
#     redeploy it without --transition-role; behavior is identical
#     post-flip.
```

## Health checks per step

| After step | Watch for |
|------------|-----------|
| migrate | `SELECT MAX(version) FROM awa.schema_version` advances; `\dt awa.ready_entries` exists for the default schema |
| prepare custom queue-storage schema | `\dt <custom_schema>.ready_entries` (and other queue-storage tables) exist when using a custom schema |
| prepare | `awa storage status` reports `state=prepared` |
| start queue-storage target | `awa.runtime_instances` shows `transition_role='queue_storage_target'` and `storage_capability='queue_storage'` for the new instance; `awa storage status` lists no `enter_mixed_transition_blockers` |
| enter-mixed-transition | `awa_maintenance_rotate_attempts_total{awa_ring="queue", awa_ring_outcome="rotated"}` is non-zero in Grafana; queue ring `current_slot` advancing |
| watch canonical drain | `awa_queue_depth{awa_job_state="available"}` on the canonical side trending to 0 |
| finalize | `awa storage status` reports `state=active`; no canonical-state runtime instances heartbeating |

## Watch list during the rollout

These are the metrics that distinguish "transition healthy" from "transition stuck", available on the OTel Prometheus dashboard (panel row "Ring rotation & prune (queue-storage)"):

- `awa_maintenance_rotate_attempts_total{awa_ring="queue", awa_ring_outcome="rotated"}` should advance steadily — this is the headline "ring is rotating" signal
- `awa_maintenance_rotate_attempts_total{awa_ring_outcome="skipped_busy"}` rate should stay flat — sustained `skipped_busy` traffic with `awa_ring_blocker="queue.ready_rows"` means producers are outpacing consumers
- `awa_queue_lag_seconds` p95 should not climb past your latency SLO
- `awa_maintenance_prune_attempts_total{awa_ring_outcome="blocked"}` rate should be near zero — non-zero `blocked` means a held-tx is preventing partition reclaim

If any of these go wrong **before** any queue-storage work is accepted, `awa storage abort` is still available. After, you commit to forward-only.

## Known issues

- **Python workers without claim-ring knobs.** Older Python wheels don't accept `queue_storage_claim_slot_count` / `queue_storage_claim_rotate_interval_ms` on `client.start()`. They'll still run with default values and the rollout will work, but operators wanting non-default ring sizing need a wheel that exposes those kwargs.
- **Held-tx during finalize.** A long-running canonical transaction (e.g., reporting query) blocks vacuum, which can stall partition prune in the queue-storage tables. `awa_maintenance_prune_attempts_total{awa_ring_outcome="blocked"}` will rise. Identify and terminate the held-tx; prune resumes on the next maintenance tick.
- **0.6 rollback after queue-storage work.** Not supported via `awa storage abort` once any rows exist in queue-storage tables. Plan accordingly: keep `0.6` workers available throughout the transition window. Only emergency rollback path is database restore.

## v017 sharded enqueue heads — operator notes

Migration `v017` adds an `enqueue_shard` column and extends the primary keys of `queue_enqueue_heads`, `queue_claim_heads`, `ready_entries`, `leases`, and `done_entries`. `lease_claims` carries the column as a regular column; its primary key stays `(claim_slot, job_id, run_lease)`. `awa.queue_meta.enqueue_shards` is the per-queue tunable (default 1, range 1..=64). The default value is observationally identical to the pre-v017 layout — shard 0 is the only shard, and strict FIFO per `(queue, priority)` is preserved.

`enqueue_shards > 1` is a semantic mode switch, comparable to choosing SQS Standard over SQS FIFO, raising Kafka partition count, or using Pub/Sub ordering keys instead of global ordering. The queue's ordering contract changes from strict FIFO per lane to **partitioned FIFO**: strict FIFO within `(queue, priority, enqueue_shard)`, no ordering promised across shards. Operators opt in per queue; the default keeps the legacy contract.

When the migration runs:

- **`canonical` state.** No queue-storage schema exists; the migration is a no-op for the queue-storage tables. `awa.queue_meta.enqueue_shards` is added with default 1 and the `BETWEEN 1 AND 64` constraint.
- **`prepared` state.** Queue-storage schema exists but receives no live traffic. The PK reshape takes an `ACCESS EXCLUSIVE` lock; the tables are empty so the lock acquires immediately and the migration completes in milliseconds.
- **`active` state.** Queue-storage is the live engine. The PK reshape still takes an `ACCESS EXCLUSIVE` lock, but on a live table it waits for in-flight enqueue and claim transactions to finish, then blocks new ones until the rewrite completes. On a quiet queue this is milliseconds; on a saturated queue with multi-million-row `ready_entries` it is longer. Run the migration during a low-traffic window.
- **`mixed_transition` state.** The migration refuses to run and raises a 55000 error. Finalize the transition (or abort and re-enter `prepared`) first.

### Raising `enqueue_shards`

Opt a contended queue into multiple shards by upserting `awa.queue_meta`:

```sql
INSERT INTO awa.queue_meta (queue, enqueue_shards)
VALUES ('my_hot_queue', 4)
ON CONFLICT (queue)
DO UPDATE SET enqueue_shards = EXCLUDED.enqueue_shards;
```

The producer-side rotor picks up the new shard count on the next enqueue. Existing in-flight rows on shard 0 continue to be claimed and drained; new no-key batches spread across shards 0..S-1 over time.

Ordering contract and trade-offs:

- **Partitioned FIFO.** Strict FIFO is preserved within each `(queue, priority, enqueue_shard)`. Cross-shard ordering is not guaranteed. Workloads that need strict cross-producer FIFO at the lane level keep `enqueue_shards = 1`. Workloads that need per-key FIFO (per customer, per order, per account) pass `InsertOpts::ordering_key` when enqueuing — rows sharing a key hash to the same shard and inherit that shard's strict FIFO contract.
- **Throughput** scales near-linearly with shard count up to roughly one shard per two concurrent producers on a contended queue; the next bottleneck is WAL bandwidth.
- **Claim cost** is `O(shard count)` per claim call: the claim path scans every shard's head row to pick a candidate. With `enqueue_shards = 64` and four priorities, that's 256 candidate rows — still trivial.

See [ADR-025](adr/025-sharded-enqueue-heads.md) for the full design and the partitioned-FIFO contract.

### Routing related jobs to the same shard

For independent jobs, the default per-store rotor uses one shard pick per `(queue, priority)` sub-batch, then spreads successive batches across shards. For jobs that must observe each other in order — successive events for the same customer, sequential steps in a workflow, all writes for one account — set `InsertOpts::ordering_key` to the bytes of the partition identifier:

```rust
let opts = InsertOpts {
    queue: "events".into(),
    ordering_key: Some(customer_id.as_bytes().to_vec()),
    ..Default::default()
};
```

The key bytes are mapped by Awa's portable shard hash and reduced modulo the queue's `enqueue_shards`, so two enqueues with the same key always land on the same shard regardless of which producer or batch boundary they sit on. Rust, SQL, and Python enqueue paths use the same byte-level routing. At `enqueue_shards = 1` the key is ignored (every job lands on shard 0 anyway).

### Lowering `enqueue_shards`

Lowering is safe at any time: every claim, rescue, and admin path joins `queue_claim_heads` to `queue_enqueue_heads` without filtering on the current shard count, so rows on now-out-of-range shards continue to drain through the same code paths. There is no risk of orphaning in-flight rows on shards `>= newS`.

The only operator-visible caveat is the per-runtime `enqueue_shards_cache`: a runtime that cached the old value keeps producing to the old shard count until that cache is invalidated. The cache is correctness-safe (producers and claimers agree on the value at row-write time) but operator-intent-stale. Procedure:

1. Lower the value:

   ```sql
   INSERT INTO awa.queue_meta (queue, enqueue_shards)
   VALUES ('my_hot_queue', 2)
   ON CONFLICT (queue)
   DO UPDATE SET enqueue_shards = EXCLUDED.enqueue_shards;
   ```

2. Restart worker processes (or rely on the next deploy) so the in-process cache observes the new value.
3. Optionally watch per-shard lag drain to zero:

   ```sql
   SELECT priority, enqueue_shard,
          <queue_storage_schema>.sequence_next_value(enqueues.seq_name) AS enqueue_cursor,
          <queue_storage_schema>.sequence_next_value(claims.seq_name) AS claim_cursor,
          <queue_storage_schema>.sequence_next_value(enqueues.seq_name)
            - <queue_storage_schema>.sequence_next_value(claims.seq_name) AS lag
   FROM <queue_storage_schema>.queue_claim_heads AS claims
   JOIN <queue_storage_schema>.queue_enqueue_heads AS enqueues
     USING (queue, priority, enqueue_shard)
   WHERE queue = 'my_hot_queue'
   ORDER BY priority, enqueue_shard;
   ```

   The `queue_*_heads` rows for shards `>= newS` linger as empty heads after the drain. They are harmless — a handful of tiny rows with no effect on throughput.

The audit and drain contract are pinned by `test_queue_storage_lowering_enqueue_shards_drains_existing_rows` in `awa/tests/queue_storage_runtime_test.rs`.

## Cross-references

- [migrations.md](migrations.md) — full migration story including SQL-level identities
- [configuration.md](configuration.md) — claim-ring / lease-ring sizing knobs
- [`docs/adr/023-receipt-plane-ring-partitioning.md`](adr/023-receipt-plane-ring-partitioning.md) — receipt-plane partition design and reverse-migration recipe
- [`docs/adr/025-sharded-enqueue-heads.md`](adr/025-sharded-enqueue-heads.md) — enqueue-head sharding design and partitioned-FIFO contract
- [`docs/grafana/awa-dashboard.json`](grafana/awa-dashboard.json) — Prometheus dashboard with the rotation/prune panels

# Upgrade Checklist: 0.5.x → 0.6

> **Planning to run 0.7?** Finalize before upgrading: the 0.7 `awa migrate` refuses unfinalized clusters ([ADR-037](adr/037-canonical-engine-deprecation.md), [upgrade-0.6-to-0.7.md](upgrade-0.6-to-0.7.md)).

This is the operator-facing source of truth for moving an existing 0.5.x cluster to 0.6 (queue-storage-by-default). It defines the pre-flight, rollout phases, rollback boundary, and health checks; [migrations.md](migrations.md) covers the general migration contract and external tooling.

> **Fresh installs do not need this file.** A new cluster runs `awa migrate` and starts workers; the first worker auto-finalizes via `awa.storage_auto_finalize_if_fresh()`. See migrations.md ["Fresh install"](migrations.md#fresh-install-no-prior-canonical-data). This checklist is for **upgrading existing 0.5.x clusters**, where canonical drain is unavoidable and auto-finalize correctly defers to the staged path.

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

**Expected:** `current_engine=canonical`, `active_engine=canonical`, `prepared_engine=NULL`, `state=canonical`. Every live `runtime_instance` reports `storage_capability=canonical` (not `queue_storage`).

This is a **safe stopping point**. The queue is fully canonical and behavior is unchanged. You can sit here indefinitely.

## Rollback boundaries

Read this before starting Phase 2. The rollout has one explicit one-way door — knowing exactly where it sits is the difference between "abort" and "restore from backup":

- **canonical → prepared**: `awa storage abort` returns to canonical. Trivial.
- **prepared → canonical**: `awa storage abort` clears the prepared engine metadata. Trivial.
- **mixed_transition (no queue-storage work yet)**: `awa storage abort` rolls back. The interlock requires no live queue-storage runtimes AND no rows in queue-storage tables. Acceptable while the routing flip just happened and producers haven't enqueued yet.
- **mixed_transition (queue-storage rows exist) — ONE-WAY**: `awa storage abort` is rejected. From this point onward you must either finish the transition (`finalize`) or restore from backup. A pure fleet downgrade to 0.5 is **not supported** because 0.5 workers don't know how to claim queue-storage work.
- **active → anything**: not supported by `awa storage abort`. Use database restore.

The `/api/storage` dashboard card shows the current transition state, backlog, schema readiness, and rollback boundary.

## Phase 2 — 0.6 rollout

> **Crossing the line below is a one-way door once any queue-storage work has been accepted.** Re-read [Rollback boundaries](#rollback-boundaries) above before kicking off Phase 2.

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

# 8. Finalize. `--wait` polls every 5s and invokes the SQL finalize after
#    canonical_live_backlog is empty for two consecutive observations.
#    Pre-flip auto runtimes remain canonical_drain_only and idle; v040 no
#    longer requires them to restart or age out before finalization. Roll
#    them normally afterward; replacements resolve directly to queue storage.
#    Default wait is unbounded; pass e.g. `--wait=2h` to cap. Progress is
#    emitted via structured `tracing` logs (set `RUST_LOG=info` to see it).
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
| --- | --- |
| migrate | `SELECT MAX(version) FROM awa.schema_version` advances; `awa storage status` reports no schema-readiness blocker |
| prepare custom queue-storage schema | `awa storage status` reports no schema-readiness blocker for the configured schema |
| prepare | `awa storage status` reports `state=prepared` |
| start queue-storage target | `awa.runtime_instances` shows `transition_role='queue_storage_target'` and `storage_capability='queue_storage'` for the new instance; `awa storage status` lists no `enter_mixed_transition_blockers` |
| enter-mixed-transition | `awa_maintenance_rotate_attempts_total{awa_ring="queue", awa_ring_outcome="rotated"}` is non-zero in Grafana; queue ring `current_slot` advancing |
| watch canonical drain | `awa_queue_depth{awa_job_state="available"}` on the canonical side trending to 0 |
| finalize | `awa storage status` reports `state=active`; live `canonical_drain_only` runtimes are idle and may be rolled normally |

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

## Cross-references

- [migrations.md](migrations.md) — general migration contract and external tooling
- [configuration.md](configuration.md) — claim-ring / lease-ring sizing knobs
- [`docs/adr/023-receipt-plane-ring-partitioning.md`](adr/023-receipt-plane-ring-partitioning.md) — receipt-plane partition design and reverse-migration recipe
- [`docs/adr/025-sharded-enqueue-heads.md`](adr/025-sharded-enqueue-heads.md) — enqueue-head sharding design and partitioned-FIFO contract
- [`docs/grafana/awa-dashboard.json`](grafana/awa-dashboard.json) — Prometheus dashboard with the rotation/prune panels

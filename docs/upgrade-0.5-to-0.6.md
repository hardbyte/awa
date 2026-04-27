# Upgrade Checklist: 0.5.x → 0.6

This is the operator-facing rollout sheet for moving an existing 0.5.x
cluster to 0.6 (queue-storage-by-default). The companion long-form
explanation is in [migrations.md](migrations.md). This file is the
short version: one-screen pre-flight, two phases, an explicit rollback
boundary, and the health checks to run at each step.

> **Fresh installs should not need this file.** A new cluster's
> intended experience is `awa migrate` + start workers. A current 0.6
> caveat means fresh installs run the same `prepare → enter-mixed-
> transition → finalize` sequence once at first install (the `auto`
> role does not yet auto-promote a fresh DB through to `active`); see
> migrations.md ["Fresh install"](migrations.md#fresh-install-no-prior-canonical-data).
> Tracking the auto-finalize gap in [issue #197](https://github.com/hardbyte/awa/issues/197).
> This checklist proper is for **upgrading existing 0.5.x clusters**,
> where canonical drain is unavoidable.

## Pre-flight

- [ ] Cluster is on `0.5.latest` everywhere (no mixed older versions)
- [ ] `awa storage status` shows `state=canonical / current=canonical / active=canonical / prepared=NULL`
- [ ] Schema is at the latest 0.5.x migration: `SELECT MAX(version) FROM awa.schema_version`
- [ ] Backups taken (or backup tooling verified) — there's no `awa migrate` downgrade path
- [ ] Operator has access to:
  - Run `awa storage` commands against the production DSN
  - Inspect `awa.runtime_instances` for live capability reporting
  - Watch worker logs / Grafana dashboards during the cutover
- [ ] Confirmed every queue-storage-capable Python worker is on a binding that includes the ADR-023 claim-ring knobs (commit `3940ba1` or later); skip if running Rust-only

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
#    schema-install cost.
awa --database-url "$DATABASE_URL" storage prepare-queue-storage-schema --schema awa_exp

# 3. Record the prepared engine.
awa --database-url "$DATABASE_URL" storage prepare --engine queue_storage

# 4. Verify prepared state.
awa --database-url "$DATABASE_URL" storage status
#    → current=canonical, active=canonical, prepared=queue_storage, state=prepared

# 5. Confirm every live runtime is queue-storage capable BEFORE the
#    flip. This is the gate that prevents canonical-only workers
#    from surviving into mixed_transition.
psql "$DATABASE_URL" -c "
  SELECT count(*) FILTER (WHERE storage_capability != 'queue_storage') AS canonical_only
  FROM awa.runtime_instances
  WHERE last_seen_at > now() - interval '90 seconds';
"
#    → canonical_only must be 0

# 6. Flip routing. New writes and cron enqueues go to queue storage.
awa --database-url "$DATABASE_URL" storage enter-mixed-transition

# 7. Watch canonical drain.
watch -n 5 'psql "$DATABASE_URL" -c "
  SELECT state, count(*) FROM awa.jobs WHERE NOT EXISTS (
    SELECT 1 FROM awa.runtime_storage_backends WHERE backend = '\''queue_storage'\''
  ) GROUP BY state;
"'
#    → wait for the canonical backlog to reach 0

# 8. Finalize once canonical backlog is empty.
awa --database-url "$DATABASE_URL" storage finalize

# 9. Verify active state.
awa --database-url "$DATABASE_URL" storage status
#    → current=queue_storage, active=queue_storage, prepared=NULL, state=active
```

## Health checks per step

| After step | Watch for |
|------------|-----------|
| migrate | `SELECT MAX(version) FROM awa.schema_version` advances |
| prepare-queue-storage-schema | `\dn awa_exp` shows the schema; tables created |
| prepare | `awa storage status` reports `state=prepared` |
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

- **Python workers without claim-ring knobs.** Pre-`3940ba1` Python bindings don't accept `queue_storage_claim_slot_count` / `queue_storage_claim_rotate_interval_ms`. They'll still run with default values and the rollout will work, but operators wanting non-default ring sizing must rebuild the Python wheel against a newer `awa-python` first.
- **Held-tx during finalize.** A long-running canonical transaction (e.g., reporting query) blocks vacuum, which can stall partition prune in the queue-storage tables. `awa.maintenance.prune.attempts{awa_ring_outcome="blocked"}` will rise. Identify and terminate the held-tx; prune resumes on the next maintenance tick.
- **0.6 rollback after queue-storage work.** Not supported via `awa storage abort` once any rows exist in queue-storage tables. Plan accordingly: keep `0.6` workers available throughout the transition window. Only emergency rollback path is database restore.

## Cross-references

- [migrations.md](migrations.md) — full migration story including SQL-level identities
- [configuration.md](configuration.md) — claim-ring / lease-ring sizing knobs
- [`docs/adr/023-receipt-plane-ring-partitioning.md`](adr/023-receipt-plane-ring-partitioning.md) — receipt-plane partition design and reverse-migration recipe
- [`docs/grafana/awa-dashboard.json`](grafana/awa-dashboard.json) — Prometheus dashboard with the rotation/prune panels
- Issue [#197](https://github.com/hardbyte/awa/issues/197) — 0.6 release-readiness tracker

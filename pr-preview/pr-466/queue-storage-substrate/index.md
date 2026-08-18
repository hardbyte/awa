> For the complete AWA documentation index, see [`llms.txt`](../llms.txt).

# Queue storage

Queue storage is Awa's PostgreSQL layout for runnable work, in-flight attempts, deferred work, terminal history, and the small control tables that coordinate workers. It is an implementation boundary, not an application API: producers and workers should use the Rust or Python clients, and operators should use the CLI, admin API, and documented read views.

This page explains the storage shape an operator needs to understand. For the end-to-end runtime design, see [Architecture](../architecture/index.md). For migration ownership and external migration runners, see [Migrations](../migrations/index.md).

## Why the queue is split into planes

A single frequently-updated jobs table accumulates dead tuples and makes claiming, history retention, and long-running attempts compete for the same indexes. Awa instead gives each workload a storage shape suited to its lifecycle:

| Plane | Primary objects | Purpose |
| --- | --- | --- |
| Ready queue | `ready_entries_*`, ready segments, tombstones | Append runnable work and claim it in ordered lanes. Whole ring slots can later be reclaimed. |
| Deferred queue | `deferred_jobs` | Hold scheduled and retryable work until maintenance promotes it. |
| Execution | claim receipts, claim batches, closures, `leases_*`, `attempt_state` | Prove which attempt owns a job. Short jobs use compact receipt evidence; jobs needing mutable state materialise a lease. |
| Terminal history | `done_entries_*`, compact completion batches, count deltas and rollups | Retain completion facts without copying the full job body into every terminal row. |
| Operator hold | `dlq_entries` | Keep explicitly dead-lettered work available for inspection, retry, or purge. |
| Control | queue metadata, lane heads, ring ledgers, runtimes, cron and uniqueness tables | Coordinate dispatch and maintenance without putting mutable metadata on the hot history path. |

The public `{schema}.terminal_jobs` view hydrates terminal facts with retained job bodies. Physical ring tables are internal and must not be mutated directly.

## How storage stays bounded

Ready, receipt, lease, and terminal families are partitioned into ring slots. Maintenance advances each ring only after its reclaimability checks succeed, then truncates the old slot as a unit. Long-lived database snapshots can delay reclamation; they do not allow maintenance to skip the safety checks. Deferred and DLQ rows use their own promotion and retention paths rather than the ring.

One worker holds the maintenance advisory lock at a time. That leader promotes due work, rescues stale attempts, rotates rings, folds count deltas, and publishes queue health. If it exits, another worker can take over from durable PostgreSQL state.

## Default and custom schemas

`awa migrate` installs the canonical control objects and the default queue-storage substrate in the `awa` schema. That default has a stable shape and cannot be reset through `prepare-queue-storage-schema`; `DROP SCHEMA awa CASCADE` would also destroy migration and transition metadata and is not a supported recovery action.

Custom storage schemas are an advanced operational tool for a separately sized substrate or a side-by-side transition:

```bash
awa storage prepare-queue-storage-schema \
  --schema my_jobs \
  --queue-slot-count 32 \
  --lease-slot-count 16
```

The command invokes the idempotent `awa.install_queue_storage_substrate(...)` helper under a per-schema advisory transaction lock. The helper is activation-neutral: preparing a schema does not route work to it. Activation is a separate staged transition described in [Upgrading from 0.5 to 0.6](../upgrade-0.5-to-0.6/index.md).

The installer is `SECURITY INVOKER`; its caller needs DDL privileges on the target schema. Workers need runtime DML privileges and `TRUNCATE` for guarded ring reclamation, but do not need DDL. See [Database roles](../security/database-roles/index.md).

## Operator rules

- Treat physical queue-storage tables and helper functions as internal unless a page explicitly names a public surface.
- Use the CLI transition commands rather than editing `storage_transition_state` or `runtime_storage_backends`.
- Do not reset or drop the default `awa` schema. Restore from backup and rerun migrations for a full-cluster rebuild.
- Keep analytical transactions short on the primary; a pinned MVCC horizon delays best-effort ring reclamation.
- When using a custom schema, apply the same runtime grants to it and prepare it with the migrator role.

## Related reading

- [Architecture](../architecture/index.md) — runtime, storage, lifecycle, and recovery.
- [Migrations](../migrations/index.md) — migration ownership and extracted SQL.
- [Database roles](../security/database-roles/index.md) — production role separation and grants.
- [Storage upgrade guide](../upgrade-0.5-to-0.6/index.md) — staged activation and drain.
- [ADR-019](../adr/019-queue-storage-redesign/index.md) and [ADR-023](../adr/023-receipt-plane-ring-partitioning/index.md) — decision rationale and consequences.

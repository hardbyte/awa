# Queue-storage substrate: ownership and customisation

Awa's queue-storage substrate is the set of per-schema tables, indexes,
sequences, and helper functions the runtime relies on to claim,
execute, and finalise jobs against the queue-storage engine. This page
explains who owns those objects, how to customise them, and the
guardrails that protect the default `awa` schema from accidental
destructive operations.

## Ownership contract

| Owner | Scope | When it runs |
|---|---|---|
| **`awa migrate`** | The canonical schema (`awa.schema_version`, `awa.runtime_instances`, `awa.storage_transition_state`, `awa.runtime_storage_backends`, etc.) **plus** the default queue-storage substrate at `awa.*` (`awa.ready_entries`, `awa.ready_tombstones`, `awa.done_entries`, `awa.leases`, `awa.queue_ring_state`, `awa.queue_terminal_live_counts`, the partitions for each, the `awa.claim_ready_runtime` helper, and the `awa.install_queue_storage_substrate` function itself). | Install time and operator-driven upgrades. |
| **`QueueStorage::prepare_schema()` (Rust) / `awa storage prepare-queue-storage-schema` (CLI)** | Non-default queue-storage schemas (anything other than `awa`), repair flows, test setups. Calls `awa.install_queue_storage_substrate(<schema>, ...)` and performs a small set of legacy upgrade fixups that don't belong in a forward-only DDL function. | Custom-schema deployments; targeted repair; the test suite. |
| **The `awa.install_queue_storage_substrate(p_schema, ...)` SQL helper** | Per-schema DDL only. Idempotent. Activation-neutral — does NOT touch `awa.runtime_storage_backends` or `awa.storage_transition_state`. | Called by both `awa migrate` (for the default schema) and `prepare_schema()` (for custom schemas). Single source of truth for substrate DDL. |

The helper takes a per-schema advisory transaction lock
(`awa.queue_storage.install:<schema>`) so concurrent installs from
Rust workers, the CLI, Python, or externally-extracted migration SQL
serialise on the same key.

## The default `awa` schema is migration-owned and default-shaped

For `p_schema = 'awa'` the helper rejects non-default configuration
with `ERRCODE = 22023`:

- `lease_claim_receipts` must be `TRUE`
- `queue_slot_count` must be `16`
- `lease_slot_count` must be `8`
- `claim_slot_count` must be `8`

The default `awa.*` substrate is the single stable shape every fresh
installation gets. If you need different slot counts or
`lease_claim_receipts = FALSE`, use a custom queue-storage schema (see
below). Attempts to tune the default schema get a clear error
pointing at the custom-schema path.

## Custom queue-storage schemas

For non-default deployments — say a high-throughput tenant that wants
`queue_slot_count = 32`, or a side-by-side rebuild during an
incident — install a substrate under a different schema name:

```bash
awa storage prepare-queue-storage-schema \
  --schema my_jobs \
  --queue-slot-count 32 \
  --lease-slot-count 16
```

The CLI calls `awa.install_queue_storage_substrate('my_jobs', 32, 16, 8, TRUE)`
under the per-schema advisory lock. Activate the schema as the
queue-storage backend via `awa storage prepare`, `awa storage
enter-mixed-transition`, and `awa storage finalize` once you're ready.
See [the upgrade guide](upgrade-0.5-to-0.6.md) for the staged flow.

## `--reset` is rejected for `--schema awa`

`awa storage prepare-queue-storage-schema --reset` runs
`DROP SCHEMA IF EXISTS <schema> CASCADE` before re-preparing. For
`--schema awa` that would also drop `schema_version`,
`runtime_instances`, `storage_transition_state`, and every other
canonical migration table, leaving the database in an unrecoverable
state.

The CLI rejects this combination:

```text
Error: "Refusing to DROP SCHEMA awa CASCADE — schema 'awa' is the
default migration-owned queue-storage substrate and also contains the
canonical migration tables (schema_version, runtime_instances,
storage_transition_state, etc.). Use --schema <other> for a throwaway
substrate, or 'awa storage abort' to rewind an in-flight transition."
```

To rebuild a queue-storage substrate from scratch:

- **Testing or recovery:** target a custom schema name with
  `--schema <other>` and activate it via the storage transition flow.
- **Rewind an in-flight transition:** use `awa storage abort`.
- **Full cluster rebuild:** restore from backup, then run `awa migrate`.

`DROP SCHEMA awa CASCADE` is not a supported operator action.

## Driving installs and upgrades from external migration tooling

Teams that already run their schema changes through a tool like
Sqitch, Liquibase, or a hand-rolled migration runner do not need to
invoke `awa migrate` or any other Rust binary. The migration set
extracted with `awa migrate --sql` (or `--extract-to`) is complete:
it includes the substrate DDL via the v023 helper, and the staged
transition is driven by SQL functions.

### Fresh install (no canonical data yet)

After applying the migration files, the first runtime that boots
calls `awa.storage_auto_finalize_if_fresh('awa')` which atomically
advances `canonical → active` when `awa.jobs` is empty and no live
runtimes have heartbeated. External tooling can call the same
function as a post-migrate step to land in `active` before the first
worker even starts:

```sql
SELECT awa.storage_auto_finalize_if_fresh('awa');
```

`storage_auto_finalize_if_fresh` has `GRANT EXECUTE ... TO PUBLIC`,
so the EXECUTE bit is open to any role. The function is
`SECURITY INVOKER` and reads/writes
`awa.storage_transition_state`, `awa.jobs`, `awa.runtime_instances`,
and `awa.runtime_storage_backends`, so callers still need the normal
runtime/migrator table privileges on those.

### Upgrade from an existing canonical-only deployment

`storage_auto_finalize_if_fresh` refuses to short-circuit when
canonical work or live runtimes exist. The operator drives the
staged transition with three SQL function calls, each of which
mirrors the equivalent `awa storage` CLI subcommand:

```sql
-- (1) Mark queue-storage as the prepared target.
SELECT awa.storage_prepare('queue_storage', '{"schema":"awa"}'::jsonb);

-- (2) Bring up at least one worker with
--     transition_role=queue_storage_target. Stop any canonical-only
--     workers. Then flip routing into mixed mode:
SELECT awa.storage_enter_mixed_transition();

-- (3) Wait for workers to drain the canonical backlog onto
--     queue-storage. The two SQL gates `storage_finalize` enforces
--     are observable directly:
--
--       SELECT awa.canonical_live_backlog();
--       -- must return 0 before finalize will advance.
--
--       SELECT count(*)
--       FROM awa.runtime_instances
--       WHERE storage_capability IN ('canonical', 'canonical_drain_only')
--         AND last_seen_at + make_interval(
--               secs => GREATEST(((GREATEST(snapshot_interval_ms, 1000) / 1000) * 3)::int, 30)
--             ) >= now();
--       -- must also be 0 (no live canonical or drain-only runtimes).
--
--     When both are 0, finalize:
SELECT awa.storage_finalize();
```

`storage_enter_mixed_transition` will reject the call until at least
one live `queue_storage_target` runtime is heartbeating.
`storage_finalize` will reject the call while
`awa.canonical_live_backlog() > 0` or while any canonical /
canonical-drain-only runtime is still inside its liveness window.
Both gates are deliberate — they prevent operators from flipping
routing onto a substrate that has no executor or while canonical
work or canonical-mode workers are still active.

The orchestration (start new-mode workers, stop old-mode workers,
wait for drain) is unchanged from the CLI flow — only the invocation
surface is different.

## Design rationale

The split between migration-owned default substrate and helper-installed
custom substrate exists for three reasons:

- **`awa migrate --sql` / `--extract-to` must reproduce the full
  default runtime schema** for external migration tools to be useful.
  All substrate DDL is reachable from the migration through the SQL
  helper, so the extracted SQL is complete.
- **Migrations that depend on queue-storage tables can write
  unconditional DDL.** Operations like the
  `queue_terminal_live_counts` decrement inside
  `awa.delete_job_compat()` need the counter table to exist. Migration
  ordering guarantees it.
- **The default schema cannot be accidentally destroyed.** The reset
  guard above protects operators from a `DROP SCHEMA awa CASCADE` that
  would take the canonical migration tables with it.

The helper is `SECURITY INVOKER` so callers need their own DDL
privileges on the target schema; the runtime role does not gain DDL
through the helper, which keeps the principle-of-least-privilege role
model intact.

## See also

- [`docs/migrations.md`](migrations.md) — migration policy and the
  `awa migrate --sql` / `--extract-to` story.
- [`docs/architecture.md`](architecture.md) — overall storage layering.
- [`docs/security.md`](security.md) — role boundaries and the
  `SECURITY INVOKER` posture.
- [`docs/upgrade-0.5-to-0.6.md`](upgrade-0.5-to-0.6.md) — operator flow
  for an existing cluster.

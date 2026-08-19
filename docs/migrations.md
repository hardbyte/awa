# Migration Guide

This guide explains how to install and upgrade the Awa schema, integrate an external migration runner, and plan rollback. Release-specific prerequisites and transition procedures live in the relevant upgrade guide.

## Migration Contract

Awa schema migrations are forward-only, atomic, and idempotent:

- **Atomic.** One `awa migrate` (or `migrations::run`) applies the whole pending range inside a single transaction guarded by a transaction-scoped advisory lock. A failed, cancelled, or interrupted run commits nothing — there is no half-applied schema to reconcile, and concurrent runners on the same database serialize and converge on one application. Every migration is transaction-safe by policy (no `CREATE INDEX CONCURRENTLY`, `VACUUM`, or explicit transaction control), which is what makes the single-transaction guarantee possible; a unit test enforces it.
- **Idempotent.** Every migration is re-runnable: DDL is guarded (`IF NOT EXISTS`, `CREATE OR REPLACE`, `DROP TRIGGER IF EXISTS` before `CREATE TRIGGER`), and each records its own version with `ON CONFLICT (version) DO NOTHING`. Applying the set again — in whole or file by file — converges on a byte-identical schema rather than erroring. Tests apply every migration twice and compare the resulting catalog against a clean install.

The runner applies only migrations newer than the recorded version, so a repeat run is normally a no-op regardless.

Migration behavior fails closed:

- a binary refuses a schema newer than it understands;
- missing or unparseable runtime capability does not satisfy a migration gate;
- an expand migration may require a minimum compatible runtime patch; and
- a migration that cannot be made rolling-compatible may require an explicitly documented exclusive window.

Representation changes follow [ADR-041](adr/041-rolling-upgrade-policy.md): expand the schema while the old representation remains authoritative, flip authority only after the fleet proves capability, and remove the old representation in a later release. The release upgrade guide defines supported ordering, capability gates, and the rollback boundary for each change.

The default queue-storage substrate in `awa.*` is migration-owned. Custom queue-storage schemas are installed through the same SQL helper; see [Queue-storage substrate](queue-storage-substrate.md) for ownership and configuration details.

## Fresh Install (No Prior Canonical Data)

### CLI

```bash
awa --database-url "$DATABASE_URL" migrate
```

### Rust

```rust
awa::migrations::run(&pool).await?;
```

### Python

```python
await client.migrate()
```

or:

```python
await awa.migrate(database_url)
```

## Upgrade an Existing Database

Read the release-specific guide before applying migrations:

- [Upgrade 0.5 to 0.6](upgrade-0.5-to-0.6.md)
- [Upgrade 0.6 to 0.7](upgrade-0.6-to-0.7.md)

Then run the migration command at the point specified by that guide:

```bash
awa --database-url "$DATABASE_URL" migrate
```

Do not assume that every release requires schema-first deployment. A guide may require a compatibility patch before migration, permit migration and binary rollout in either order, or define a capability-gated flip after the binaries have rolled.

Use the storage status command when an upgrade includes a storage transition:

```bash
awa --database-url "$DATABASE_URL" storage status
```

The [queue-storage substrate guide](queue-storage-substrate.md) documents preparation of custom schemas and non-default slot counts.

## External Migration Tooling

To manage Awa SQL with Flyway, Liquibase, dbmate, or another runner, extract the bundled migrations:

```bash
awa --database-url "$DATABASE_URL" migrate --extract-to ./sql/awa
```

Use `awa migrate --sql` to print the same migration set to standard output. That output is wrapped in a single transaction that takes the runner's advisory lock, so piping it into `psql` is atomic and serialized exactly like `awa migrate`:

```bash
awa migrate --sql | psql -v ON_ERROR_STOP=1 "$DATABASE_URL"
```

`ON_ERROR_STOP=1` is required to *detect* a failure, not to be safe from one. Plain `psql` reports the error, fails every following statement, turns the trailing `COMMIT` into a rollback — and still exits `0`, so a deploy script that gates on the exit status reads a fully rolled-back migration as success.

The rendered SQL looks like this:

```sql
BEGIN;
SELECT pg_advisory_xact_lock(4708303813013489490);
-- Migration V1: ...
COMMIT;
```

Pass `--no-transaction` to omit the wrapper when the consuming runner opens its own transaction per migration — Flyway, Liquibase, and dbmate all do. The per-version files written by `--extract-to` are never wrapped, for the same reason.

Applying unwrapped SQL through a runner that does **not** wrap it (piping into `psql`, which is autocommit) is not atomic: a failure part-way leaves the schema half-migrated. Either keep the wrapper or make sure the runner supplies one.

Each extracted file is individually re-runnable, so a runner that crashes between applying a file and recording it can safely retry that file.

0.7 changes two things about the extracted set, both one-time. Migration v001 gained the re-runnability guards the rest of the set already had, so its file content changed; if your runner validates checksums of applied migrations, re-extract and clear the recorded checksum for V1 (the applied schema is unaffected — only the stored checksum is stale). Filenames are unchanged except `V17` and `V21`, whose descriptions contain `/` and which no earlier extraction could write at all.

`--from` / `--to` / `--version` select a range to *render*; they are only valid with `--sql` or `--extract-to`. Applying always brings the database to the current version, so `awa migrate` rejects them rather than silently applying a wider range.

`python -m awa migrate --sql` renders the identical output, wrapper included, and takes the same `--no-transaction` flag.

The same SQL is available programmatically:

- Rust: `awa::migrations::migration_sql()`, with the runner's advisory-lock key as `awa::migrations::MIGRATION_LOCK_KEY`
- Python: `awa.migrations()`, with the key as `awa.migration_lock_key()`

Extracted SQL does not execute Rust preflights such as runtime version floors or exclusive-window checks. The external rollout must enforce the release guide's binary prerequisites and preserve the documented lock or ordering boundary before applying the SQL.

Operational storage commands such as `storage prepare`, `storage enter-mixed-transition`, and `storage finalize` are not migration DDL. Run them only where the applicable upgrade guide instructs; do not append them to the extracted migration files.

## Check Schema Version

From SQL:

```sql
SELECT MAX(version) AS schema_version
FROM awa.schema_version;
```

From Rust:

```rust
let version = awa::migrations::current_version(&pool).await?;
```

Schema versions increase monotonically. Application code should call `awa migrate` rather than depend on a particular numeric version.

## Rollback

Awa does not ship down migrations. Use a database backup, snapshot, or reverse SQL owned by your external migration system if the schema itself must be restored.

Application rollback depends on the release phase:

- before an authority flip, the additive expanded schema normally remains compatible with the documented previous release;
- after a flip, pre-flip binaries are fenced and rollback is limited to flip-aware releases; and
- after a contract migration, follow that release's separately documented compatibility boundary.

Do not infer rollback support from the presence of old tables or columns. Follow the release upgrade guide, which identifies the last reversible step and any required operator action.

## Related Guides

- [Queue-storage substrate](queue-storage-substrate.md)
- [Deployment](deployment.md)
- [PostgreSQL roles and privileges](security.md)
- [Troubleshooting](troubleshooting.md)

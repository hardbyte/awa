# Migration Guide

This guide explains how to install and upgrade the Awa schema, integrate an external migration runner, and plan rollback. Release-specific prerequisites and transition procedures live in the relevant upgrade guide.

## Migration Contract

Awa schema migrations are forward-only. The built-in runner serializes migration work, applies each migration transactionally, and makes migrations safe to re-run where practical.

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

Use `awa migrate --sql` to print the same migration set to standard output.

The same SQL is available programmatically:

- Rust: `awa::migrations::migration_sql()`
- Python: `awa.migrations()`

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

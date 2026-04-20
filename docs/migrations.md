# Migration Guide

This guide covers fresh installs, upgrades, external migration tooling, and rollback strategy.

## Migration Model

Awa ships forward-only schema migrations.

Current guarantees from the migration layer:

- migrations are additive-only
- fresh installs bootstrap the full schema
- rerunning migrations is safe
- older pre-0.4 version numbering is normalized during upgrade

The additive-only policy is what makes rolling upgrades practical.

## Fresh Install

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

## Upgrading an Existing Database

The normal upgrade order is:

1. deploy the new schema with `awa migrate`
2. roll out new application code
3. let old workers drain and exit cleanly

Because migrations are additive-only, old workers should continue to function during the rollout as long as your application-level payload compatibility still holds.

## Storage Transition Preparation

This release introduces a generic storage-transition framework that future
storage-engine upgrades can build on. The important point for operators is that
it does **not** change the current execution engine.

New surfaces:

```bash
awa --database-url "$DATABASE_URL" storage status
awa --database-url "$DATABASE_URL" storage prepare --engine queue_storage
awa --database-url "$DATABASE_URL" storage abort
```

Current behavior:

- `storage status` reports the singleton row in `awa.storage_transition_state`
- `storage prepare` records a future engine and optional metadata, but keeps
  enqueue routing and worker execution on canonical storage
- `storage abort` clears a prepared-but-inactive future engine
- workers continue to run the canonical engine before and after `prepare`

This is intentional. The `0.5.x` prep release is only adding the reusable
tables, status APIs, capability metadata, and compat-routing seam needed for a
later engine migration. It is not activating queue storage.

### Why This Exists

If Awa needs another storage redesign in the future, the plan is to reuse the
same framework:

- `awa.storage_transition_state`
- transition epochs
- storage status / prepare / abort operator commands
- runtime storage capability reporting
- `awa.insert_job_compat()` as the write-routing seam

Only the engine-specific install, backlog checks, and finalization logic should
change between migrations.

## External Migration Tooling

If you manage SQL with Flyway, Liquibase, dbmate, or a homegrown process, extract the bundled SQL:

```bash
awa --database-url "$DATABASE_URL" migrate --extract-to ./sql/awa
```

That writes one SQL file per migration. The same SQL is also available programmatically:

- Rust: `awa::migrations::migration_sql()`
- Python: `awa.migrations()`

## Checking Schema Version

From SQL:

```sql
SELECT MAX(version) AS schema_version
FROM awa.schema_version;
```

From Rust:

```rust
let version = awa::migrations::current_version(&pool).await?;
```

## Compatibility Notes

Relevant behavior:

- pre-0.4 legacy version rows are normalized automatically during upgrade
- schema versions increase monotonically as new migrations are added; use `awa migrate` rather than depending on a specific numeric version in application code
- `v005` switches admin metadata maintenance from row-level to statement-level triggers
- `SchemaNotMigrated` means your application expects a newer schema than the database currently has
- there are no bundled down migrations

## Rollback Strategy

There are two distinct rollback cases.

### 1. Application Rollback

If a release has to be reverted but the Awa schema migration already ran:

- roll back application code first
- leave the Awa schema in place

That is the expected path because the migrations are additive-only.

### 2. Database Rollback

Awa does not ship reverse SQL. If you need to undo the schema itself, use your normal database rollback controls:

- restore from backup or snapshot
- or own the reverse SQL in your external migration system

Do not expect `awa migrate` to downgrade the schema.

## Breaking Changes

If Awa ever needs a non-additive schema change, the intended contract is a major-version upgrade with a stop-the-world procedure documented explicitly for that release.

## Recommended Production Flow

```bash
# 1. Apply schema
awa --database-url "$DATABASE_URL" migrate

# 2. Optional: record and verify future storage prep state
awa --database-url "$DATABASE_URL" storage status

# 3. Roll out workers
# 4. Verify runtime health / queue stats
awa --database-url "$DATABASE_URL" queue stats
```

## Next

- [PostgreSQL roles and privileges](security.md)
- [Deployment guide](deployment.md)
- [Configuration](configuration.md)
- [Troubleshooting](troubleshooting.md)

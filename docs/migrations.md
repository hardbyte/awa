# Migration Guide

This guide covers fresh installs, upgrades, queue-storage cutover, external
migration tooling, and rollback strategy.

## Migration Model

Awa ships forward-only schema migrations.

Current guarantees from the migration layer:

- migrations are additive-only
- fresh installs bootstrap the full schema
- rerunning migrations is safe
- older pre-0.4 version numbering is normalized during upgrade

The additive-only policy still matters, but it is not the whole story anymore:
switching the active worker engine to queue storage is an operational cutover,
not a mixed-engine rolling upgrade.

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

## Queue Storage Cutover

Schema migrations remain additive, but selecting queue storage as the active
worker engine changes where new jobs are written and where workers claim from.
Treat that as a storage cutover, not as a rolling mixed-engine deployment.

Rules:

- do not run canonical workers and queue-storage workers against the same
  database at the same time
- drain or stop the old worker fleet before starting the new one
- producers and admin surfaces can keep using the compatibility SQL/API layer,
  but workers should use one engine only
- `awa.runtime_storage_backends` is the database-level selector for the active
  queue-storage schema

Recommended cutover:

1. Apply the schema with `awa migrate`.
2. Drain and stop the existing worker fleet completely.
3. Start the replacement fleet with queue storage enabled.
4. Verify the selected backend schema and queue health.
5. Resume normal traffic and monitor lease rotation / prune.

Rust workers enable queue storage explicitly in code:

```rust
use awa::QueueStorageConfig;
use std::time::Duration;

let client = awa::Client::builder(pool.clone())
    .queue("default", awa::QueueConfig::default())
    .queue_storage(
        QueueStorageConfig {
            schema: "awa_qs".to_string(),
            ..Default::default()
        },
        Duration::from_secs(1),
        Duration::from_millis(50),
    )
    .build()?;
```

Python workers do the same at `start()` time:

```python
await client.start(
    [("default", 4)],
    queue_storage_schema="awa_qs",
    queue_storage_queue_rotate_interval_ms=1000,
    queue_storage_lease_rotate_interval_ms=50,
)
```

Confirm the active backend from SQL:

```sql
SELECT backend, schema_name, updated_at
FROM awa.runtime_storage_backends;
```

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
- `v010` / `v011` add the queue-storage compatibility layer and active-backend selector
- `SchemaNotMigrated` means your application expects a newer schema than the database currently has
- there are no bundled down migrations

## Rollback Strategy

There are two distinct rollback cases.

### 1. Application Rollback

If a release has to be reverted but the Awa schema migration already ran:

- roll back application code first
- leave the Awa schema in place

That is the expected path because the migrations are additive-only.

### 2. Storage-Engine Rollback

Rolling back from queue storage to canonical workers on the same live database
is not supported.

Canonical workers do not claim from queue-storage segments, and queue-storage
workers update the database-level active backend selector when they install.

If a release must be backed out after the database has been cut over to queue
storage:

- keep the database on queue storage and roll back only application code, or
- restore a database backup/snapshot taken before the cutover

Do not treat mixed canonical + queue-storage worker fleets as a rollback plan.

### 3. Database Rollback

Awa does not ship reverse SQL. If you need to undo the schema itself, use your normal database rollback controls:

- restore from backup or snapshot
- or own the reverse SQL in your external migration system

Do not expect `awa migrate` to downgrade the schema.

## Breaking Changes

Queue storage is the current example of a larger storage transition: the SQL
migrations are additive, but the worker cutover itself is operationally
stop-the-world. Future non-additive changes would need the same level of
explicit release documentation.

## Recommended Production Flow

```bash
# 1. Apply schema
awa --database-url "$DATABASE_URL" migrate

# 2. Roll out one worker engine
# 3. Verify runtime health / queue stats
awa --database-url "$DATABASE_URL" queue stats
```

## Next

- [PostgreSQL roles and privileges](security.md)
- [Deployment guide](deployment.md)
- [Configuration](configuration.md)
- [Troubleshooting](troubleshooting.md)

# Migration Guide

This guide covers fresh installs, ordinary upgrades, the `0.5.x -> 0.6`
queue-storage transition, external migration tooling, and rollback strategy.

## Migration Model

Awa ships forward-only schema migrations.

Current guarantees from the migration layer:

- migrations are additive-only
- fresh installs bootstrap the full schema
- rerunning migrations is safe
- older pre-`0.4` version numbering is normalized during upgrade

That additive-only guarantee is enough for ordinary code-and-schema upgrades.
The `0.5.x -> 0.6` storage change adds one extra concern: schema migration and
worker-engine activation are separate steps.

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

## Ordinary Upgrades

For normal additive releases, the upgrade order is:

1. deploy the new schema with `awa migrate`
2. roll out new application code
3. let old workers drain and exit cleanly

Because migrations are additive-only, older workers continue to function during
that rollout as long as your application-level payload compatibility still
holds.

## `0.5.x -> 0.6` Queue-Storage Upgrade

The `0.5.x` line ends at schema version `9`. The `0.6` queue-storage release
adds schema version `10`, which installs:

- the queue-storage compatibility layer
- the active-backend selector in `awa.runtime_storage_backends`
- `awa.jobs` / `awa.insert_job_compat()` routing that follows the active
  backend when queue storage is activated

Important properties of that migration:

- running `awa migrate` does **not** activate queue storage by itself
- until queue storage is activated, `awa.jobs` and
  `awa.insert_job_compat()` continue to use canonical tables
- existing `0.5` workers can keep processing canonical queues after the schema
  migration

The test suite covers this explicitly:

- migrating a `0.5.x` schema to current leaves `awa.active_queue_storage_schema()`
  as `NULL`
- compatibility inserts still route to canonical tables before activation
- once queue storage is activated, compatibility writes route to queue storage
  and `awa.jobs` follows the active backend only

### Supported Cutover

The supported `0.5.x -> 0.6` cutover is:

1. apply schema migrations
2. keep existing `0.5` workers running while the new schema is in place
3. drain and stop the canonical worker fleet
4. start the `0.6` queue-storage worker fleet
5. verify the active backend and queue health

Confirm activation from SQL:

```sql
SELECT backend, schema_name, updated_at
FROM awa.runtime_storage_backends;
```

### Why This Is Not Yet a Pure Rolling Worker Upgrade

Once queue storage is activated, `awa.jobs` switches to the active
queue-storage backend. Canonical rows remain in the database, but they are no
longer part of the active runtime view. That is why Awa currently treats
queue-storage activation as a cutover step rather than a mixed-engine rollout.

Supporting a fully ordinary worker rollout:

- `0.5` workers keep running
- `0.6` workers roll out
- old workers terminate
- no explicit storage activation pause

would require one more design step: separating **queue-storage schema
installation** from **queue-storage backend activation**, then making backend
activation an explicit operator action rather than something worker startup
performs implicitly.

## Queue-Storage Worker Configuration

Rust workers default to queue storage. Override only when you need a specific
schema name or non-default segment sizing:

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

Python workers expose the same knobs at `start()` time:

```python
await client.start(
    [("default", 4)],
    queue_storage_schema="awa_qs",
    queue_storage_queue_rotate_interval_ms=1000,
    queue_storage_lease_rotate_interval_ms=50,
)
```

## External Migration Tooling

If you manage SQL with Flyway, Liquibase, dbmate, or a homegrown process,
extract the bundled SQL:

```bash
awa --database-url "$DATABASE_URL" migrate --extract-to ./sql/awa
```

That writes one SQL file per migration version. The same SQL is also available
programmatically:

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

- pre-`0.4` legacy version rows are normalized automatically during upgrade
- schema versions increase monotonically as new migrations are added; use
  `awa migrate` rather than depending on a specific numeric version in
  application code
- `v005` switches admin metadata maintenance from row-level to statement-level
  triggers
- `v010` adds the queue-storage compatibility layer and active-backend
  selector
- `SchemaNotMigrated` means your application expects a newer schema than the
  database currently has
- Awa does not ship down migrations

## Rollback Strategy

There are three distinct rollback cases.

### 1. Application Rollback

If a release has to be reverted but the Awa schema migration already ran:

- roll back application code first
- leave the Awa schema in place

That is the expected path because the migrations are additive-only.

### 2. Queue-Storage Rollback

Rolling back from queue storage to canonical workers on the same live database
is not supported as a mixed-worker strategy.

Canonical workers do not claim from queue-storage segments, and queue-storage
activation changes what `awa.jobs` exposes as the active backend.

If a release must be backed out after queue storage has been activated:

- keep the database on queue storage and roll back only application code, or
- restore a database backup/snapshot taken before activation

Do not treat mixed canonical + queue-storage worker fleets as a rollback plan.

### 3. Database Rollback

Awa does not ship reverse SQL. If you need to undo the schema itself, use your
normal database rollback controls:

- restore from backup or snapshot
- or own the reverse SQL in your external migration system

Do not expect `awa migrate` to downgrade the schema.

## Recommended Production Flow

For the `0.5.x -> 0.6` transition:

```bash
# 1. Apply schema
awa --database-url "$DATABASE_URL" migrate

# 2. Let 0.5 workers continue while the schema is in place

# 3. Drain old workers and start queue-storage workers

# 4. Verify runtime health / queue stats
awa --database-url "$DATABASE_URL" queue stats
```

## Next

- [PostgreSQL roles and privileges](security.md)
- [Deployment guide](deployment.md)
- [Configuration](configuration.md)
- [Troubleshooting](troubleshooting.md)

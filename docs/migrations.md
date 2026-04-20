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

Awa ships a generic storage-transition framework that schema migrations,
operator tooling, and future storage-engine upgrades share. The framework is
intentionally storage-engine neutral: the first user is the planned
`canonical -> queue_storage` migration, but the same tables and helpers can
drive any later engine transition without a second ad-hoc path.

In a canonical deployment the framework is dormant — the routing seam and
capability metadata are present, but worker execution remains canonical.

New surfaces:

```bash
awa --database-url "$DATABASE_URL" storage status
awa --database-url "$DATABASE_URL" storage prepare --engine queue_storage
awa --database-url "$DATABASE_URL" storage abort
```

Equivalent SQL for extracted-migration or direct-operator workflows:

```sql
SELECT * FROM awa.storage_status();

SELECT * FROM awa.storage_prepare(
    'queue_storage',
    '{}'::jsonb
);

SELECT * FROM awa.storage_abort();
```

These functions are one-shot operational commands. They are not schema-migration
DDL and should be run deliberately by an operator or rollout tool, not embedded
into the extracted migration SQL itself.

Reserved SQL identities for a later engine release that finalises the
transition:

- `awa.storage_enter_mixed_transition()`
- `awa.storage_finalize()`

On a canonical-only deployment these raise `55000` with a clear
"requires a later engine release" error, so operators cannot accidentally
force the state machine forward on a build that cannot drive it.

Canonical-only behaviour:

- `storage status` reports the singleton row in `awa.storage_transition_state`
- `storage status` also reports canonical live backlog, live runtime capability counts,
  and blocker lists for `enter-mixed-transition` / `finalize`
- `storage prepare` records a future engine and optional metadata, but keeps
  enqueue routing and worker execution on canonical storage
- `storage abort` clears a prepared engine selection; it is also the
  forward-compatible primitive for aborting a `mixed_transition` rollout
  before final activation on a release that supports it
- workers continue to run the canonical engine before and after `prepare`

### `0.5.x` -> `0.6` Queue-Storage Rollout

The first use of the transition framework is the `0.5.x` -> `0.6` move from
canonical storage to queue storage. The sequence has two phases: first bring
the whole fleet to a `0.5.x` release that has this framework (without
activating anything), then roll out `0.6` which completes the transition.

#### Phase 1: last `0.5.x` release everywhere

1. Deploy `0.5.latest` everywhere with a normal rolling deploy.
2. Run:

   ```bash
   awa --database-url "$DATABASE_URL" migrate
   ```

3. Verify the cluster is still canonical:

   ```bash
   awa --database-url "$DATABASE_URL" storage status
   ```

   Expected:

   - `current_engine = canonical`
   - `active_engine = canonical`
   - `prepared_engine = NULL`
   - `state = canonical`

4. Confirm live runtime capability is still canonical:

   ```sql
   SELECT instance_id, storage_capability, last_seen_at
   FROM awa.runtime_instances
   ORDER BY last_seen_at DESC;
   ```

5. This is a safe stopping point. The queue is still fully canonical and
   behavior is unchanged. Operators can remain here indefinitely.

#### Phase 2: `0.6` rollout on top of the prep release

1. Roll out `0.6` binaries. `0.5.x` and `0.6` pods may coexist at this stage.
   While state is still `canonical` or `prepared`, all execution and writes
   remain canonical.
2. Run:

   ```bash
   awa --database-url "$DATABASE_URL" storage prepare --engine queue_storage
   ```

3. Verify prepared state:

   - `current_engine = canonical`
   - `active_engine = canonical`
   - `prepared_engine = queue_storage`
   - `state = prepared`

4. Confirm every live runtime instance is now queue-storage capable before the
   routing flip. This is the gate that prevents canonical-only workers from
   surviving into `mixed_transition`.

5. Planned `0.6` behavior is then:

   - `awa storage enter-mixed-transition` flips new writes and cron enqueues to
     queue storage
   - queue-storage-capable workers begin reporting capability for the new
     engine, while canonical drain work is handled by `0.6` workers in
     drain-only mode
   - canonical backlog drains while new work lands in queue storage
   - `awa storage finalize` (or an assisted automatic equivalent) advances the
     state to `active` once live capability and backlog checks pass

Expected status progression during that rollout:

- before `prepare`: `canonical / canonical / NULL / canonical`
- after `prepare`: `canonical / canonical / queue_storage / prepared`
- during mixed transition: `canonical / queue_storage / queue_storage / mixed_transition`
- after finalization: `queue_storage / queue_storage / NULL / active`

Running `storage prepare` before every worker is on `0.5.latest` is harmless.
The prepared flag is dormant in `0.5.x`; it only becomes operational when `0.6`
code starts acting on it.

If rollout stalls because some workers never upgrade, use:

```sql
SELECT instance_id, storage_capability, last_seen_at
FROM awa.runtime_instances
ORDER BY last_seen_at DESC;
```

alongside `awa storage status` or the `/api/runtime` UI/API view to identify
which runtime instances are still reporting canonical capability. The cluster
must not enter `mixed_transition` until canonical-only workers have stopped
heartbeating. Finalization must also wait until canonical backlog is empty.

If you need to stop the rollout before final activation, `storage abort` is the
intended primitive. It returns the singleton row to `canonical` and clears the
prepared engine metadata. It does **not** make an already-created queue-storage
backlog runnable by `0.5` workers. If `0.6` has already routed jobs into queue
storage, keep `0.6` workers available until that backlog is drained.

Current `0.5.x` fail-safe behavior if someone forces the state forward without
`0.6` support:

- `awa.insert_job_compat()` raises `55000`
- batch insert and COPY insert paths also fail closed
- the reserved `awa.storage_enter_mixed_transition()` and
  `awa.storage_finalize()` functions raise “requires 0.6”

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

For the planned `0.5.x` -> `0.6` storage transition, there is an additional
constraint:

- once the cluster has entered `mixed_transition` and `0.6` has routed jobs to
  queue storage, a pure fleet downgrade back to `0.5` is not supported
- `0.5` workers do not know how to claim queue-storage work
- use `storage abort` only as a way to stop advancing the rollout and to return
  new writes to canonical while `0.6` workers remain available to drain any
  queue-storage backlog that already exists

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

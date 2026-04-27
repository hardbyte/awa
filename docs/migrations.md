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
storage-engine upgrades can build on.

The prep-release migration on `main` does **not** change the current execution
engine. On this rebased queue-storage branch, the later compatibility migration
builds on that foundation and also adds:

- the queue-storage compatibility layer
- the active-backend selector in `awa.runtime_storage_backends`
- the segmented DLQ table in the active queue-storage schema (`{schema}.dlq_entries`)
- `awa.jobs` / `awa.insert_job_compat()` routing that follows the active
  backend when queue storage is activated

New operator surfaces:

```bash
awa --database-url "$DATABASE_URL" storage status
awa --database-url "$DATABASE_URL" storage prepare --engine queue_storage
awa --database-url "$DATABASE_URL" storage prepare-queue-storage-schema --schema awa_exp
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

`storage prepare-queue-storage-schema` is also an operational command, but it
only materializes the queue-storage schema. It does **not** change
`awa.storage_transition_state` and it does **not** activate routing.

On the `0.5.x` prep release these SQL identities existed as stubs. On this
`0.6` branch they are implemented:

- `awa.storage_enter_mixed_transition()`
- `awa.storage_finalize()`

Current behavior:

- `storage status` reports the singleton row in `awa.storage_transition_state`
- `storage status` also reports canonical live backlog, live runtime capability counts,
  and blocker lists for `enter-mixed-transition` / `finalize`
- `storage prepare` records a future engine and optional metadata, but keeps
  enqueue routing and worker execution on canonical storage
- `storage prepare-queue-storage-schema` creates the target queue-storage
  schema ahead of time without changing routing
- `storage abort` returns routing to canonical and clears a prepared or mixed-transition
  rollout before final activation
- `storage enter-mixed-transition` requires prepared queue-storage metadata,
  a prepared queue-storage schema, and no live canonical-only runtimes
- `storage finalize` requires zero canonical live backlog and no live
  canonical / drain-only runtimes
- workers continue to run the canonical engine before and after `prepare`

This is intentional. The `0.5.x` prep release is only adding the reusable
tables, status APIs, capability metadata, and compat-routing seam needed for a
later engine migration. It is not activating queue storage.

### Preview: `0.5.x` -> `0.6` Queue-Storage Rollout

This release is laying the protocol surface for the planned `0.6` storage-engine
upgrade. The intended operator sequence has two phases.

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

   In `0.6`, the intended worker roles are:

   - the default `auto` role, which stays canonical before the routing flip
     and becomes `canonical_drain_only` once mixed transition starts
   - an explicit queue-storage target role, which prepares the queue-storage
     executor before the routing flip so new work can start executing
     immediately after `enter-mixed-transition`

   Current APIs:

   - Rust: `Client::builder(...).transition_role(TransitionWorkerRole::QueueStorageTarget)`
   - Python: `AsyncClient.start(..., storage_transition_role="queue_storage_target")`

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
intended primitive. In `prepared`, it returns the singleton row to `canonical`
and clears the prepared engine metadata. In `mixed_transition`, `0.6` now
enforces a rollback interlock: abort is only allowed while there are **no**
live queue-storage runtimes and **no** rows in queue-storage tables. Once
queue-storage has started accepting work, abort is rejected instead of trying
to paper over a partial rollback.

Current `0.5.x` fail-safe behavior if someone forces the state forward without
`0.6` support:

- `awa.insert_job_compat()` raises `55000`
- batch insert and COPY insert paths also fail closed
- the reserved `awa.storage_enter_mixed_transition()` and
  `awa.storage_finalize()` functions raise “requires 0.6”

#### `0.6` Receipt-plane partition migration (ADR-023)

The `0.6` queue-storage engine partitions the receipt plane:
`lease_claims` and `lease_claim_closures` are
`PARTITIONED BY LIST (claim_slot)` parents with one child per
claim-ring slot. Upgrading from a 0.5.x deploy where these were
regular (non-partitioned) tables runs an in-place migration inside
`prepare_schema()`:

1. The legacy tables are renamed in place to
   `lease_claims_legacy` / `lease_claim_closures_legacy`.
2. The new partitioned parents are created alongside the legacy
   tables.
3. Rows are copied from the legacy tables into the partitioned
   parents, all landing in the current `claim_ring_state.current_slot`
   (so existing receipts close out on their normal lifecycle in
   that partition; the ring's natural rotation re-balances over
   subsequent claims).
4. The legacy tables are dropped.

Steps 3 and 4 run inside a single Postgres transaction so a crash
mid-migration leaves the schema in exactly one of two states:
pre-migration (legacy tables still present, partitioned parents
empty) or post-migration (legacy tables gone, partitioned parents
populated). On restart, `prepare_schema()` detects whichever state
is current and either re-runs the migration or skips it. The
`ON CONFLICT DO NOTHING` on the copy step is a defensive
belt-and-braces; under the transactional shape it should never
fire.

If the migration is partially applied at the time of a `reset()`
call, the legacy tables are dropped explicitly before the main
`TRUNCATE`. Without that, `reset()` would TRUNCATE the partitioned
parents but leave the legacy data intact, and the next
`prepare_schema()` would re-run the migration on top and silently
re-insert old rows. Operators should not normally call `reset()`
during an upgrade — it's a test fixture rather than a recovery
primitive — but the safety net is there.

**Reverse migration** (only relevant if a production rollback fires
between an ADR-023 deploy and the older runtime catching up): create
unpartitioned `lease_claims` and `lease_claim_closures` tables,
`INSERT ... SELECT * FROM` each partitioned parent, drop the
partitioned parents. This is not committed to source — the forward
path is the only supported one — but operators are expected to keep
this recipe in their runbook for the rollout window.

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
- `storage abort` is only a rollback path from `mixed_transition` before any
  queue-storage runtime is live and before any queue-storage rows exist
- once queue-storage has accepted work, keep `0.6` workers available and treat
  the transition as one-way until finalize or full database restore

### 2. Database Rollback

Awa does not ship reverse SQL. If you need to undo the schema itself, use your normal database rollback controls:

- restore from backup or snapshot
- or own the reverse SQL in your external migration system

Do not expect `awa migrate` to downgrade the schema.

## Breaking Changes

If Awa ever needs a non-additive schema change, the intended contract is a major-version upgrade with a stop-the-world procedure documented explicitly for that release.

## Recommended Production Flow

The shape depends on whether you're doing a **fresh install** or
**upgrading from `0.5.x`**.

### Fresh install (no prior canonical data)

A new cluster can install queue storage directly without going through
the staged transition.

```bash
# 1. Apply schema
awa --database-url "$DATABASE_URL" migrate

# 2. Materialize the queue-storage schema before workers start.
#    Skipping this means the first worker pays the schema-install
#    cost on its first claim attempt.
awa --database-url "$DATABASE_URL" storage prepare-queue-storage-schema --schema awa_exp

# 3. Activate queue storage. Without prior canonical work, this can
#    move directly to active without a mixed-transition phase.
awa --database-url "$DATABASE_URL" storage prepare --engine queue_storage
awa --database-url "$DATABASE_URL" storage enter-mixed-transition
awa --database-url "$DATABASE_URL" storage finalize

# 4. Roll out workers (Rust or Python). Each runtime is queue-storage
#    capable by default; transition_role="auto" follows storage_status.

# 5. Verify routing + worker health
awa --database-url "$DATABASE_URL" storage status
awa --database-url "$DATABASE_URL" queue stats
```

### Upgrading from `0.5.x`

**Do not use the fresh-install recipe.** Live canonical data and
canonical-only `0.5.x` workers must drain through `mixed_transition`
before queue storage can take over. Follow the staged transition flow
documented above under
[Preview: `0.5.x` → `0.6` Queue-Storage Rollout](#preview-05x---06-queue-storage-rollout):

1. Phase 1 — last `0.5.x` release everywhere with `awa migrate` applied.
2. Phase 2 — roll out `0.6` binaries, then `prepare → enter-mixed-transition → finalize`.

Rollback boundaries and `storage abort` semantics are documented in
[Rollback Strategy](#rollback-strategy) below.

## Next

- [PostgreSQL roles and privileges](security.md)
- [Deployment guide](deployment.md)
- [Configuration](configuration.md)
- [Troubleshooting](troubleshooting.md)

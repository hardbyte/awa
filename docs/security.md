# PostgreSQL Roles and Privileges

AWA can run with a single database user, but production deployments should separate **schema management** from **runtime execution**. This guide documents the minimum-privilege role model.

## Role model

```
awa_owner       NOLOGIN   — owns all schema objects
  ├── awa_migrator  LOGIN — runs migrations (inherits awa_owner)
  └── awa_runtime   LOGIN — workers, producers, awa serve, CLI admin
```

**`awa_owner`** is a `NOLOGIN` group role that owns the `awa` schema and all objects in it. No process connects as `awa_owner` directly.

**`awa_migrator`** is a `LOGIN` role that is a member of `awa_owner`. It runs `awa migrate` and can create/alter/drop schema objects. Use this role only for migrations — not for workers or the UI.

**`awa_runtime`** is a `LOGIN` role with the minimum privileges needed to run workers, enqueue jobs, and serve the admin UI. It cannot modify the schema.

## Setting up roles

### 1. Create roles

```sql
-- Run as a superuser or database owner
CREATE ROLE awa_owner NOLOGIN;
CREATE ROLE awa_migrator LOGIN PASSWORD 'strong-password-here';
CREATE ROLE awa_runtime LOGIN PASSWORD 'strong-password-here';

-- awa_migrator inherits awa_owner (can create/alter schema objects)
GRANT awa_owner TO awa_migrator;

-- Both roles need to connect
GRANT CONNECT ON DATABASE mydb TO awa_migrator;
GRANT CONNECT ON DATABASE mydb TO awa_runtime;

-- awa_owner needs CREATE to make the schema
GRANT CREATE ON DATABASE mydb TO awa_owner;
```

### 2. Run migrations

```bash
awa --database-url "postgres://awa_migrator:pass@host/mydb" migrate
```

The migrator creates the `awa` schema and all objects. Objects are owned by `awa_migrator` (who inherits `awa_owner`).

### 3. Transfer ownership (recommended)

After the initial migration, transfer object ownership to `awa_owner` so it's decoupled from the login role:

```sql
ALTER SCHEMA awa OWNER TO awa_owner;

-- Transfer tables, partitioned tables, views, materialized views, and sequences.
DO $$
DECLARE r RECORD;
BEGIN
  FOR r IN
    SELECT c.relkind, c.oid::regclass AS obj
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'awa'
      AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
  LOOP
    IF r.relkind = 'S' THEN
      EXECUTE format('ALTER SEQUENCE %s OWNER TO awa_owner', r.obj);
    ELSE
      EXECUTE format('ALTER TABLE %s OWNER TO awa_owner', r.obj);
    END IF;
  END LOOP;
END$$;

-- Transfer functions.
DO $$
DECLARE r RECORD;
BEGIN
  FOR r IN SELECT p.oid::regprocedure AS func
           FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
           WHERE n.nspname = 'awa' LOOP
    EXECUTE format('ALTER FUNCTION %s OWNER TO awa_owner', r.func);
  END LOOP;
END$$;

-- Transfer standalone enum/domain types. Table row types and generated array
-- types are owned through their base objects and should not be altered here.
DO $$
DECLARE r RECORD;
BEGIN
  FOR r IN
    SELECT format('%I.%I', n.nspname, t.typname) AS typ
    FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE n.nspname = 'awa'
      AND t.typtype IN ('d', 'e')
  LOOP
    EXECUTE format('ALTER TYPE %s OWNER TO awa_owner', r.typ);
  END LOOP;
END$$;
```

### 4. Grant runtime privileges

```sql
-- Schema access
GRANT USAGE ON SCHEMA awa TO awa_runtime;

-- Sequences: canonical `jobs_id_seq`, and queue-storage `job_id_seq`
-- once prepare_schema has materialized it.
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA awa TO awa_runtime;

-- All tables: the runtime needs full DML because triggers run as the
-- invoking role (SECURITY INVOKER), so inserting a job also writes to
-- the admin metadata cache tables via triggers. The maintenance leader
-- also calls refresh_admin_metadata(), which truncates dirty-key tables
-- after taking the metadata advisory lock.
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA awa TO awa_runtime;

-- Functions (trigger functions execute with invoker privileges)
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA awa TO awa_runtime;
-- Keep the queue-storage installer helper migrator/operator-only.
REVOKE EXECUTE ON FUNCTION awa.install_queue_storage_substrate(TEXT, INT, INT, INT, BOOLEAN)
  FROM awa_runtime;

-- Default privileges for future migrations.
ALTER DEFAULT PRIVILEGES FOR ROLE awa_owner IN SCHEMA awa
  GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO awa_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE awa_owner IN SCHEMA awa
  GRANT USAGE, SELECT ON SEQUENCES TO awa_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE awa_owner IN SCHEMA awa
  GRANT EXECUTE ON FUNCTIONS TO awa_runtime;

-- If migrations run as awa_migrator without `SET ROLE awa_owner`, future
-- objects are owned by awa_migrator, so set defaults for that role too.
ALTER DEFAULT PRIVILEGES FOR ROLE awa_migrator IN SCHEMA awa
  GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO awa_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE awa_migrator IN SCHEMA awa
  GRANT USAGE, SELECT ON SEQUENCES TO awa_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE awa_migrator IN SCHEMA awa
  GRANT EXECUTE ON FUNCTIONS TO awa_runtime;
```

If you use the compatibility `insert_many_copy` path (Rust
`InsertOpts::copy()`), also grant:

```sql
GRANT TEMP ON DATABASE mydb TO awa_runtime;
```

This allows creating temporary staging tables for the `COPY` bulk insert path.
Queue-storage direct COPY (`QueueStorage::enqueue_params_copy` in Rust,
`enqueue_many_copy` in Python) writes to the queue-storage tables directly and
does not need this temporary-table grant.

### Custom queue-storage schema

The queue-storage backend defaults to keeping its tables in the same
`awa` schema, so the grants above cover both control-plane and
queue-storage tables. If you override the schema name (Rust:
`QueueStorageConfig.schema`; Python: `queue_storage_schema=...`; CLI:
`awa storage prepare-queue-storage-schema --schema <name>`), repeat
the grant block against that schema:

```sql
GRANT USAGE ON SCHEMA my_qs_schema TO awa_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA my_qs_schema TO awa_runtime;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA my_qs_schema TO awa_runtime;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA my_qs_schema TO awa_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE awa_owner IN SCHEMA my_qs_schema
  GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO awa_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE awa_owner IN SCHEMA my_qs_schema
  GRANT USAGE, SELECT ON SEQUENCES TO awa_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE awa_owner IN SCHEMA my_qs_schema
  GRANT EXECUTE ON FUNCTIONS TO awa_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE awa_migrator IN SCHEMA my_qs_schema
  GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO awa_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE awa_migrator IN SCHEMA my_qs_schema
  GRANT USAGE, SELECT ON SEQUENCES TO awa_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE awa_migrator IN SCHEMA my_qs_schema
  GRANT EXECUTE ON FUNCTIONS TO awa_runtime;
```

The default `awa` queue-storage substrate is migrated by `awa migrate`. Custom
queue-storage schemas are migrated by
`awa storage prepare-queue-storage-schema`, which should also run as the
migrator role and creates objects owned by `awa_migrator` / `awa_owner`. The
runtime role needs read/write/execute privileges plus `TRUNCATE` for
ring-partition rotation; it never needs DDL.

### 5. Configure your processes

| Process | Role | Connection string |
|---|---|---|
| `awa migrate` | `awa_migrator` | `postgres://awa_migrator:pass@host/db` |
| Workers (Rust/Python) | `awa_runtime` | `postgres://awa_runtime:pass@host/db` |
| `awa serve` (UI + API) | `awa_runtime` | `postgres://awa_runtime:pass@host/db` |
| `awa job`, `awa queue`, etc | `awa_runtime` | `postgres://awa_runtime:pass@host/db` |

## What the runtime role needs and why

The runtime grants look broad (`SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES`) because AWA's internal tables are maintained by `SECURITY INVOKER` trigger functions. When a worker inserts a job, triggers automatically update:

- `awa.queue_state_counts` — per-queue job state tallies
- `awa.job_kind_catalog` — distinct job kinds
- `awa.job_queue_catalog` — distinct queues
- `awa.job_unique_claims` — unique key deduplication

Since these triggers run with the caller's privileges, the runtime role needs write access to these internal tables even though application code never touches them directly.

The maintenance leader also calls `awa.refresh_admin_metadata()` as a full reconciliation safety net. That function runs with invoker privileges and uses `TRUNCATE` on `awa.admin_dirty_queues` and `awa.admin_dirty_kinds` after taking the metadata advisory lock, so `awa_runtime` needs the `TRUNCATE` table privilege too.

The runtime also directly upserts into the descriptor catalogs on startup and on each runtime snapshot tick:

- `awa.queue_descriptors` — declared queue display names, ownership, tags
- `awa.job_kind_descriptors` — declared job-kind display names, ownership, tags
- `awa.runtime_instances` — the runtime's own liveness row, including per-queue and per-kind descriptor hashes used for drift detection

These writes are not trigger-driven; they come from `ClientBuilder::build()` / `AsyncClient.start()` and from the snapshot reporter. The broad grant already covers them.

Other PostgreSQL features used at runtime:

| Feature | Purpose | Privilege needed |
|---|---|---|
| `LISTEN` / `NOTIFY` | Queue wakeup without polling | `CONNECT` (no extra grant) |
| `pg_try_advisory_lock` | Leader election for maintenance | Built-in function (no grant) |
| `COPY ... FROM STDIN` | Bulk insert path | `TEMP` on database |
| `FOR UPDATE SKIP LOCKED` | Non-blocking job claiming | `SELECT`, `UPDATE` on table |

## Managing roles with pgroles

For teams that manage PostgreSQL access declaratively, [pgroles](https://github.com/hardbyte/pgroles) can maintain the role model as a YAML manifest:

```yaml
profiles:
  runtime:
    grants:
      - on: { type: schema }
        privileges: [USAGE]
      - on: { type: table, name: "*" }
        privileges: [SELECT, INSERT, UPDATE, DELETE, TRUNCATE]
      - on: { type: sequence, name: "*" }
        privileges: [USAGE, SELECT]
      - on: { type: function, name: "*" }
        privileges: [EXECUTE]
    default_privileges:
      - on_type: table
        privileges: [SELECT, INSERT, UPDATE, DELETE, TRUNCATE]
      - on_type: sequence
        privileges: [USAGE, SELECT]
      - on_type: function
        privileges: [EXECUTE]

schemas:
  - name: awa
    profiles: [runtime]

roles:
  - name: awa_owner
    login: false
  - name: awa_migrator
    login: true
    password:
      from_env: AWA_MIGRATOR_PASSWORD
  - name: awa_runtime
    login: true
    password:
      from_env: AWA_RUNTIME_PASSWORD

memberships:
  - role: awa_owner
    members:
      - name: awa_migrator
```

`pgroles diff` shows planned changes, `pgroles apply` converges. You can also run `pgroles generate` against an existing AWA database to produce an initial manifest.

## Migrating from a single-user setup

If you're currently running everything as one superuser or app role:

1. Create `awa_owner`, `awa_migrator`, `awa_runtime` as above
2. Transfer ownership to `awa_owner`
3. Grant runtime privileges
4. Update your migration tooling to use `awa_migrator`
5. Update worker/serve connection strings to use `awa_runtime`
6. Verify with `awa --database-url postgres://awa_runtime:... job list`

This is additive — no schema changes, no downtime.

## Future: tighter runtime grants

The current model requires broad table grants because compatibility triggers
and helper functions still use `SECURITY INVOKER`. A future migration could
convert the internal queue-storage helpers to `SECURITY DEFINER` (running as
`awa_owner`), which would let the runtime role be restricted to the active
queue-storage schema plus the shared control tables (`queue_meta`,
`job_unique_claims`, `cron_jobs`, `runtime_instances`, and
`runtime_storage_backends`). This is tracked but not yet implemented — the
current model is secure for the separation it provides (runtime can't modify
schema).

## Deployable roles

Awa is one process binary, but for production it splits into several
*deployable roles*. Each role has a different exposure profile, and
mixing them onto the same listener is the most common source of
operational risk. See [ADR-027](adr/027-callback-ingress-surface.md) for
the design rationale and [`docs/http-callbacks.md`](http-callbacks.md)
for the per-role deployment shape.

| Role | Purpose | Exposure | Mutates job state? |
|---|---|---|---|
| **Admin UI / API** (`awa serve`) | operator inspection and mutation — jobs, queues, runtime, DLQ, stats | private operator network | yes |
| **Callback receiver** (`awa callbacks serve` or user-owned router) | `complete` / `fail` / `heartbeat` for `HttpWorker` async mode and external systems | public or partner-facing, signed | yes |
| **Workers / dispatchers** (`awa::Client` with registered workers) | claim jobs, execute handlers, dispatch via `HttpWorker` | internal | yes |
| **Maintenance runtime** (background tasks on any worker process; future: dedicated role per ADR-028) | promotion, rescue, pruning, metadata refresh | internal | yes |
| **Database** | storage and coordination | private | yes |

A single development setup can collapse these onto one process: `awa serve` runs admin + callback receiver, an embedded `awa::Client` runs workers, and Postgres is reachable on localhost. Production deployments **should** split at least the admin UI from the callback receiver — the admin surface stays on the operator network, the receiver lives wherever it needs to be reachable from the function (often public).

### Common deployment shapes

- **All-in-one dev:** `awa serve` + an embedded `awa::Client`. Admin UI and callback receiver share the same listener with permissive CORS. Fine for local development; never run this on a public listener.
- **Private admin + public callback receiver:** `awa serve` on a private VPC subnet, `awa callbacks serve --callback-hmac-secret …` on an external load balancer. The receiver router omits static UI assets, the admin REST routes, and permissive CORS.
- **User-owned callback API:** mount the three callback ingress routes inside your existing FastAPI / axum / Flask app, using `awa_model::callback_contract::verify` (Rust) or `awa.callback_contract.verify` (Python) so the signature contract cannot drift. See [`docs/callback-receivers.md`](callback-receivers.md).
- **Receiver + maintenance-only runtime:** the receiver handles ingress while a dedicated runtime instance runs promotion / rescue / pruning. Workers stay on their own deployment. Maintenance-only runtime is tracked in [ADR-028](adr/028-maintenance-only-runtime-role.md).
- **HTTP-worker deployments:** the worker process still runs an `awa::Client` — it claims jobs, calls the function, and registers the callback. The receiver is a separate listener. A function endpoint without a corresponding dispatcher process will *never* see jobs.

## Admin Surface

`awa serve` is an **operator surface**: it bundles the admin REST API, the React dashboard, the static fallback, permissive CORS, and (today) the callback receiver routes behind a single router. Treat it like a database admin console:

- Put it behind your normal authentication and authorization layer.
- Restrict network access with ingress policy, firewall rules, or private networking.
- Prefer binding to localhost or an internal service address unless you explicitly need external access.

When callbacks must be externally reachable but the admin surface must stay private, run them on separate listeners using `awa callbacks serve` or a user-owned receiver — the admin endpoints simply do not exist on those routers.

## Callback Endpoints

The callback receiver exposes three **mutating ingress** endpoints, regardless of which deployable role hosts them:

- `POST {prefix}/:callback_id/complete`
- `POST {prefix}/:callback_id/fail`
- `POST {prefix}/:callback_id/heartbeat`

`{prefix}` defaults to `/api/callbacks`, matching the historical `awa serve` shape so existing deployments keep working unchanged. It is configurable on both sides:

- Worker side: `HttpWorkerConfig::callback_path_prefix`
- `awa callbacks serve` side: `--path-prefix` / `AWA_CALLBACK_PATH_PREFIX`
- Custom-receiver side: mount your routes at whatever prefix you want and pass that prefix back to the worker config

These endpoints mutate job state and must not be exposed without protection. The full HTTP worker flow, callback payloads, and signature contract are documented in [HTTP workers and callback signatures](http-callbacks.md).

## Callback Signature Verification

Awa supports per-callback request authentication with a 32-byte BLAKE3 keyed hash.

- Configure the callback receiver with `--callback-hmac-secret <64-hex-chars>` or `AWA_CALLBACK_HMAC_SECRET`.
- Configure `HttpWorkerConfig.hmac_secret` with the same 32-byte key.
- The worker signs the callback ID and sends the signature as `X-Awa-Signature`.
- The function normally forwards that same header when it calls Awa back.
- The callback receiver verifies that header before accepting `complete`, `fail`, or `heartbeat`.

Example:

```bash
export AWA_CALLBACK_HMAC_SECRET=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
awa --database-url "$DATABASE_URL" serve --host 0.0.0.0 --port 3000
```

If no callback secret is configured, signature verification is disabled. That is acceptable only for trusted internal deployments where the callback receiver is already protected by network boundaries or an authenticating proxy.

The option name says `hmac` for operational familiarity, but the implementation
uses BLAKE3 keyed hashing over the callback ID string, not RFC HMAC.

## Custom Callback Receivers

When you host the callback ingress routes inside your own application (FastAPI, axum, Flask, etc.), reuse the shared helpers in `awa::callback_contract` (Rust) or `awa.callback_contract` (Python) rather than re-implementing the signature algorithm. The Python wrappers are thin PyO3 bindings around the same Rust functions, with a pinned BLAKE3 test vector asserted from both sides so the bindings cannot drift.

See [callback receivers](callback-receivers.md) for end-to-end custom-axum and FastAPI examples.

## Operational Guidance

- Rotate callback secrets like any other shared secret.
- Use different secrets per environment.
- Prefer HTTPS/TLS termination in front of any externally reachable callback receiver.
- Avoid logging callback signatures or other shared-secret material.

## Next

- [Deployment guide](deployment.md)
- [Configuration](configuration.md)
- [Troubleshooting](troubleshooting.md)

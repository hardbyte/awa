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

-- Transfer all tables, views, sequences, functions
DO $$
DECLARE r RECORD;
BEGIN
  FOR r IN SELECT tablename FROM pg_tables WHERE schemaname = 'awa' LOOP
    EXECUTE format('ALTER TABLE awa.%I OWNER TO awa_owner', r.tablename);
  END LOOP;
END$$;

DO $$
DECLARE r RECORD;
BEGIN
  FOR r IN SELECT p.oid::regprocedure AS func
           FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
           WHERE n.nspname = 'awa' LOOP
    EXECUTE format('ALTER FUNCTION %s OWNER TO awa_owner', r.func);
  END LOOP;
END$$;

ALTER VIEW awa.jobs OWNER TO awa_owner;
ALTER SEQUENCE awa.jobs_id_seq OWNER TO awa_owner;
ALTER TYPE awa.job_state OWNER TO awa_owner;
```

### 4. Grant runtime privileges

```sql
-- Schema access
GRANT USAGE ON SCHEMA awa TO awa_runtime;

-- Sequence (job ID generation)
GRANT USAGE, SELECT ON SEQUENCE awa.jobs_id_seq TO awa_runtime;

-- All tables: the runtime needs full DML because triggers run as the
-- invoking role (SECURITY INVOKER), so inserting a job also writes to
-- the admin metadata cache tables via triggers.
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA awa TO awa_runtime;

-- Functions (trigger functions execute with invoker privileges)
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA awa TO awa_runtime;

-- Default privileges for future migrations
ALTER DEFAULT PRIVILEGES FOR ROLE awa_owner IN SCHEMA awa
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO awa_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE awa_owner IN SCHEMA awa
  GRANT USAGE, SELECT ON SEQUENCES TO awa_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE awa_owner IN SCHEMA awa
  GRANT EXECUTE ON FUNCTIONS TO awa_runtime;
```

If you use the high-throughput `insert_many_copy` path (Rust `InsertOpts::copy()`), also grant:

```sql
GRANT TEMP ON DATABASE mydb TO awa_runtime;
```

This allows creating temporary staging tables for the `COPY` bulk insert path.

### 5. Configure your processes

| Process | Role | Connection string |
|---|---|---|
| `awa migrate` | `awa_migrator` | `postgres://awa_migrator:pass@host/db` |
| Workers (Rust/Python) | `awa_runtime` | `postgres://awa_runtime:pass@host/db` |
| `awa serve` (UI + API) | `awa_runtime` | `postgres://awa_runtime:pass@host/db` |
| `awa job`, `awa queue`, etc | `awa_runtime` | `postgres://awa_runtime:pass@host/db` |

## What the runtime role needs and why

The runtime grants look broad (`SELECT, INSERT, UPDATE, DELETE ON ALL TABLES`) because AWA's internal tables are maintained by `SECURITY INVOKER` trigger functions. When a worker inserts a job, triggers automatically update:

- `awa.queue_state_counts` — per-queue job state tallies
- `awa.job_kind_catalog` — distinct job kinds
- `awa.job_queue_catalog` — distinct queues
- `awa.job_unique_claims` — unique key deduplication

Since these triggers run with the caller's privileges, the runtime role needs write access to these internal tables even though application code never touches them directly.

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
        privileges: [SELECT, INSERT, UPDATE, DELETE]
      - on: { type: sequence, name: "*" }
        privileges: [USAGE, SELECT]
      - on: { type: function, name: "*" }
        privileges: [EXECUTE]
    default_privileges:
      - on_type: table
        privileges: [SELECT, INSERT, UPDATE, DELETE]
      - on_type: sequence
        privileges: [USAGE, SELECT]
      - on_type: function
        privileges: [EXECUTE]

schemas:
  - name: awa
    profiles: [runtime]

roles:
  - name: awa_runtime
    login: true
    password:
      from_env: AWA_RUNTIME_PASSWORD

memberships:
  - role: awa-runtime
    members:
      - name: awa_runtime
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

The current model requires broad table grants because triggers use `SECURITY INVOKER`. A future migration could convert internal trigger functions to `SECURITY DEFINER` (running as `awa_owner`), which would let the runtime role be restricted to just the core tables (`jobs_hot`, `scheduled_jobs`, `queue_meta`, `cron_jobs`, `runtime_instances`). This is tracked but not yet implemented — the current model is secure for the separation it provides (runtime can't modify schema).

## Admin Surface

`awa serve` exposes a read/write admin API and UI. Treat it as an operator surface, not a public endpoint.

- Put it behind your normal authentication and authorization layer.
- Restrict network access with ingress policy, firewall rules, or private networking.
- Prefer binding to localhost or an internal service address unless you explicitly need external access.

## Callback Endpoints

When using `HttpWorker` async mode, `awa-ui` exposes these callback receiver endpoints:

- `POST /api/callbacks/:callback_id/complete`
- `POST /api/callbacks/:callback_id/fail`
- `POST /api/callbacks/:callback_id/heartbeat`

These endpoints mutate job state and should not be exposed without protection.

## Callback Signature Verification

Awa supports per-callback request authentication with a 32-byte BLAKE3 keyed hash.

- Configure the callback receiver with `--callback-hmac-secret <64-hex-chars>` or `AWA_CALLBACK_HMAC_SECRET`.
- Configure `HttpWorkerConfig.hmac_secret` with the same 32-byte key.
- The worker signs the callback ID and sends the signature as `X-Awa-Signature`.
- The callback receiver verifies that header before accepting `complete`, `fail`, or `heartbeat`.

Example:

```bash
export AWA_CALLBACK_HMAC_SECRET=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
awa --database-url "$DATABASE_URL" serve --host 0.0.0.0 --port 3000
```

If no callback secret is configured, signature verification is disabled. That is acceptable only for trusted internal deployments where the callback receiver is already protected by network boundaries or an authenticating proxy.

## Custom Callback Receivers

If you do not use `awa-ui` and instead mount your own callback handlers around `admin::complete_external`, `admin::fail_external`, or `admin::heartbeat_callback`, you must provide equivalent request authentication yourself.

## Operational Guidance

- Rotate callback secrets like any other shared secret.
- Use different secrets per environment.
- Prefer HTTPS/TLS termination in front of any externally reachable callback receiver.
- Avoid logging callback signatures or other shared-secret material.

## Next

- [Deployment guide](deployment.md)
- [Configuration](configuration.md)
- [Troubleshooting](troubleshooting.md)

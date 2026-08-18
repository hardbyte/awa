> For the complete AWA documentation index, see [`llms.txt`](../../llms.txt).

# Database roles and privileges

AWA can run with one database user, but production deployments should separate schema management from runtime execution.

## Role model

```text
awa_owner       NOLOGIN   owns the schema and its objects
└── awa_migrator  LOGIN   runs migrations; member of awa_owner

awa_runtime     LOGIN     workers, producers, admin UI and CLI operations
```

Create the roles as a database owner or superuser:

```sql
CREATE ROLE awa_owner NOLOGIN;
CREATE ROLE awa_migrator LOGIN PASSWORD 'replace-me';
CREATE ROLE awa_runtime LOGIN PASSWORD 'replace-me';

GRANT awa_owner TO awa_migrator;
GRANT CONNECT ON DATABASE mydb TO awa_migrator, awa_runtime;
GRANT CREATE ON DATABASE mydb TO awa_owner;
```

Run `awa migrate` as `awa_migrator`. For long-lived installations, make `awa_owner` the owner of the `awa` schema and its tables, sequences, functions, and standalone enum/domain types. This decouples ownership from a login credential.

## Runtime grants

The 0.6 runtime uses `SECURITY INVOKER` triggers and maintenance helpers, so its grants are intentionally broader than an application's enqueue-only privileges:

```sql
GRANT USAGE ON SCHEMA awa TO awa_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
  ON ALL TABLES IN SCHEMA awa TO awa_runtime;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA awa TO awa_runtime;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA awa TO awa_runtime;

REVOKE EXECUTE ON FUNCTION
  awa.install_queue_storage_substrate(TEXT, INT, INT, INT, BOOLEAN)
  FROM awa_runtime;
```

The runtime needs `TRUNCATE` for guarded ring-partition reclamation. Compatibility COPY through `InsertOpts::copy()` also needs `TEMP` on the database; direct queue-storage COPY does not use that temporary staging table.

Set matching default privileges for every role that creates objects during migrations:

```sql
ALTER DEFAULT PRIVILEGES FOR ROLE awa_owner IN SCHEMA awa
  GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO awa_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE awa_owner IN SCHEMA awa
  GRANT USAGE, SELECT ON SEQUENCES TO awa_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE awa_owner IN SCHEMA awa
  GRANT EXECUTE ON FUNCTIONS TO awa_runtime;
```

If migrations create objects as `awa_migrator` without first running `SET ROLE awa_owner`, repeat those three statements with `FOR ROLE awa_migrator`.

## Custom queue-storage schemas

Repeat the schema, table, sequence, function, and default-privilege grants for a custom queue-storage schema. Prepare custom schemas as the migrator; the runtime needs DML, sequence access, function execution, and `TRUNCATE`, but never DDL. See [Queue storage](../../queue-storage-substrate/index.md).

## Why the grants are broad

Runtime triggers maintain queue counts, descriptors, uniqueness claims, and other metadata as the invoking role. The elected maintenance task promotes and rescues jobs, refreshes metadata, and reclaims eligible ring slots. Consequently a login holding `awa_runtime` is trusted Awa infrastructure: do not grant it to a producer-only application or a public callback service.

[ADR-042](../../adr/042-caller-owned-finalization-transactions/index.md) and [ADR-043](../../adr/043-postgresql-capability-functions/index.md) describe accepted/proposed boundaries for a narrower application finalizer and capability-specific runtime roles planned for 0.7. They are not the current 0.6 privilege model.

## Verify the split

- Connect as the migrator to run `awa migrate`.
- Connect as the runtime to start workers and run ordinary `awa job` / `awa queue` commands.
- Confirm the runtime cannot create or alter schema objects.
- Keep the migrator credential out of worker and admin-service configuration.

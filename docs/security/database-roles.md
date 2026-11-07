# Database roles and privileges

AWA can run with one database user, but production deployments should separate schema management from runtime execution.

## Role model

```text
awa_owner       NOLOGIN   owns the schema and its objects
└── awa_migrator  LOGIN   runs migrations; member of awa_owner

awa_runtime     LOGIN     workers, producers, admin UI and CLI operations
```

Create the roles as a superuser or a role with the cluster-wide `CREATEROLE` attribute. Database ownership alone cannot create roles:

```sql
CREATE ROLE awa_owner NOLOGIN;
CREATE ROLE awa_migrator LOGIN PASSWORD 'replace-me';
CREATE ROLE awa_runtime LOGIN PASSWORD 'replace-me';

GRANT awa_owner TO awa_migrator;
GRANT CONNECT ON DATABASE mydb TO awa_migrator, awa_runtime;
GRANT CREATE ON DATABASE mydb TO awa_owner;
```

Run migrations through the migrator login while making `awa_owner` the effective role for every migration connection:

```bash
PGOPTIONS='-c role=awa_owner' \
  awa --database-url "$AWA_MIGRATOR_DATABASE_URL" migrate
```

This makes the `awa` schema and the tables, sequences, functions, and standalone enum/domain types created by migrations belong to the non-login owner from the outset. It also avoids relying on membership or default privileges to repair ownership later. The `awa_migrator` login must be allowed to `SET ROLE awa_owner`; on PostgreSQL 16 and newer, preserve that option when granting the membership.

## Runtime grants

The 0.6 runtime and current 0.7 development runtime use `SECURITY INVOKER` triggers and maintenance helpers, so their grants are intentionally broader than an application's enqueue-only privileges:

```sql
GRANT USAGE ON SCHEMA awa TO awa_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
  ON ALL TABLES IN SCHEMA awa TO awa_runtime;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA awa TO awa_runtime;
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
  GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO awa_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE awa_owner IN SCHEMA awa
  GRANT EXECUTE ON FUNCTIONS TO awa_runtime;
```

If an existing installation created objects as `awa_migrator` without first setting the owner role, transfer every existing schema object to `awa_owner` before relying on owner-scoped defaults. New migrations should use the effective-role command above.

## Lane sequences

Each queue lane, `(queue, priority, enqueue_shard)`, has an enqueue and a claim sequence. From schema v053 they are created by the schema owner, whichever role first enqueues into, claims from or promotes into the lane: the lane helpers call `provision_lane` and `provision_lane_sequences` in the queue-storage schema. These are `SECURITY DEFINER` functions owned by the schema owner with a pinned `search_path`. Like the other runtime-callable functions they are executable by `PUBLIC`, so schema `USAGE` is what gates them, and they only create lanes, after checking that the queue, priority and shard are set and the shard is 0–63. A runtime therefore needs neither schema `CREATE` nor default privileges to start using a new queue, priority or shard.

A new lane sequence gets the privileges of the head table it backs. A role that may `INSERT` or `UPDATE` `queue_enqueue_heads` gets `USAGE, SELECT, UPDATE` on the enqueue sequences, and one that may only `SELECT` it gets `SELECT`; `queue_claim_heads` does the same for the claim sequences. Granting the runtime the table privileges above is enough, and no sequence default privileges are needed for lanes.

Run migrations with the schema owner as the effective role, as above, so the provisioning functions belong to it. A migrator that cannot `SET ROLE` to the owner leaves them owned by itself and the migration warns.

`awa storage prepare-queue` still creates every priority and shard of a queue in one transaction, and any runtime role may run it. Use it to create lanes before a deploy rather than on first use.

### Check and repair lane ownership

Lanes created before v053 belong to whichever role touched them first, and only that role, or the roles its default privileges named, can use them. A different runtime role then fails with `permission denied for sequence` on that lane. List lane sequences that are not owned by the schema owner or lack a runtime grant (read-only, any role):

```bash
awa --database-url "$AWA_DATABASE_URL" storage lane-sequences --schema awa
```

```sql
SELECT * FROM awa.lane_sequence_problems('awa');
```

The command exits 2 while any remain.

`awa migrate` repairs them once the migration commits, one sequence per short transaction: it gives each sequence to the schema owner and adds its missing grants, wherever it is allowed to. Reassigning takes the sequence's `ACCESS EXCLUSIVE` lock, so the repair waits at most 300 ms for it. A sequence that an open enqueue or claim holds for longer is skipped, left unchanged and reported as busy. A sequence the repair is not allowed to change is reported with the roles that can finish it. Neither fails the migration. Run the repair again at any time; it is idempotent:

```bash
awa --database-url "$AWA_MIGRATOR_DATABASE_URL" storage lane-sequences --repair
```

Which role can repair a sequence:

- Reassigning it needs a role that holds the current owner's privileges (a member of the owner, with inheritance) and can `SET ROLE` to the schema owner.
- Adding its grants needs the current owner's privileges.

The schema owner holds neither the migration role's nor an application role's privileges, so a repair running as `awa_owner` cannot reassign sequences those roles created. That includes the migration role connected with `PGOPTIONS='-c role=awa_owner'`. Run the repair as the migration role itself, without `PGOPTIONS`, as above. `awa migrate` and `--repair` retry each sequence the effective role could not repair as the login role, so `awa migrate` run with `PGOPTIONS='-c role=awa_owner'`, as documented, still repairs the migration role's sequences.

For a migrator with SET-only membership in `awa_owner`, grant schema access and helper execution explicitly before running the repair. Run as the schema owner:

```sql
GRANT USAGE ON SCHEMA awa TO awa_migrator;
GRANT EXECUTE ON FUNCTION
  awa.lane_sequence_problems(TEXT, TEXT),
  awa.repair_lane_sequence(TEXT, TEXT)
  TO awa_migrator;
```

For a custom queue-storage schema, also grant `USAGE` on that schema. The migrator must still hold the old sequence owner's privileges and be allowed to `SET ROLE` to the schema owner; inherited privileges from the schema owner are not required.

For each sequence that remains, the output lists `repair_roles`, the login roles other than superusers that can repair it, and the statements to run. A busy sequence only needs another `--repair`. When `repair_roles` is empty, which is usual for a sequence an application role created on managed PostgreSQL, either:

- run `missing_grants` as the current owner. Every runtime role can then use the lane, although the owner stays unchanged; or
- give the migration role the owner's privileges, for example `GRANT app_role TO awa_migrator` from a role with `ADMIN` on `app_role`, rerun `--repair` as the migration role, then revoke the membership.

Without the CLI, repair one sequence per transaction from `psql` in autocommit mode, connected as the migration role:

```sql
SELECT format('SELECT * FROM awa.repair_lane_sequence(%L, %L)', 'awa', sequence_name)
FROM awa.lane_sequence_problems('awa') \gexec
```

## Custom queue-storage schemas

Repeat the schema, table, sequence, function, and default-privilege grants for a custom queue-storage schema. Prepare custom schemas as the migrator; the runtime needs DML, sequence access, function execution, and `TRUNCATE`, but never DDL. Check lanes in a custom schema with `awa storage lane-sequences --schema <name>`. See [Queue storage](../queue-storage-substrate.md).

## Why the grants are broad

Runtime triggers maintain queue counts, descriptors, uniqueness claims, and other metadata as the invoking role. The elected maintenance task promotes and rescues jobs, refreshes metadata, and reclaims eligible ring slots. Consequently a login holding `awa_runtime` is trusted Awa infrastructure: do not grant it to a producer-only application or a public callback service.

[ADR-042](../adr/042-caller-owned-finalization-transactions.md) and [ADR-043](../adr/043-postgresql-capability-functions.md) describe accepted/proposed boundaries for a narrower application finalizer and capability-specific runtime roles. They are not implemented by the current 0.6 or 0.7 development runtime.

## Verify the split

- Connect as the migrator to run `awa migrate`.
- Connect as the runtime to start workers and run ordinary `awa job` / `awa queue` commands.
- Confirm the runtime cannot create or alter schema objects.
- Keep the migrator credential out of worker and admin-service configuration.

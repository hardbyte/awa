# Upgrading from 0.6.9 to 0.6.10

0.6.10 is a rolling patch upgrade. Traffic keeps running throughout, and you can roll binaries back to 0.6.9 at any point.

## Upgrade

1. Install the 0.6.10 CLI (`pip install awa-cli==0.6.10`, `cargo install awa-cli --version 0.6.10`, or the release binary or image).
2. Run `awa migrate` once, as the role that owns the Awa schemas, while workers keep running:

   ```bash
   awa --database-url "$AWA_MIGRATOR_DATABASE_URL" migrate
   ```

3. Roll admin processes (`awa serve`) and workers to 0.6.10, in any order and at your usual pace.
4. Check that queues keep draining and that applications log no new errors.

A 0.6.9 database prints:

```text
Schema: v40 (unchanged)
Schema patch wait_free_dirty_marks: already present
Schema patch prepared_lane_sequences: applied
  updated: awa.install_queue_storage_substrate, awa.refresh_lane_sequence_helpers, lane helpers in schema awa, lane helpers in schema <your queue-storage schema>
Storage routing: unchanged (state active, engine queue_storage, schema <your queue-storage schema>, epoch N)
Migrations applied successfully.
```

A second run reports `already present` and changes nothing.

You do not need to drain queues, stop workers, change storage routing, or change configuration or privileges. The admin UI's faster queue and dashboard reads, and its handling of abandoned requests, come from the 0.6.10 `awa serve` process (or an application embedding `awa_ui::router`). They do not depend on the schema patch.

## What `awa migrate` changes

The schema stays at v40. The `prepared_lane_sequences` patch replaces the queue-storage installer, adds `awa.refresh_lane_sequence_helpers`, and replaces three lane helper functions in `awa` and in every other queue-storage schema. Those helpers create and advance lane sequences. After the patch they skip `CREATE SEQUENCE` for a sequence that exists, so a runtime role can work without schema `CREATE` once its queues are provisioned (see [below](#optional-run-the-runtime-role-without-schema-create)).

The patch:

- takes no lock on any Awa table or index, and reads no queue rows;
- keeps every lane's sequence position;
- leaves storage routing unchanged;
- applies as one transaction, so an interrupted run leaves nothing applied; run it again.

If `awa migrate` exits 1 with `still missing: lane helpers in schema <name>`, the migrating role cannot use or does not own that schema. Run `awa migrate` again as the role that owns it.

### External migration runners

`awa migrate --sql --pending` prints only what the database lacks: on a 0.6.9 database, the `R__prepared_lane_sequences` patch. The output is one transaction that takes the same advisory lock as `awa migrate`. Apply it with psql's `ON_ERROR_STOP`, without which psql exits 0 even when the transaction rolled back:

```bash
awa --database-url "$AWA_MIGRATOR_DATABASE_URL" migrate --sql --pending \
  | psql -v ON_ERROR_STOP=1 "$AWA_MIGRATOR_DATABASE_URL"
```

Pass `--no-transaction` if your runner opens the transaction itself. With `--extract-to`, apply `R__prepared_lane_sequences.sql` as a repeatable script after `V40`. `--from`, `--to` and `--version` select a range to render with `--sql` or `--extract-to`; `awa migrate` refuses them when applying.

## Roll back

Roll the application back to 0.6.9 and leave the patch installed. 0.6.9 workers, producers, `awa serve` and `awa migrate` all work against it: the helpers keep their 0.6.9 arguments and behaviour for a role with schema `CREATE`, and the 0.6.9 `awa migrate` changes nothing on a v40 database. No reverse SQL is needed or provided.

This also holds after the optional privilege change below, for queues you provisioned.

## Optional: run the runtime role without schema `CREATE`

A 0.6.9 runtime role needs schema `CREATE`, because it creates each lane's sequences the first time it uses the lane. After the patch, you can revoke that once every queue is provisioned. Do this only after `awa migrate` has applied the patch, and run each step as the owner.

1. **Provision every physical queue** that workers serve or producers enqueue to:

   ```bash
   awa --database-url "$AWA_MIGRATOR_DATABASE_URL" storage prepare-queue \
     --schema <queue-storage schema> --queue <queue> [--enqueue-shards N] [--queue-stripe-count N]
   ```

   Match `--enqueue-shards` and `--queue-stripe-count` to the runtime's configuration, and provision each physical queue of a `PartitionedQueue`. The queues in use so far are listed by `SELECT DISTINCT queue FROM <queue-storage schema>.queue_lanes`. Provisioning is safe on live queues, including lanes the 0.6.9 runtime created, and keeps their positions.

2. **Grant sequence `UPDATE`** on the owner's sequences. Sequences the runtime created are its own already, and `GRANT ... ON ALL SEQUENCES` fails on them, so grant per sequence:

   ```sql
   DO $$
   DECLARE v_seq TEXT;
   BEGIN
       FOR v_seq IN
           SELECT format('%I.%I', schemaname, sequencename) FROM pg_sequences
           WHERE schemaname IN ('awa', '<queue-storage schema>') AND sequenceowner = current_user
       LOOP
           EXECUTE format('GRANT USAGE, SELECT, UPDATE ON SEQUENCE %s TO awa_runtime', v_seq);
       END LOOP;
   END
   $$;
   ALTER DEFAULT PRIVILEGES IN SCHEMA awa GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO awa_runtime;
   ALTER DEFAULT PRIVILEGES IN SCHEMA <queue-storage schema> GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO awa_runtime;
   ```

3. **Verify** that this returns no rows:

   ```sql
   SELECT format('%I.%I', schemaname, sequencename) FROM pg_sequences
   WHERE schemaname IN ('awa', '<queue-storage schema>')
     AND NOT has_sequence_privilege('awa_runtime', format('%I.%I', schemaname, sequencename), 'USAGE, SELECT, UPDATE');
   ```

4. **Revoke `CREATE`**:

   ```sql
   REVOKE CREATE ON SCHEMA awa FROM awa_runtime;
   REVOKE CREATE ON SCHEMA <queue-storage schema> FROM awa_runtime;
   ```

Workers and producers on 0.6.9 or 0.6.10 keep working on provisioned queues. A queue that was not provisioned fails with SQLSTATE `42501` and `lane sequence ... has not been provisioned`; provision it and retry. Provision new queues before their first use. To undo this step, grant `CREATE` back.

## How this is tested

Every release-branch CI run, and so every tag, runs `awa/tests/patch_upgrade_rehearsal_test.rs` on PostgreSQL 17 and 18. It installs with the released 0.6.9 CLI as an owner role, activates queue storage in a custom schema, and runs the released 0.6.9 Python worker and producer as a separate runtime role over a banked backlog with retries, scheduled jobs and slow jobs in flight. It then:

- applies the patch with the candidate `awa migrate` under that traffic, checking that no Awa table is locked, that idle lanes keep their exact sequence positions and live ones only advance, and that routing is unchanged;
- runs both versions together while killing and restarting workers;
- rolls back to 0.6.9 only, including the 0.6.9 `awa migrate`;
- tightens privileges as above and restarts the 0.6.9 workers.

Every enqueued job must complete, on a later attempt if it was retried, and per-queue counts must match what was enqueued.

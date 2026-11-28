-- v064: retain a job's enqueue shard on deferred and DLQ rows.
-- Tracking: https://github.com/hardbyte/awa/issues/340
--
-- Adds a nullable enqueue_shard column to deferred_jobs and dlq_entries in
-- every queue-storage schema. A retried, snoozed, rescued or dead-lettered job
-- keeps the shard of the lane it was claimed from, and a scheduled job with an
-- ordering key keeps the shard its key selects. When the job becomes ready
-- again it goes back to that shard, taken modulo the destination queue's
-- enqueue_shards, instead of a rotor-chosen one. NULL routes through the rotor,
-- as every row written before this migration does. ADR-033 builds per-key
-- execution control on this.
--
-- This file is the migration tail. `awa migrate` and
-- `awa migrate --sql --from 63 --to 64` run V23_UP first, which redefines
-- the installer (so fresh installs and prepare_schema create the column) and
-- defines awa.install_queue_storage_retained_shard. The default substrate is
-- not replayed: this tail calls only that function for each schema
-- awa.queue_storage_schemas() selects.
--
-- N-1 binaries are unaffected: they write deferred and DLQ rows with explicit
-- column lists, which leave the column NULL, and never read it. A binary that
-- retains shards probes the catalog for the column before writing or reading
-- it, so it can run before this migration and starts retaining shards with
-- its first write after the migration commits. Rows an N-1 binary writes after
-- the migration carry NULL and are routed as before.
--
-- External SQL runners: no runtime preflight is needed. The compat insert
-- functions (awa.insert_job_compat, awa.insert_jobs_compat_batch) are
-- unchanged and leave the column NULL on scheduled rows.
--
-- Locks: adding a column takes ACCESS EXCLUSIVE on deferred_jobs and
-- dlq_entries, held until the migration transaction commits. Neither table is
-- scanned or rewritten: the column is nullable without a default. A schema
-- that already has the column is not locked. When no lock_timeout is set,
-- each lock acquisition waits at most 2 s. A timeout (SQLSTATE 55P03) or a
-- deadlock (40P01) rolls the whole migration back and it can be re-run.
--
-- Wall time: catalog updates only, so it does not grow with data volume;
-- it is the time to acquire the locks plus milliseconds per schema.
--
-- Design: docs/adr/033-per-key-execution-control.md

DO $$
DECLARE
    v_schema TEXT;
    v_previous_lock_timeout TEXT := current_setting('lock_timeout');
BEGIN
    IF v_previous_lock_timeout = '0' THEN
        PERFORM set_config('lock_timeout', '2s', true);
    END IF;

    FOR v_schema IN
        SELECT discovered.schema_name FROM awa.queue_storage_schemas() AS discovered
    LOOP
        CONTINUE WHEN to_regclass(format('%I.deferred_jobs', v_schema)) IS NULL
            OR to_regclass(format('%I.dlq_entries', v_schema)) IS NULL;
        BEGIN
            PERFORM awa.install_queue_storage_retained_shard(v_schema);
        EXCEPTION
            WHEN lock_not_available THEN
                RAISE EXCEPTION 'v064 could not lock %''s deferred_jobs and dlq_entries within lock_timeout % to add the enqueue_shard column; a long transaction holds them. Nothing was migrated: re-run when they are free, or with a longer lock_timeout for the migration session',
                    quote_ident(v_schema), current_setting('lock_timeout')
                    USING ERRCODE = '55P03';
            WHEN deadlock_detected THEN
                RAISE EXCEPTION 'v064 deadlocked with a concurrent transaction while locking %''s deferred_jobs and dlq_entries to add the enqueue_shard column. Nothing was migrated: re-run it',
                    quote_ident(v_schema)
                    USING ERRCODE = '40P01';
        END;
    END LOOP;

    PERFORM set_config('lock_timeout', v_previous_lock_timeout, true);
END;
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (64, 'Retain the enqueue shard on deferred and DLQ rows')
ON CONFLICT (version) DO NOTHING;

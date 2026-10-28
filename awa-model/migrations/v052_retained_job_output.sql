-- v052: retained job output (ADR-048)
--
-- Adds `done_entries.output` and `receipt_completion_batches.outputs` to
-- every queue-storage schema, a NOT VALID cardinality check on the batch
-- array, and a final `output` column on `terminal_jobs`.
--
-- This file is the migration tail. `awa migrate` and
-- `awa migrate --sql --from 51 --to 52` run V23_UP first, which redefines
-- the installer (so fresh installs and prepare_schema create the columns)
-- and defines awa.install_queue_storage_job_output. The default substrate is
-- not replayed: this tail calls only that function for each schema
-- awa.queue_storage_schemas() selects.
--
-- N-1 binaries are unaffected: they insert terminal rows with explicit
-- column lists, which leave the new columns NULL, and read terminal_jobs by
-- explicit column. A binary that knows ADR-048 probes the catalog for the
-- columns before writing or reading them, so it can run before this
-- migration.
--
-- Locks: adding a column to a partitioned table takes ACCESS EXCLUSIVE on
-- the parent and every partition, and replacing the view takes it on the
-- view; all are held until the migration transaction commits. Nothing is
-- scanned or rewritten: the columns are nullable without defaults and the
-- check is NOT VALID. When no lock_timeout is set, each lock acquisition
-- waits at most 2 s. A timeout (SQLSTATE 55P03) rolls the whole migration
-- back and it can be re-run.
-- Design: docs/adr/048-retained-job-output.md

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
        CONTINUE WHEN to_regclass(format('%I.done_entries', v_schema)) IS NULL
            OR to_regclass(format('%I.receipt_completion_batches', v_schema)) IS NULL;
        BEGIN
            PERFORM awa.install_queue_storage_job_output(v_schema);
        EXCEPTION WHEN lock_not_available THEN
            RAISE EXCEPTION 'v052 could not lock %''s terminal tables within lock_timeout % to add the job output columns; a long transaction or terminal read holds them. Nothing was migrated: re-run when they are free, or with a longer lock_timeout for the migration session',
                quote_ident(v_schema), current_setting('lock_timeout')
                USING ERRCODE = '55P03';
        END;
    END LOOP;

    PERFORM set_config('lock_timeout', v_previous_lock_timeout, true);
END;
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (52, 'Retained job output columns (ADR-048)')
ON CONFLICT (version) DO NOTHING;

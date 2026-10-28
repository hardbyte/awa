-- v052: retained job output (ADR-048)
--
-- Adds `done_entries.output` and `receipt_completion_batches.outputs` to
-- every queue-storage schema, a NOT VALID cardinality check on the batch
-- array, and a final `output` column on `terminal_jobs`. Also adds
-- `awa.job_kind_descriptors.output`, the output type a kind declares
-- (`#[awa(output = T)]`), capped at 64 KiB of JSON text.
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
-- view; all are held until the migration transaction commits. The
-- descriptor catalog is locked the same way, for the time it takes to add
-- the column and check its handful of rows. No job table is scanned or
-- rewritten: the columns are nullable without defaults and the batch check
-- is NOT VALID. The view is locked before its tables, the order
-- terminal_jobs readers take them. When no lock_timeout is set, each lock
-- acquisition waits at most 2 s. A timeout (SQLSTATE 55P03) or a deadlock
-- (40P01) rolls the whole migration back and it can be re-run.
--
-- Wall time: catalog updates only, so it does not grow with data volume;
-- it is the time to acquire the locks plus milliseconds per schema.
--
-- Design: docs/adr/048-retained-job-output.md
-- Discussion: https://github.com/hardbyte/awa/pull/565 (ADR),
-- https://github.com/hardbyte/awa/pull/566 (implementation)

DO $$
DECLARE
    v_schema TEXT;
    v_previous_lock_timeout TEXT := current_setting('lock_timeout');
BEGIN
    IF v_previous_lock_timeout = '0' THEN
        PERFORM set_config('lock_timeout', '2s', true);
    END IF;

    BEGIN
        ALTER TABLE awa.job_kind_descriptors ADD COLUMN IF NOT EXISTS output JSONB;
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conrelid = 'awa.job_kind_descriptors'::regclass
              AND conname = 'job_kind_descriptors_output_size'
        ) THEN
            ALTER TABLE awa.job_kind_descriptors
                ADD CONSTRAINT job_kind_descriptors_output_size
                CHECK (output IS NULL OR octet_length(output::text) <= 65536);
        END IF;
    EXCEPTION
        WHEN lock_not_available THEN
            RAISE EXCEPTION 'v052 could not lock awa.job_kind_descriptors within lock_timeout % to add its output column; a runtime''s descriptor sync or a long transaction holds it. Nothing was migrated: re-run it',
                current_setting('lock_timeout')
                USING ERRCODE = '55P03';
    END;

    FOR v_schema IN
        SELECT discovered.schema_name FROM awa.queue_storage_schemas() AS discovered
    LOOP
        CONTINUE WHEN to_regclass(format('%I.done_entries', v_schema)) IS NULL
            OR to_regclass(format('%I.receipt_completion_batches', v_schema)) IS NULL;
        BEGIN
            PERFORM awa.install_queue_storage_job_output(v_schema);
        EXCEPTION
            WHEN lock_not_available THEN
                RAISE EXCEPTION 'v052 could not lock %''s terminal tables within lock_timeout % to add the job output columns; a long transaction or terminal read holds them. Nothing was migrated: re-run when they are free, or with a longer lock_timeout for the migration session',
                    quote_ident(v_schema), current_setting('lock_timeout')
                    USING ERRCODE = '55P03';
            WHEN deadlock_detected THEN
                RAISE EXCEPTION 'v052 deadlocked with a concurrent transaction while locking %''s terminal tables to add the job output columns. Nothing was migrated: re-run it',
                    quote_ident(v_schema)
                    USING ERRCODE = '40P01';
        END;
    END LOOP;

    PERFORM set_config('lock_timeout', v_previous_lock_timeout, true);
END;
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (52, 'Retained job output columns (ADR-048)')
ON CONFLICT (version) DO NOTHING;

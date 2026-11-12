-- v056: recognise only a full job_ids GIN index as the claim batch index.
-- Tracking: https://github.com/hardbyte/awa/pull/701
-- <schema>.ensure_claim_batch_job_index(slot) from v054 reported a claim
-- partition indexed when it had any valid GIN index whose definition named
-- job_ids, a partial one included, and its CREATE INDEX IF NOT EXISTS took a
-- different relation that already had the index's name for the index. The
-- function now accepts only a valid, non-partial GIN index on job_ids alone
-- with the default opclass. When something else has the index's name it
-- raises a warning naming it, returns false and leaves it in place.
--
-- awa.refresh_claim_batch_job_index_helper now resets EXECUTE on the function
-- after it hands the function to the owner of lease_claim_batches, acting as
-- that owner when the caller may only SET ROLE to it, so EXECUTE ends with
-- exactly the roles that hold TRUNCATE on the table.
--
-- This file only reinstalls the function in each queue-storage schema; it
-- builds and drops no index. `awa migrate` and
-- `awa migrate --sql --from 55 --to 56` run V23_UP first, which defines the
-- helper.
--
-- N-1 binaries call the function with the same argument and result. An index
-- that v054's function built is a full job_ids index and is still recognised.
--
-- Live load: function replacement only. It takes no lock on any awa or
-- queue-storage table. Run it as the owner of the queue-storage tables, or as
-- a migrator that can SET ROLE to it, so the function is owned by the table
-- owner and can create the index.

DO $$
DECLARE
    v_schema TEXT;
BEGIN
    FOR v_schema IN
        SELECT discovered.schema_name FROM awa.queue_storage_schemas() AS discovered
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_class AS batches
            WHERE batches.oid = to_regclass(format('%I.lease_claim_batches', v_schema))
              AND batches.relkind = 'p'
        ) THEN
            CONTINUE;
        END IF;

        -- Serialize with a concurrent prepare_schema() of the same schema. This
        -- is an advisory lock, not a table lock: claims and completions do not
        -- take it.
        PERFORM pg_advisory_xact_lock(
            hashtextextended(format('awa.queue_storage.install:%I', v_schema), 0)
        );
        PERFORM awa.refresh_claim_batch_job_index_helper(v_schema);
    END LOOP;
END;
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (56, 'Recognise only a full job_ids index as the claim batch index')
ON CONFLICT (version) DO NOTHING;

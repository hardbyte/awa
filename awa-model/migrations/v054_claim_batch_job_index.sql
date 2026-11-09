-- v054: Index compact claim batches by job id.
-- Tracking: https://github.com/hardbyte/awa/pull/684
-- Point lookups of compact receipt claims by job id (heartbeat and progress
-- flush, completion fallbacks, receipt close, cancellation, rescue and
-- load_job) filter lease_claim_batches with job_ids && / @>, which a GIN index
-- on job_ids serves. The installer now installs
-- <schema>.ensure_claim_batch_job_index(slot) through
-- awa.refresh_claim_batch_job_index_helper and builds the index on each empty
-- claim partition; claim prune calls the function right after it truncates a
-- slot, so every partition gains the index within one claim-ring cycle.
--
-- This file only installs the function in each queue-storage schema; it
-- builds no index. `awa migrate` and `awa migrate --sql --from 53 --to 54` run
-- V23_UP first, which defines the helper.
--
-- A 0.6.15 database already carries the function from the 0.6
-- claim_batch_job_index schema patch; replacing it is idempotent.
--
-- N-1 binaries never call the function, and their by-job lookups return the
-- same rows with or without the index. Binaries that carry the indexed
-- lookups stay correct on an unindexed partition; they scan the claim batches.
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
VALUES (54, 'Index compact claim batches by job id')
ON CONFLICT (version) DO NOTHING;

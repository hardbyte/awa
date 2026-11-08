-- Schema patch claim_batch_job_index: index compact claim batches by job id.
--
-- Applied after the v023 definitions, whose installer now creates a GIN index
-- on lease_claim_batches.job_ids in each claim-slot partition that is empty,
-- and installs <schema>.ensure_claim_batch_job_index(slot) through
-- awa.refresh_claim_batch_job_index_helper. Claim prune calls that function
-- right after it truncates a slot, so every partition gains the index within
-- one claim-ring cycle. The block below installs the function in each
-- queue-storage schema and indexes the claim partitions that are empty now.
--
-- Live load: function replacement, plus index builds on empty partitions
-- only. A partition with rows, or one whose lock is held, is skipped without
-- waiting (LOCK ... NOWAIT), so no build ever runs over live claim batches.
-- An index built here holds that partition's SHARE lock until the patch
-- commits. It does not re-run the installer, which would lock hot tables such
-- as leases and deferred_jobs for the rest of the transaction.
--
-- 0.6 delivery: the 0.6 series cannot add a migration version, so this ships as
-- an idempotent schema patch. `awa migrate` applies it when the schema is
-- exactly at v040 and the probe in migrations.rs reports it missing;
-- `awa migrate --sql` prints it as a repeatable R__ script for external
-- runners. Released 0.6 binaries never call the new function, and their
-- by-job lookups return the same rows with or without the index. Binaries
-- that carry the indexed lookups stay correct on an unpatched schema; they
-- scan the claim batches as before. Run it as the owner of the queue-storage
-- tables, or as a migrator that can SET ROLE to it.

DO $$
DECLARE
    v_schema TEXT;
    v_slot INT;
BEGIN
    FOR v_schema IN
        SELECT n.nspname
        FROM pg_namespace AS n
        WHERE has_schema_privilege(current_user, n.oid, 'USAGE')
          AND EXISTS (
              SELECT 1 FROM pg_proc AS awa_p
              WHERE awa_p.pronamespace = n.oid
                AND awa_p.proname = 'claim_ready_runtime'
                AND oidvectortypes(awa_p.proargtypes)
                    = 'text, bigint, double precision, double precision'
          )
          AND EXISTS (
              SELECT 1 FROM pg_class AS awa_c
              WHERE awa_c.relnamespace = n.oid
                AND awa_c.relname = 'lease_claim_batches'
                AND awa_c.relkind = 'p'
          )
        ORDER BY n.nspname
    LOOP
        -- Serialize with a concurrent prepare_schema() of the same schema. This
        -- is an advisory lock, not a table lock: claims and completions do not
        -- take it.
        PERFORM pg_advisory_xact_lock(
            hashtextextended(format('awa.queue_storage.install:%I', v_schema), 0)
        );
        PERFORM awa.refresh_claim_batch_job_index_helper(v_schema);

        FOR v_slot IN
            SELECT substring(child.relname FROM '^lease_claim_batches_([0-9]+)$')::int
            FROM pg_inherits AS inheritance
            JOIN pg_class AS child ON child.oid = inheritance.inhrelid
            WHERE inheritance.inhparent = format('%I.lease_claim_batches', v_schema)::regclass
              AND child.relname ~ '^lease_claim_batches_[0-9]+$'
            ORDER BY 1
        LOOP
            BEGIN
                EXECUTE format('SELECT %I.ensure_claim_batch_job_index($1)', v_schema)
                    USING v_slot;
            EXCEPTION WHEN insufficient_privilege THEN
                RAISE WARNING 'could not index %.lease_claim_batches_% by job id: %',
                    v_schema, v_slot, SQLERRM
                    USING HINT = 'Apply the patch as the owner of the queue-storage tables, or as a role that can SET ROLE to it.';
            END;
        END LOOP;
    END LOOP;
END;
$$;

-- Schema patch prepared_lane_sequences: use provisioned lane sequences without
-- runtime DDL (awa 0.7 migration v049, minus the schema_version row).
--
-- Applied after the v023 definitions of awa.refresh_lane_sequence_helpers and
-- awa.install_queue_storage_substrate, so new queue-storage schemas get the
-- updated helpers from the installer. On each installed schema the block below
-- replaces only the three lane helpers (ensure_lane_sequences and the two
-- head-sync trigger functions). They skip CREATE SEQUENCE when the sequence
-- exists and report a provisioning hint (SQLSTATE 42501) when it is missing and
-- the caller lacks schema CREATE.
--
-- Live load: function replacement only. The patch takes no table locks, drops
-- no triggers and reads no queue rows, so workers keep claiming and completing
-- while it runs. It does not re-run the installer, which would lock hot tables
-- such as leases and deferred_jobs for the rest of the transaction.
--
-- 0.6 delivery: the 0.6 series cannot add a migration version, so this ships as
-- an idempotent schema patch. `awa migrate` applies it when the schema is
-- exactly at v040 and the probe in migrations.rs reports it missing;
-- `awa migrate --sql` prints it as a repeatable R__ script for external
-- runners. Released 0.6 binaries call the same functions with the same
-- arguments and cursor semantics. Run it as the schema owner or migrator.

DO $$
DECLARE
    v_schema TEXT;
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
        ORDER BY n.nspname
    LOOP
        IF to_regclass(format('%I.queue_enqueue_heads', v_schema)) IS NULL
           OR to_regclass(format('%I.queue_claim_heads', v_schema)) IS NULL
           OR to_regprocedure(format('%I.queue_lane_sequence_name(text,text,smallint,smallint)', v_schema)) IS NULL
           OR to_regprocedure(format('%I.set_sequence_next(text,bigint)', v_schema)) IS NULL THEN
            CONTINUE;
        END IF;

        -- Serialize with a concurrent prepare_schema() of the same schema. This
        -- is an advisory lock, not a table lock: claims and completions do not
        -- take it.
        PERFORM pg_advisory_xact_lock(
            hashtextextended(format('awa.queue_storage.install:%I', v_schema), 0)
        );
        PERFORM awa.refresh_lane_sequence_helpers(v_schema);
    END LOOP;
END;
$$;

-- Schema patch definer_lane_provisioning: create queue lanes as the schema
-- owner, whichever role touches them first (awa 0.7 migration v053, minus the
-- schema_version row).
--
-- Applied after the v023 definitions, whose awa.refresh_lane_sequence_helpers
-- now installs two SECURITY DEFINER functions in each queue-storage schema,
-- provision_lane and provision_lane_sequences, owned by the schema owner. The
-- lane helpers create a missing sequence through them, and each new sequence
-- gets the grants of the head table it backs, so a lane is usable by every
-- runtime role however its creator's default privileges are set. The block
-- below refreshes the helpers on each installed schema and counts existing
-- lane sequences that need repair. It does not reassign them: ALTER SEQUENCE
-- OWNER takes the sequence's ACCESS EXCLUSIVE lock until commit. After the
-- patch commits, `awa migrate` repairs each one in its own short transaction
-- with a bounded lock wait (awa.repair_lane_sequence); another runner can do
-- the same with `awa storage lane-sequences --repair`.
--
-- Live load: function replacement only. Reads the catalog; no table DDL, row
-- scans or sequence locks.
--
-- 0.6 delivery: the 0.6 series cannot add a migration version, so this ships as
-- an idempotent schema patch. `awa migrate` applies it when the schema is
-- exactly at v040 and the probe in migrations.rs reports it missing;
-- `awa migrate --sql` prints it as a repeatable R__ script for external
-- runners. Released 0.6 binaries call the same helpers with the same arguments
-- and cursor semantics, and lanes they create are owned by the schema owner
-- too. Run it as the schema owner, or as a migrator that can SET ROLE to it.

DO $$
DECLARE
    v_schema TEXT;
    v_count BIGINT;
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

        SELECT count(*) INTO v_count
        FROM awa.lane_sequence_problems(v_schema) AS problem;

        IF v_count > 0 THEN
            RAISE NOTICE '% lane sequence(s) in schema % are not owned by the schema owner or lack runtime grants', v_count, v_schema
                USING HINT = format(
                    'awa migrate repairs them after this commits. Otherwise run awa storage lane-sequences --schema %s --repair as the migration role itself, without SET ROLE.',
                    v_schema
                );
        END IF;
    END LOOP;
END;
$$;

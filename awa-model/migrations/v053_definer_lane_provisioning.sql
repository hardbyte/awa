-- v053: Create queue lanes as the schema owner, whichever role touches them first.
-- Tracking: https://github.com/hardbyte/awa/pull/637
-- Lane sequences used to belong to the role that first enqueued into, claimed
-- from or promoted into a lane, and only that role (plus whoever its default
-- privileges named) could use them. refresh_lane_sequence_helpers now installs
-- two SECURITY DEFINER functions, provision_lane and provision_lane_sequences,
-- owned by the schema owner, and the lane helpers create a missing sequence
-- through them. Each new sequence gets the grants of the head table it backs.
-- This migration does not reassign existing lane sequences: ALTER SEQUENCE
-- OWNER takes the sequence's ACCESS EXCLUSIVE lock until commit, and the
-- migration runs in one transaction. After it commits, `awa migrate` repairs
-- each lane sequence in its own short transaction with a bounded lock wait
-- (awa.repair_lane_sequence). Another runner can do the same with
-- `awa storage lane-sequences --repair`.
--
-- N-1 binaries call the same helpers with the same arguments and cursor
-- semantics, so lanes they create are owned by the schema owner too. A binary
-- that predates provision_lane keeps using the helpers; this binary probes for
-- provision_lane and uses the helpers when it is missing.
--
-- Live load: function replacement only. Reads the catalog; no table DDL, row
-- scans or sequence locks.

DO $$
DECLARE
    v_schema TEXT;
    v_count BIGINT;
BEGIN
    FOR v_schema IN
        SELECT discovered.schema_name FROM awa.queue_storage_schemas() AS discovered
    LOOP
        IF to_regclass(format('%I.queue_enqueue_heads', v_schema)) IS NULL
           OR to_regclass(format('%I.queue_claim_heads', v_schema)) IS NULL
           OR to_regclass(format('%I.queue_ring_state', v_schema)) IS NULL
           OR to_regclass(format('%I.lease_ring_state', v_schema)) IS NULL
           OR to_regclass(format('%I.claim_ring_state', v_schema)) IS NULL THEN
            CONTINUE;
        END IF;

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

INSERT INTO awa.schema_version (version, description)
VALUES (53, 'Create queue lanes as the schema owner')
ON CONFLICT (version) DO NOTHING;

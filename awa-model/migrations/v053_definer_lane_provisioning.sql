-- v053: Create queue lanes as the schema owner, whichever role touches them first.
-- Tracking: https://github.com/hardbyte/awa/pull/637
-- Lane sequences used to belong to the role that first enqueued into, claimed
-- from or promoted into a lane, and only that role (plus whoever its default
-- privileges named) could use them. refresh_lane_sequence_helpers now installs
-- two SECURITY DEFINER functions, provision_lane and provision_lane_sequences,
-- owned by the schema owner, and the lane helpers create a missing sequence
-- through them. Each new sequence gets the grants of the head table it backs.
-- This migration then repairs existing lane sequences: it gives each one to
-- the schema owner and adds its missing grants, one sequence at a time and
-- only where the migrating role may. Sequences it cannot reassign are reported
-- in a warning and by awa.lane_sequence_problems(); the migration still commits.
--
-- N-1 binaries call the same helpers with the same arguments and cursor
-- semantics, so lanes they create are owned by the schema owner too. A binary
-- that predates provision_lane keeps using the helpers; this binary probes for
-- provision_lane and uses the helpers when it is missing.
--
-- Live load: function replacement, plus ALTER SEQUENCE OWNER on the lane
-- sequences that need it. That takes each such sequence's ACCESS EXCLUSIVE
-- lock until commit, so enqueues and claims on those lanes wait for the
-- migration to commit; correctly owned lanes are not touched. Reads the
-- catalog only; no table DDL or row scans.

DO $$
DECLARE
    v_schema TEXT;
    v_leftovers TEXT;
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

        SELECT count(*),
               string_agg(format('%s (owner %s, %s)', repair.sequence_name, repair.owner, repair.outcome),
                          ', ' ORDER BY repair.sequence_name)
        INTO v_count, v_leftovers
        FROM awa.repair_lane_sequences(v_schema) AS repair
        WHERE repair.outcome <> 'repaired';

        IF v_count > 0 THEN
            RAISE WARNING '% lane sequence(s) in schema % still need repair: %', v_count, v_schema, v_leftovers
                USING HINT = format(
                    'Run awa storage lane-sequences --schema %s for the statements to run and the role to run them as.',
                    v_schema
                );
        END IF;
    END LOOP;
END;
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (53, 'Create queue lanes as the schema owner')
ON CONFLICT (version) DO NOTHING;

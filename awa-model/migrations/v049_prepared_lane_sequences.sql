-- v049: Use provisioned lane sequences without runtime DDL.
-- Tracking: https://github.com/hardbyte/awa/pull/501
-- N-1 binaries use unchanged helper signatures and cursor semantics. Owners
-- retain lazy sequence creation; restricted roles use provisioned lanes.
-- Live load: function replacement only, with no table DDL or row scans.
-- Three schemas took 54 ms on disposable PostgreSQL 17. Row volume does not
-- change the work; function lock waits and schema count determine wall time.

DO $$
DECLARE
    v_schema TEXT;
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

        -- The full installer drops head triggers and locks live queue tables.
        PERFORM awa.refresh_lane_sequence_helpers(v_schema);
    END LOOP;
END;
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (49, 'Use provisioned lane sequences without runtime DDL')
ON CONFLICT (version) DO NOTHING;

-- v058: provision a lane's queue_lanes row before its lane lock.
-- Tracking: https://github.com/hardbyte/awa/pull/728
-- <schema>.provision_lane took the lane's advisory lock and then inserted its
-- queue_lanes row. Producers and QueueStorage insert the row first and take
-- the lock after it, so a provision_lane call and a producer that both created
-- the same new lane could each hold what the other waited for, and PostgreSQL
-- cancelled one with a deadlock error. provision_lane now inserts the row
-- first.
--
-- This file reinstalls provision_lane and provision_lane_sequences in each
-- queue-storage schema through awa.refresh_lane_sequence_helpers, which
-- V23_UP defines. N-1 binaries call provision_lane with the same arguments.
--
-- Live load: function replacement only. It takes no lock on any awa or
-- queue-storage table.

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
        PERFORM awa.refresh_lane_sequence_helpers(v_schema);
    END LOOP;
END;
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (58, 'Provision a lane''s row before its lock')
ON CONFLICT (version) DO NOTHING;

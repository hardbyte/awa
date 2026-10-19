-- v052: Fewer round trips and sequence writes per queue-storage enqueue.
-- Tracking: https://github.com/hardbyte/awa/issues/522
-- reserve_enqueue_seq keeps its signature, lock key and return value. It
-- provisions lane sequences only for a lane row without a sequence name and
-- reserves a multi-row range with one sequence write instead of two.
-- N-1 binaries call it unchanged.
-- Live load: function replacement only, with no table DDL or row scans.
-- Function lock waits and schema count determine wall time.

DO $$
DECLARE
    v_schema TEXT;
BEGIN
    FOR v_schema IN
        SELECT discovered.schema_name FROM awa.queue_storage_schemas() AS discovered
    LOOP
        IF to_regclass(format('%I.queue_enqueue_heads', v_schema)) IS NULL
           OR to_regclass(format('%I.queue_claim_heads', v_schema)) IS NULL
           OR to_regclass(format('%I.queue_ring_state', v_schema)) IS NULL THEN
            CONTINUE;
        END IF;

        PERFORM awa.refresh_enqueue_functions(v_schema);
    END LOOP;
END;
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (52, 'Fewer round trips and sequence writes per queue-storage enqueue')
ON CONFLICT (version) DO NOTHING;

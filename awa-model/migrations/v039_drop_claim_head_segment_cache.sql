-- v039: drop the queue_claim_heads ready-segment routing cache.
--
-- v023 owns the queue-storage substrate helper. The migration runner reapplies
-- the helper definition before this file, so claim_ready_runtime() is already
-- redefined to resolve the ready slot/generation from ready_segments on every
-- claim, and fresh installs no longer create the ready_segment_* columns.
--
-- This migration refreshes every already-installed queue-storage schema (so the
-- cache-free claim_ready_runtime() reaches awa and any in-transition awa_exp)
-- and then drops the now-unused cache columns. The cache re-introduced a
-- per-claim UPDATE on a single hot queue_claim_heads row whose dead tuples could
-- not be reclaimed under a pinned MVCC snapshot, which made queue_claim_heads
-- the dominant dead-tuple accumulator during long-running transactions. The
-- table returns to the cold lane-registry shape #295 intended.

DO $$
DECLARE
    v_schema TEXT;
    v_queue_slots INT;
    v_lease_slots INT;
    v_claim_slots INT;
    v_claim_runtime REGPROCEDURE;
    v_claim_runtime_def TEXT;
    v_lease_claim_receipts BOOLEAN;
BEGIN
    FOR v_schema IN
        SELECT n.nspname
        FROM pg_namespace AS n
        WHERE to_regprocedure(format(
            '%I.claim_ready_runtime(text,bigint,double precision,double precision)',
            n.nspname
        )) IS NOT NULL
    LOOP
        IF to_regclass(format('%I.queue_ring_state', v_schema)) IS NULL
           OR to_regclass(format('%I.lease_ring_state', v_schema)) IS NULL
           OR to_regclass(format('%I.claim_ring_state', v_schema)) IS NULL THEN
            CONTINUE;
        END IF;

        EXECUTE format(
            'SELECT slot_count FROM %I.queue_ring_state WHERE singleton = TRUE',
            v_schema
        )
        INTO v_queue_slots;

        EXECUTE format(
            'SELECT slot_count FROM %I.lease_ring_state WHERE singleton = TRUE',
            v_schema
        )
        INTO v_lease_slots;

        EXECUTE format(
            'SELECT slot_count FROM %I.claim_ring_state WHERE singleton = TRUE',
            v_schema
        )
        INTO v_claim_slots;

        IF v_queue_slots IS NULL OR v_lease_slots IS NULL OR v_claim_slots IS NULL THEN
            CONTINUE;
        END IF;

        v_claim_runtime := to_regprocedure(format(
            '%I.claim_ready_runtime(text,bigint,double precision,double precision)',
            v_schema
        ));
        v_claim_runtime_def := pg_get_functiondef(v_claim_runtime::oid);
        v_lease_claim_receipts := v_schema = 'awa'
            OR position(format('INSERT INTO %I.lease_claims', v_schema) IN v_claim_runtime_def) > 0;

        PERFORM awa.install_queue_storage_substrate(
            v_schema,
            v_queue_slots,
            v_lease_slots,
            v_claim_slots,
            v_lease_claim_receipts
        );

        EXECUTE format(
            'ALTER TABLE %I.queue_claim_heads DROP COLUMN IF EXISTS ready_segment_slot',
            v_schema
        );
        EXECUTE format(
            'ALTER TABLE %I.queue_claim_heads DROP COLUMN IF EXISTS ready_segment_generation',
            v_schema
        );
        EXECUTE format(
            'ALTER TABLE %I.queue_claim_heads DROP COLUMN IF EXISTS ready_segment_next_lane_seq',
            v_schema
        );
    END LOOP;
END;
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (39, 'Drop queue_claim_heads ready-segment routing cache for queue storage')
ON CONFLICT (version) DO NOTHING;

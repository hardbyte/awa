-- v039: refresh claim_ready_runtime to the cache-free ready-segment routing path.
--
-- v023 owns the queue-storage substrate helper; the migration runner reapplies
-- it before this file, so claim_ready_runtime() is already redefined to resolve
-- the target ready slot from ready_segments on every claim (ordered by
-- next_lane_seq so the existing index short-circuits at LIMIT 1) instead of
-- caching the result back onto the singleton queue_claim_heads row. That
-- per-claim UPDATE was the dominant dead-tuple accumulator under a pinned MVCC
-- snapshot.
--
-- This migration refreshes every already-installed queue-storage schema so the
-- cache-free function reaches awa and any in-transition awa_exp. The
-- ready_segment_* cache columns are intentionally LEFT IN PLACE: the
-- additive-only migration policy (migrations.rs) forbids dropping columns that a
-- rolling worker's queue_storage_schema_ready check still requires. They are
-- unused after this migration; a future major version can drop them.

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
    END LOOP;
END;
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (39, 'Refresh claim_ready_runtime to cache-free ready-segment routing')
ON CONFLICT (version) DO NOTHING;

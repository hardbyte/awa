-- v042: compact deadline receipt claims (#246).
--
-- v023 owns the queue-storage substrate helper; the migration runner reapplies
-- it before this file, so claim_ready_runtime() is already redefined to write
-- deadline-backed claims as compact lease_claim_batches rows (with the batch's
-- single deadline_at populated) instead of one row-local lease_claims row per
-- job. Every job claimed by one call shares claimed_at, so a batch carries
-- exactly one deadline value; at claim saturation this collapses the
-- receipt-plane write volume for deadline-backed queues from one row per job
-- to one row per claimed batch, which is what removed the e2e latency
-- regression measured in #246.
--
-- The refreshed helper also adds, per installed schema:
--   * claim_ring_slots.batch_deadline_cursor_deadline_at /
--     batch_deadline_cursor_batch_id — the per-slot sweep cursor for the new
--     compact-batch deadline rescue (pattern follows the v035 row-local
--     deadline cursor),
--   * a partial (deadline_at, batch_id) WHERE deadline_at IS NOT NULL index
--     on every lease_claim_batches child so the batch deadline sweep is
--     index-backed and free for zero-deadline traffic.
--
-- Additive only: the row-local lease_claims deadline machinery (columns,
-- indexes, cursors, rescue scan) is retained for claims materialized into
-- mutable leases and for legacy in-flight rows written before this upgrade.
--
-- This migration refreshes every already-installed queue-storage schema so
-- the compact claim function and the new cursor columns reach awa and any
-- in-transition custom substrate.

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
        WHERE has_schema_privilege(current_user, n.oid, 'USAGE')
          AND EXISTS (
              SELECT 1 FROM pg_proc AS awa_p
              WHERE awa_p.pronamespace = n.oid
                AND awa_p.proname = 'claim_ready_runtime'
                AND oidvectortypes(awa_p.proargtypes)
                    = 'text, bigint, double precision, double precision'
          )
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
        -- Receipts detection extends the v039 heuristic: pre-v042 receipts
        -- functions INSERT INTO lease_claims (row path), while v042+
        -- receipts functions only INSERT INTO lease_claim_batches. Checking
        -- both keeps the heuristic correct for old custom schemas AND for
        -- idempotent re-runs after this refresh has already been applied.
        v_lease_claim_receipts := v_schema = 'awa'
            OR position(format('INSERT INTO %I.lease_claims', v_schema) IN v_claim_runtime_def) > 0
            OR position(format('INSERT INTO %I.lease_claim_batches', v_schema) IN v_claim_runtime_def) > 0;

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
VALUES (42, 'Compact deadline receipt claims and batch deadline-rescue cursors (#246)')
ON CONFLICT (version) DO NOTHING;

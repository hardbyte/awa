-- v049: per-slot claim functions
--
-- The claim CTE now runs in per-queue-slot functions `claim_ready_slot_N`
-- that address the slot's ring children directly, so each statement plans
-- against one child, goes generic after a few executions and is only
-- invalidated when that child is truncated. `claim_ready_runtime` keeps its
-- signature and result and dispatches to the slot function; N-1 binaries
-- keep calling `claim_ready_runtime` and see identical evidence and results.
--
-- This file is the migration tail, not a standalone installer upgrade.
-- `awa migrate` and `awa migrate --sql --from 48 --to 49` run V23_UP first:
-- it defines the current installer and replays the canonical substrate;
-- this tail reinstalls discovered custom substrates too. Executing only this
-- file against a stale installer does not install the slot functions.
--
-- Reinstallation drops/recreates ring-state and queue-head triggers and can
-- take ACCESS EXCLUSIVE table locks through transaction commit. Lock waits,
-- schema count and the whole pending range determine wall time; there is no
-- fixed duration or function-only/lock-free replay guarantee. Use ownership-
-- capable DDL credentials, a quiet window and bounded lock_timeout. External
-- SQL runners must perform documented runtime/cron preflights before replay.

DO $$
DECLARE
    v_schema TEXT;
    v_queue_slots INT;
    v_lease_slots INT;
    v_claim_slots INT;
    v_claim_runtime REGPROCEDURE;
    v_claim_slot REGPROCEDURE;
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
        -- v049 moved the claim CTE (and its receipt INSERTs) into the
        -- per-slot functions, so the mode marker lives there now.
        v_claim_slot := to_regprocedure(format(
            '%I.claim_ready_slot_0(text,int,bigint,smallint,smallint,bigint,bigint,double precision,timestamptz,timestamptz,double precision,bigint)',
            v_schema
        ));
        IF v_claim_slot IS NOT NULL THEN
            v_claim_runtime_def := v_claim_runtime_def || pg_get_functiondef(v_claim_slot::oid);
        END IF;
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
VALUES (49, 'Per-slot claim functions')
ON CONFLICT (version) DO NOTHING;

-- v048: claim cursor catch-up inside the claim
--
-- `claim_ready_runtime` now moves a lane's claim cursor over committed
-- `ready_claim_attempt_batches` evidence at the start of a claim, so the
-- claiming runtime no longer needs a second transaction (and a second
-- `FOR UPDATE` on the claim head) to advance the cursor after its claim
-- commits. The evidence written by a claim is unchanged.
--
-- N-1 binaries retain guarded post-commit advances. Current receipt claimers
-- may omit an advance only for a non-drained lane on v048; drained lanes,
-- legacy leases and pre-v048 schemas retain it. A cached pre-v048 capability
-- remains a conservative fallback after migration.
--
-- This file is the migration tail, not a standalone installer upgrade.
-- `awa migrate` and `awa migrate --sql --from 47 --to 48` run V23_UP first:
-- it defines the current installer and replays the canonical substrate;
-- this tail reinstalls discovered custom substrates too. Executing only this
-- file against a stale installer does not install the changed function shape.
--
-- Reinstallation drops/recreates ring-state and queue-head triggers and can
-- take ACCESS EXCLUSIVE table locks through transaction commit. Lock waits,
-- schema count and the whole pending range determine wall time; there is no
-- fixed duration or function-only/lock-free replay guarantee. Use ownership-
-- capable DDL credentials, a quiet window and bounded lock_timeout. External
-- SQL runners must perform documented runtime/cron preflights before replay;
-- SQL export supplies the transaction/advisory wrapper, not Rust preflights.
-- Tracking: https://github.com/hardbyte/awa/pull/513
-- Model coverage: https://github.com/hardbyte/awa/pull/507#issuecomment-6105499712
-- Released-artifact rehearsal tracking: https://github.com/hardbyte/awa/issues/427

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
        SELECT discovered.schema_name FROM awa.queue_storage_schemas() AS discovered
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
VALUES (48, 'Claim cursor catch-up inside the claim')
ON CONFLICT (version) DO NOTHING;

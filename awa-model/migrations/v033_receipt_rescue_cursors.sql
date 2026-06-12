-- Add per-claim-slot receipt-rescue cursors to installed queue-storage
-- substrates. The helper body lives in v023 and is refreshed before this
-- migration runs; this sweep applies the additive columns and rescue index to
-- every substrate that already exists.

DO $$
DECLARE
    v_schema TEXT;
    v_queue_slots INT;
    v_lease_slots INT;
    v_claim_slots INT;
    v_claim_runtime REGPROCEDURE;
    v_claim_runtime_def TEXT;
    v_lease_claim_receipts BOOLEAN;
    v_terminal_counter_trusted BOOLEAN;
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

        EXECUTE format(
            'SELECT terminal_counter_trusted_at IS NOT NULL FROM %I.queue_ring_state WHERE singleton = TRUE',
            v_schema
        )
        INTO v_terminal_counter_trusted;

        PERFORM awa.install_queue_storage_substrate(
            v_schema,
            v_queue_slots,
            v_lease_slots,
            v_claim_slots,
            v_lease_claim_receipts
        );

        IF COALESCE(v_terminal_counter_trusted, FALSE) THEN
            EXECUTE format(
                'UPDATE %I.queue_ring_state SET terminal_counter_trusted_at = COALESCE(terminal_counter_trusted_at, now()) WHERE singleton = TRUE',
                v_schema
            );
        END IF;
    END LOOP;
END
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (33, 'Add per-slot receipt-rescue cursors for queue storage (#169)')
ON CONFLICT (version) DO NOTHING;

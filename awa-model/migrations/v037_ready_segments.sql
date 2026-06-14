-- #352: ready segment and claim-attempt ledgers for claim routing.
--
-- v023 owns the queue-storage substrate helper. The migration runner reapplies
-- the helper definition before this file, then this migration refreshes every
-- installed queue-storage schema so ready_segments is created, existing
-- committed ready rows and in-flight receipt claims are backfilled,
-- claim_ready_runtime() routes through the compact segment and
-- claim-attempt control planes before reading ready_entries, and
-- insert_job_compat() writes segment metadata for SQL compatibility producers.

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

CREATE OR REPLACE FUNCTION awa.storage_abort()
RETURNS TABLE (
    current_engine TEXT,
    active_engine TEXT,
    prepared_engine TEXT,
    state TEXT,
    transition_epoch BIGINT,
    details JSONB,
    entered_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    finalized_at TIMESTAMPTZ
)
LANGUAGE plpgsql
SET search_path = pg_catalog, awa, public
AS $$
DECLARE
    v_state TEXT;
    v_schema TEXT;
    v_live_queue_storage_count BIGINT;
    v_queue_storage_rows BIGINT := 0;
BEGIN
    SELECT
        sts.state,
        COALESCE(NULLIF(sts.details->>'schema', ''), 'awa_exp')
    INTO v_state, v_schema
    FROM awa.storage_transition_state AS sts
    WHERE sts.singleton
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'storage transition state row is missing'
            USING ERRCODE = '55000';
    END IF;

    IF v_state = 'mixed_transition' THEN
        SELECT count(*)::bigint
        INTO v_live_queue_storage_count
        FROM awa.runtime_instances AS runtime
        WHERE runtime.storage_capability = 'queue_storage'
          AND runtime.last_seen_at + make_interval(
                secs => GREATEST(((GREATEST(runtime.snapshot_interval_ms, 1000) / 1000) * 3)::int, 30)
              ) >= now();

        IF v_live_queue_storage_count > 0 THEN
            RAISE EXCEPTION 'cannot abort mixed transition while % queue-storage runtime(s) are still live', v_live_queue_storage_count
                USING ERRCODE = '55000';
        END IF;

        IF to_regclass(format('%I.%I', v_schema, 'ready_entries')) IS NOT NULL THEN
            EXECUTE format(
                'SELECT
                    (SELECT count(*)::bigint FROM %1$I.ready_entries)
                  + (SELECT count(*)::bigint FROM %1$I.ready_claim_attempt_batches)
                  + (SELECT count(*)::bigint FROM %1$I.ready_tombstones)
                  + (SELECT count(*)::bigint FROM %1$I.ready_segments)
                  + (SELECT count(*)::bigint FROM %1$I.deferred_jobs)
                  + (SELECT count(*)::bigint FROM %1$I.leases)
                  + (SELECT count(*)::bigint FROM %1$I.lease_claims)
                  + (SELECT count(*)::bigint FROM %1$I.lease_claim_closures)
                  + (SELECT count(*)::bigint FROM %1$I.lease_claim_closure_batches)
                  + (SELECT count(*)::bigint FROM %1$I.attempt_state)
                  + (SELECT count(*)::bigint FROM %1$I.done_entries)
                  + (SELECT count(*)::bigint FROM %1$I.receipt_completion_batches)
                  + (SELECT count(*)::bigint FROM %1$I.receipt_completion_tombstones)
                  + (SELECT count(*)::bigint FROM %1$I.dlq_entries)',
                v_schema
            )
            INTO v_queue_storage_rows;
        END IF;

        IF v_queue_storage_rows > 0 THEN
            RAISE EXCEPTION 'cannot abort mixed transition while queue storage contains % row(s)', v_queue_storage_rows
                USING ERRCODE = '55000';
        END IF;
    END IF;

    DELETE FROM awa.runtime_storage_backends
    WHERE backend = 'queue_storage';

    UPDATE awa.storage_transition_state AS sts
    SET
        prepared_engine = NULL,
        state = 'canonical',
        transition_epoch = CASE
            WHEN sts.state IN ('prepared', 'mixed_transition') THEN sts.transition_epoch + 1
            ELSE sts.transition_epoch
        END,
        details = '{}'::jsonb,
        entered_at = CASE
            WHEN sts.state = 'canonical' THEN sts.entered_at
            ELSE now()
        END,
        updated_at = now(),
        finalized_at = NULL
    WHERE sts.singleton
      AND sts.state IN ('canonical', 'prepared', 'mixed_transition');

    RETURN QUERY
    SELECT * FROM awa.storage_status();
END
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (37, 'Add ready_segments and ready_claim_attempt_batches range ledgers for queue storage')
ON CONFLICT (version) DO NOTHING;

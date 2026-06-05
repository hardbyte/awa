-- #295 pulled into v0.6: move the hot queue-storage lane cursors from
-- MVCC-updated heap rows to PostgreSQL sequences, and stripe the #290
-- terminal live counter so completion-heavy queues do not hammer one
-- counter row under pinned MVCC.
--
-- v023's install helper is part of this migration's step list before this
-- file, so `awa.install_queue_storage_substrate` has already been replaced
-- with the sequence-cursor substrate definition. This file applies that
-- refreshed helper to every existing AWA-owned queue-storage substrate and
-- records schema version 27.
--
-- The old `queue_enqueue_heads.next_seq` and `queue_claim_heads.claim_seq`
-- columns are retained as rolling-upgrade compatibility columns. The enqueue
-- trigger translates legacy row UPDATEs into sequence reservations. Claim
-- cursor movement is intentionally sequence-owned and post-commit: a legacy
-- `claim_seq` row UPDATE may differ from the hot cursor, but it cannot
-- advance a non-transactional sequence past work that later rolls back.

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

        -- Existing #290 deployments may already have
        -- queue_terminal_live_counts rows from the pre-striped shape. Adding
        -- counter_bucket gives those rows DEFAULT 0, while v27 delete/retry
        -- paths decrement by job_id % 256. Rebuild the counter once during
        -- the v27 rewalk so upgraded schemas do not keep an inflated bucket-0
        -- aggregate.
        EXECUTE format('TRUNCATE TABLE %I.queue_terminal_live_counts', v_schema);
        EXECUTE format(
            $sql$
            INSERT INTO %1$I.queue_terminal_live_counts AS counts (
                ready_slot, queue, priority, enqueue_shard, counter_bucket, live_terminal_count
            )
            SELECT
                ready_slot,
                queue,
                priority,
                enqueue_shard,
                mod(mod(job_id, 256::bigint) + 256::bigint, 256::bigint)::smallint AS counter_bucket,
                count(*)::bigint
            FROM %1$I.done_entries
            GROUP BY ready_slot, queue, priority, enqueue_shard, counter_bucket
            ON CONFLICT (ready_slot, queue, priority, enqueue_shard, counter_bucket) DO UPDATE
            SET live_terminal_count = EXCLUDED.live_terminal_count
            $sql$,
            v_schema
        );

        EXECUTE format(
            'UPDATE %I.queue_ring_state SET terminal_counter_trusted_at = now() WHERE singleton = TRUE',
            v_schema
        );
    END LOOP;
END
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (27, 'Move lane cursors to sequences and stripe terminal counters (#295/#290)')
ON CONFLICT (version) DO NOTHING;

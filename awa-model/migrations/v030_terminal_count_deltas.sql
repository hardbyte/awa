-- Add the queue_terminal_count_deltas ledger to every installed
-- queue-storage substrate and route compatibility terminal deletes through
-- signed deltas instead of direct live-counter updates.

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

        -- v27/v28 already rebuilt striped terminal counters before any
        -- deployment can reach v30. The helper refresh above is shape-only
        -- here, so preserve a previously trusted marker if it was cleared by
        -- the installer's conservative upgrade guard. If an operator had
        -- deliberately cleared the marker before upgrade, keep it cleared.
        IF COALESCE(v_terminal_counter_trusted, FALSE) THEN
            EXECUTE format(
                'UPDATE %I.queue_ring_state SET terminal_counter_trusted_at = COALESCE(terminal_counter_trusted_at, now()) WHERE singleton = TRUE',
                v_schema
            );
        ELSE
            EXECUTE format(
                'UPDATE %I.queue_ring_state SET terminal_counter_trusted_at = NULL WHERE singleton = TRUE',
                v_schema
            );
        END IF;
    END LOOP;
END
$$;

CREATE OR REPLACE FUNCTION awa.delete_job_compat(p_id BIGINT)
RETURNS BOOLEAN
LANGUAGE plpgsql
SET search_path = pg_catalog, awa, public
AS $$
DECLARE
    v_schema TEXT;
    v_queue TEXT;
    v_priority SMALLINT;
    v_lane_seq BIGINT;
    v_enqueue_shard SMALLINT;
    v_state awa.job_state;
    v_unique_key BYTEA;
    v_unique_states TEXT;
    v_ready_slot INT;
    v_ready_generation BIGINT;
    v_rows INT;
BEGIN
    v_schema := awa.active_queue_storage_schema();

    IF v_schema IS NULL THEN
        RAISE EXCEPTION 'queue storage is not active'
            USING ERRCODE = '55000';
    END IF;

    EXECUTE format(
        'WITH target AS (
             SELECT
                 ready.ready_slot,
                 ready.ready_generation,
                 ready.queue,
                 ready.priority,
                 ready.enqueue_shard,
                 ready.lane_seq,
                 ready.job_id,
                 ready.unique_key,
                 ready.unique_states
             FROM %1$I.ready_entries AS ready
             JOIN %1$I.queue_claim_heads AS claims
               ON claims.queue = ready.queue
              AND claims.priority = ready.priority
              AND claims.enqueue_shard = ready.enqueue_shard
             WHERE ready.job_id = $1
               AND ready.lane_seq >= %1$I.sequence_next_value(claims.seq_name)
               AND NOT EXISTS (
                   SELECT 1
                   FROM %1$I.ready_tombstones AS tomb
                   WHERE tomb.ready_slot = ready.ready_slot
                     AND tomb.ready_generation = ready.ready_generation
                     AND tomb.queue = ready.queue
                     AND tomb.priority = ready.priority
                     AND tomb.enqueue_shard = ready.enqueue_shard
                     AND tomb.lane_seq = ready.lane_seq
               )
             ORDER BY ready.lane_seq DESC
             LIMIT 1
             FOR UPDATE OF ready SKIP LOCKED
         ),
         tombstone AS (
             INSERT INTO %1$I.ready_tombstones (
                 ready_slot, ready_generation, queue, priority, enqueue_shard, lane_seq, job_id
             )
             SELECT ready_slot, ready_generation, queue, priority, enqueue_shard, lane_seq, job_id
             FROM target
             ON CONFLICT DO NOTHING
         )
         SELECT queue, priority, lane_seq, enqueue_shard,
                ''available''::awa.job_state, unique_key, unique_states
         FROM target',
        v_schema
    )
    INTO v_queue, v_priority, v_lane_seq, v_enqueue_shard,
         v_state, v_unique_key, v_unique_states
    USING p_id;
    GET DIAGNOSTICS v_rows = ROW_COUNT;

    IF v_rows > 0 THEN
        PERFORM awa.release_queue_storage_unique_claim(
            p_id,
            v_unique_key,
            v_unique_states,
            v_state
        );
        RETURN TRUE;
    END IF;

    EXECUTE format(
        'DELETE FROM %I.deferred_jobs
         WHERE job_id = $1
         RETURNING queue, priority, state, unique_key, unique_states',
        v_schema
    )
    INTO v_queue, v_priority, v_state, v_unique_key, v_unique_states
    USING p_id;
    GET DIAGNOSTICS v_rows = ROW_COUNT;

    IF v_rows > 0 THEN
        PERFORM awa.release_queue_storage_unique_claim(
            p_id,
            v_unique_key,
            v_unique_states,
            v_state
        );
        RETURN TRUE;
    END IF;

    EXECUTE format(
        'WITH deleted AS (
             DELETE FROM %1$I.leases AS leases
             WHERE job_id = $1
             RETURNING
                 leases.ready_slot,
                 leases.ready_generation,
                 leases.job_id,
                 leases.queue,
                 leases.priority,
                 leases.enqueue_shard,
                 leases.lane_seq,
                 leases.run_lease,
                 leases.state
         ),
         deleted_attempt AS (
             DELETE FROM %1$I.attempt_state AS attempt
             USING deleted
             WHERE attempt.job_id = deleted.job_id
               AND attempt.run_lease = deleted.run_lease
         )
         SELECT
             deleted.queue,
             deleted.priority,
             deleted.state,
             ready.unique_key,
             ready.unique_states
         FROM deleted
         JOIN %1$I.ready_entries AS ready
           ON ready.ready_slot = deleted.ready_slot
          AND ready.ready_generation = deleted.ready_generation
          AND ready.queue = deleted.queue
          AND ready.priority = deleted.priority
          AND ready.enqueue_shard = deleted.enqueue_shard
          AND ready.lane_seq = deleted.lane_seq
          AND ready.job_id = deleted.job_id',
        v_schema
    )
    INTO v_queue, v_priority, v_state, v_unique_key, v_unique_states
    USING p_id;
    GET DIAGNOSTICS v_rows = ROW_COUNT;

    IF v_rows > 0 THEN
        PERFORM awa.release_queue_storage_unique_claim(
            p_id,
            v_unique_key,
            v_unique_states,
            v_state
        );
        RETURN TRUE;
    END IF;

    EXECUTE format(
        'DELETE FROM %I.done_entries
         WHERE job_id = $1
         RETURNING queue, priority, state, unique_key, unique_states,
                   ready_slot, ready_generation, enqueue_shard',
        v_schema
    )
    INTO v_queue, v_priority, v_state, v_unique_key, v_unique_states,
         v_ready_slot, v_ready_generation, v_enqueue_shard
    USING p_id;
    GET DIAGNOSTICS v_rows = ROW_COUNT;

    IF v_rows > 0 THEN
        IF to_regclass(format('%I.queue_terminal_count_deltas', v_schema)) IS NOT NULL THEN
            EXECUTE format(
                'INSERT INTO %I.queue_terminal_count_deltas (
                     ready_slot,
                     ready_generation,
                     queue,
                     priority,
                     enqueue_shard,
                     counter_bucket,
                     terminal_delta
                 )
                 VALUES (
                     $1,
                     $2,
                     $3,
                     $4,
                     $5,
                     mod(mod($6, 256::bigint) + 256::bigint, 256::bigint)::smallint,
                     -1
                 )',
                v_schema
            )
            USING v_ready_slot, v_ready_generation, v_queue, v_priority, v_enqueue_shard, p_id;
        ELSIF to_regclass(format('%I.queue_terminal_live_counts', v_schema)) IS NOT NULL THEN
            EXECUTE format(
                'UPDATE %I.queue_terminal_live_counts AS counts
                 SET live_terminal_count = GREATEST(0, counts.live_terminal_count - 1)
                 WHERE counts.ready_slot     = $1
                   AND counts.queue          = $2
                   AND counts.priority       = $3
                   AND counts.enqueue_shard  = $4
                   AND counts.counter_bucket = mod(mod($5, 256::bigint) + 256::bigint, 256::bigint)::smallint',
                v_schema
            )
            USING v_ready_slot, v_queue, v_priority, v_enqueue_shard, p_id;
        END IF;
        PERFORM awa.release_queue_storage_unique_claim(
            p_id,
            v_unique_key,
            v_unique_states,
            v_state
        );
        RETURN TRUE;
    END IF;

    EXECUTE format(
        'DELETE FROM %I.dlq_entries
         WHERE job_id = $1
         RETURNING queue, priority, state, unique_key, unique_states',
        v_schema
    )
    INTO v_queue, v_priority, v_state, v_unique_key, v_unique_states
    USING p_id;
    GET DIAGNOSTICS v_rows = ROW_COUNT;

    IF v_rows > 0 THEN
        PERFORM awa.release_queue_storage_unique_claim(
            p_id,
            v_unique_key,
            v_unique_states,
            v_state
        );
        RETURN TRUE;
    END IF;

    RETURN FALSE;
END;
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (30, 'Add terminal-count delta ledger and async rollup')
ON CONFLICT (version) DO NOTHING;

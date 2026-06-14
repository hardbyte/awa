-- Compact successful receipt completions.
--
-- Successful short-job completions no longer need a wide done_entries row or
-- per-job lease_claim_closures row as receipt-closure evidence. The hot path
-- stores terminal history in receipt_completion_batches and claim-local
-- closure evidence in lease_claim_closure_batches. terminal_jobs expands the
-- terminal batches for public reads. Cold SQL-compatible deletes of synthesized
-- completed rows append receipt_completion_tombstones.

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
END;
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
    v_run_lease BIGINT;
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
                   ready_slot, ready_generation, enqueue_shard, run_lease',
        v_schema
    )
    INTO v_queue, v_priority, v_state, v_unique_key, v_unique_states,
         v_ready_slot, v_ready_generation, v_enqueue_shard, v_run_lease
    USING p_id;
    GET DIAGNOSTICS v_rows = ROW_COUNT;

    IF v_rows > 0 THEN
        EXECUTE format(
            'WITH inserted AS (
                 INSERT INTO %1$I.lease_claim_closures (
                     claim_slot,
                     job_id,
                     run_lease,
                     outcome,
                     closed_at
                 )
                 SELECT
                     claims.claim_slot,
                     claims.job_id,
                     claims.run_lease,
                     ''terminal_removed'',
                     clock_timestamp()
                 FROM %1$I.lease_claims AS claims
                 WHERE claims.job_id = $1
                   AND claims.run_lease = $2
                 ON CONFLICT (claim_slot, job_id, run_lease) DO NOTHING
                 RETURNING claim_slot, job_id, run_lease, closed_at
             ),
             marked AS (
                 UPDATE %1$I.lease_claims AS claims
                 SET closed_at = COALESCE(claims.closed_at, inserted.closed_at)
                 FROM inserted
                 WHERE claims.claim_slot = inserted.claim_slot
                   AND claims.job_id = inserted.job_id
                   AND claims.run_lease = inserted.run_lease
                 RETURNING claims.job_id
             )
             SELECT count(*) FROM marked',
            v_schema
        )
        USING p_id, v_run_lease;

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
        'WITH target AS (
             SELECT
                 batch.ready_slot,
                 batch.ready_generation,
                 batch.queue,
                 batch.priority,
                 batch.enqueue_shard,
                 item.lane_seq,
                 item.job_id,
                 item.run_lease,
                 ready.unique_key,
                 ready.unique_states
             FROM %1$I.receipt_completion_batches AS batch
             CROSS JOIN LATERAL unnest(
                 batch.job_ids,
                 batch.run_leases,
                 batch.lane_seqs
             ) AS item(job_id, run_lease, lane_seq)
             JOIN %1$I.ready_entries AS ready
               ON ready.ready_slot = batch.ready_slot
              AND ready.ready_generation = batch.ready_generation
              AND ready.queue = batch.queue
              AND ready.priority = batch.priority
              AND ready.enqueue_shard = batch.enqueue_shard
              AND ready.lane_seq = item.lane_seq
              AND ready.job_id = item.job_id
             WHERE item.job_id = $1
               AND NOT EXISTS (
                   SELECT 1
                   FROM %1$I.receipt_completion_tombstones AS tomb
                   WHERE tomb.ready_slot = batch.ready_slot
                     AND tomb.ready_generation = batch.ready_generation
                     AND tomb.job_id = item.job_id
                     AND tomb.run_lease = item.run_lease
               )
             ORDER BY batch.finalized_at DESC
             LIMIT 1
         ),
         inserted AS (
             INSERT INTO %1$I.receipt_completion_tombstones (
                 ready_slot,
                 ready_generation,
                 job_id,
                 run_lease,
                 queue,
                 priority,
                 enqueue_shard,
                 lane_seq
             )
             SELECT
                 ready_slot,
                 ready_generation,
                 job_id,
                 run_lease,
                 queue,
                 priority,
                 enqueue_shard,
                 lane_seq
             FROM target
             ON CONFLICT (ready_slot, job_id, run_lease) DO NOTHING
             RETURNING ready_slot, ready_generation, job_id, run_lease, queue, priority, enqueue_shard
         )
         SELECT
             inserted.queue,
             inserted.priority,
             ''completed''::awa.job_state,
             target.unique_key,
             target.unique_states,
             inserted.ready_slot,
             inserted.ready_generation,
             inserted.enqueue_shard,
             inserted.run_lease
         FROM inserted
         JOIN target USING (ready_slot, ready_generation, job_id, run_lease)',
        v_schema
    )
    INTO v_queue, v_priority, v_state, v_unique_key, v_unique_states,
         v_ready_slot, v_ready_generation, v_enqueue_shard, v_run_lease
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
VALUES (36, 'Compact successful receipt completions into batch terminal history')
ON CONFLICT (version) DO NOTHING;

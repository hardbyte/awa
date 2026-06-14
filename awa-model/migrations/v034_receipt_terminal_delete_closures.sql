-- If successful receipt completion uses done_entries as its closure evidence,
-- compatibility deletes must materialize an explicit receipt closure before
-- deleting that terminal row. Otherwise the claim-ring prune can be pinned by a
-- claim whose only evidence was just removed.

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
            'INSERT INTO %1$I.lease_claim_closures (
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
             ON CONFLICT (claim_slot, job_id, run_lease) DO NOTHING',
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
VALUES (34, 'Materialize receipt closures before terminal compatibility deletes')
ON CONFLICT (version) DO NOTHING;

-- Add the ready-tombstone ledger to every installed queue-storage substrate.
--
-- This migration refreshes the queue storage substrate to include
-- `ready_tombstones` and modifies the `claim_ready_runtime()`
-- function to anti-join against it and advance the claim cursor
-- over tombstoned lanes.

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
END
$$;

CREATE OR REPLACE FUNCTION awa.jobs_compat()
RETURNS TABLE (
    id BIGINT,
    kind TEXT,
    queue TEXT,
    args JSONB,
    state awa.job_state,
    priority SMALLINT,
    attempt SMALLINT,
    max_attempts SMALLINT,
    run_at TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ,
    deadline_at TIMESTAMPTZ,
    attempted_at TIMESTAMPTZ,
    finalized_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ,
    errors JSONB[],
    metadata JSONB,
    tags TEXT[],
    unique_key BYTEA,
    unique_states BIT(8),
    callback_id UUID,
    callback_timeout_at TIMESTAMPTZ,
    callback_filter TEXT,
    callback_on_complete TEXT,
    callback_on_fail TEXT,
    callback_transform TEXT,
    run_lease BIGINT,
    progress JSONB
)
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog, awa, public
AS $$
DECLARE
    v_schema TEXT;
BEGIN
    v_schema := awa.active_queue_storage_schema();

    IF v_schema IS NULL THEN
        RETURN QUERY
        SELECT
            j.id,
            j.kind,
            j.queue,
            j.args,
            j.state,
            j.priority,
            j.attempt,
            j.max_attempts,
            j.run_at,
            j.heartbeat_at,
            j.deadline_at,
            j.attempted_at,
            j.finalized_at,
            j.created_at,
            j.errors,
            j.metadata,
            j.tags,
            j.unique_key,
            j.unique_states,
            j.callback_id,
            j.callback_timeout_at,
            j.callback_filter,
            j.callback_on_complete,
            j.callback_on_fail,
            j.callback_transform,
            j.run_lease,
            j.progress
        FROM awa.jobs_hot AS j
        UNION ALL
        SELECT
            j.id,
            j.kind,
            j.queue,
            j.args,
            j.state,
            j.priority,
            j.attempt,
            j.max_attempts,
            j.run_at,
            j.heartbeat_at,
            j.deadline_at,
            j.attempted_at,
            j.finalized_at,
            j.created_at,
            j.errors,
            j.metadata,
            j.tags,
            j.unique_key,
            j.unique_states,
            j.callback_id,
            j.callback_timeout_at,
            j.callback_filter,
            j.callback_on_complete,
            j.callback_on_fail,
            j.callback_transform,
            j.run_lease,
            j.progress
        FROM awa.scheduled_jobs AS j;
        RETURN;
    END IF;

    RETURN QUERY EXECUTE format(
        $sql$
        WITH current_available AS (
            SELECT
                ready.job_id AS id,
                ready.kind,
                ready.queue,
                ready.args,
                'available'::awa.job_state AS state,
                ready.priority,
                ready.attempt,
                ready.max_attempts,
                ready.run_at,
                NULL::timestamptz AS heartbeat_at,
                NULL::timestamptz AS deadline_at,
                ready.attempted_at,
                NULL::timestamptz AS finalized_at,
                ready.created_at,
                awa.queue_storage_payload_errors(ready.payload) AS errors,
                COALESCE(NULLIF(ready.payload->'metadata', 'null'::jsonb), '{}'::jsonb) AS metadata,
                awa.queue_storage_payload_tags(ready.payload) AS tags,
                ready.unique_key,
                CASE
                    WHEN ready.unique_states IS NULL THEN NULL::bit(8)
                    ELSE ready.unique_states::bit(8)
                END AS unique_states,
                NULL::uuid AS callback_id,
                NULL::timestamptz AS callback_timeout_at,
                NULL::text AS callback_filter,
                NULL::text AS callback_on_complete,
                NULL::text AS callback_on_fail,
                NULL::text AS callback_transform,
                ready.run_lease,
                NULLIF(ready.payload->'progress', 'null'::jsonb) AS progress
            FROM %1$I.ready_entries AS ready
            JOIN %1$I.queue_claim_heads AS claims
              ON claims.queue = ready.queue
             AND claims.priority = ready.priority
             AND claims.enqueue_shard = ready.enqueue_shard
            WHERE ready.lane_seq >= %1$I.sequence_next_value(claims.seq_name)
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
        )
        SELECT
            current_available.id,
            current_available.kind,
            current_available.queue,
            current_available.args,
            current_available.state,
            current_available.priority,
            current_available.attempt,
            current_available.max_attempts,
            current_available.run_at,
            current_available.heartbeat_at,
            current_available.deadline_at,
            current_available.attempted_at,
            current_available.finalized_at,
            current_available.created_at,
            current_available.errors,
            current_available.metadata,
            current_available.tags,
            current_available.unique_key,
            current_available.unique_states,
            current_available.callback_id,
            current_available.callback_timeout_at,
            current_available.callback_filter,
            current_available.callback_on_complete,
            current_available.callback_on_fail,
            current_available.callback_transform,
            current_available.run_lease,
            current_available.progress
        FROM current_available
        UNION ALL
        SELECT
            deferred.job_id AS id,
            deferred.kind,
            deferred.queue,
            deferred.args,
            deferred.state,
            deferred.priority,
            deferred.attempt,
            deferred.max_attempts,
            deferred.run_at,
            NULL::timestamptz AS heartbeat_at,
            NULL::timestamptz AS deadline_at,
            deferred.attempted_at,
            deferred.finalized_at,
            deferred.created_at,
            awa.queue_storage_payload_errors(deferred.payload) AS errors,
            COALESCE(NULLIF(deferred.payload->'metadata', 'null'::jsonb), '{}'::jsonb) AS metadata,
            awa.queue_storage_payload_tags(deferred.payload) AS tags,
            deferred.unique_key,
            CASE
                WHEN deferred.unique_states IS NULL THEN NULL::bit(8)
                ELSE deferred.unique_states::bit(8)
            END AS unique_states,
            NULL::uuid AS callback_id,
            NULL::timestamptz AS callback_timeout_at,
            NULL::text AS callback_filter,
            NULL::text AS callback_on_complete,
            NULL::text AS callback_on_fail,
            NULL::text AS callback_transform,
            deferred.run_lease,
            NULLIF(deferred.payload->'progress', 'null'::jsonb) AS progress
        FROM %1$I.deferred_jobs AS deferred
        UNION ALL
        SELECT
            leases.job_id AS id,
            ready.kind,
            ready.queue,
            ready.args,
            leases.state,
            leases.priority,
            leases.attempt,
            leases.max_attempts,
            ready.run_at,
            leases.heartbeat_at,
            leases.deadline_at,
            leases.attempted_at,
            NULL::timestamptz AS finalized_at,
            ready.created_at,
            awa.queue_storage_payload_errors(ready.payload) AS errors,
            CASE
                WHEN attempt.callback_result IS NULL
                    THEN COALESCE(NULLIF(ready.payload->'metadata', 'null'::jsonb), '{}'::jsonb)
                ELSE COALESCE(NULLIF(ready.payload->'metadata', 'null'::jsonb), '{}'::jsonb)
                    || jsonb_build_object('_awa_callback_result', attempt.callback_result)
            END AS metadata,
            awa.queue_storage_payload_tags(ready.payload) AS tags,
            ready.unique_key,
            CASE
                WHEN ready.unique_states IS NULL THEN NULL::bit(8)
                ELSE ready.unique_states::bit(8)
            END AS unique_states,
            leases.callback_id,
            leases.callback_timeout_at,
            attempt.callback_filter,
            attempt.callback_on_complete,
            attempt.callback_on_fail,
            attempt.callback_transform,
            leases.run_lease,
            COALESCE(
                NULLIF(attempt.progress, 'null'::jsonb),
                NULLIF(ready.payload->'progress', 'null'::jsonb)
            ) AS progress
        FROM %1$I.leases AS leases
        JOIN %1$I.ready_entries AS ready
          ON ready.ready_slot = leases.ready_slot
         AND ready.ready_generation = leases.ready_generation
         AND ready.queue = leases.queue
         AND ready.priority = leases.priority
         AND ready.enqueue_shard = leases.enqueue_shard
         AND ready.lane_seq = leases.lane_seq
        LEFT JOIN %1$I.attempt_state AS attempt
          ON attempt.job_id = leases.job_id
         AND attempt.run_lease = leases.run_lease
        UNION ALL
        SELECT
            done.job_id AS id,
            done.kind,
            done.queue,
            done.args,
            done.state,
            done.priority,
            done.attempt,
            done.max_attempts,
            done.run_at,
            NULL::timestamptz AS heartbeat_at,
            NULL::timestamptz AS deadline_at,
            done.attempted_at,
            done.finalized_at,
            done.created_at,
            awa.queue_storage_payload_errors(done.payload) AS errors,
            COALESCE(NULLIF(done.payload->'metadata', 'null'::jsonb), '{}'::jsonb) AS metadata,
            awa.queue_storage_payload_tags(done.payload) AS tags,
            done.unique_key,
            CASE
                WHEN done.unique_states IS NULL THEN NULL::bit(8)
                ELSE done.unique_states::bit(8)
            END AS unique_states,
            NULL::uuid AS callback_id,
            NULL::timestamptz AS callback_timeout_at,
            NULL::text AS callback_filter,
            NULL::text AS callback_on_complete,
            NULL::text AS callback_on_fail,
            NULL::text AS callback_transform,
            done.run_lease,
            NULLIF(done.payload->'progress', 'null'::jsonb) AS progress
        FROM %1$I.terminal_jobs AS done
        UNION ALL
        SELECT
            dlq.job_id AS id,
            dlq.kind,
            dlq.queue,
            dlq.args,
            dlq.state,
            dlq.priority,
            dlq.attempt,
            dlq.max_attempts,
            dlq.run_at,
            NULL::timestamptz AS heartbeat_at,
            NULL::timestamptz AS deadline_at,
            dlq.attempted_at,
            dlq.finalized_at,
            dlq.created_at,
            awa.queue_storage_payload_errors(dlq.payload) AS errors,
            COALESCE(NULLIF(dlq.payload->'metadata', 'null'::jsonb), '{}'::jsonb) AS metadata,
            awa.queue_storage_payload_tags(dlq.payload) AS tags,
            dlq.unique_key,
            CASE
                WHEN dlq.unique_states IS NULL THEN NULL::bit(8)
                ELSE dlq.unique_states::bit(8)
            END AS unique_states,
            NULL::uuid AS callback_id,
            NULL::timestamptz AS callback_timeout_at,
            NULL::text AS callback_filter,
            NULL::text AS callback_on_complete,
            NULL::text AS callback_on_fail,
            NULL::text AS callback_transform,
            dlq.run_lease,
            NULLIF(dlq.payload->'progress', 'null'::jsonb) AS progress
        FROM %1$I.dlq_entries AS dlq
        $sql$,
        v_schema
    );
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
                   ready_slot, enqueue_shard',
        v_schema
    )
    INTO v_queue, v_priority, v_state, v_unique_key, v_unique_states,
         v_ready_slot, v_enqueue_shard
    USING p_id;
    GET DIAGNOSTICS v_rows = ROW_COUNT;

    IF v_rows > 0 THEN
        IF to_regclass(format('%I.queue_terminal_live_counts', v_schema)) IS NOT NULL THEN
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
VALUES (28, 'Add ready_tombstones ledger and compatibility filters (#295)')
ON CONFLICT (version) DO NOTHING;

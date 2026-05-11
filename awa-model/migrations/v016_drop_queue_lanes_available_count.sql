-- v016: Drop the redundant available_count cache from queue_lanes.
--
-- queue_lanes.available_count was a denormalised cache of
-- `queue_enqueue_heads.next_seq - queue_claim_heads.claim_seq` for
-- the same (queue, priority). The two head tables already track
-- every enqueue and claim independently, so the cache was a third
-- counter that had to be UPDATEd on every enqueue, claim, and
-- completion batch — at a multiple of its siblings' write rate.
-- Under a pinned xmin (long-running reader transaction) this made
-- queue_lanes the dominant source of heap bloat in the schema:
-- live rows ≈ 16, dead tuples in the tens of thousands within
-- minutes, with a measured throughput drop while the column
-- existed.
--
-- This migration removes the column. Readers derive the count
-- on demand from the head-table difference. The compat functions
-- (`insert_job_compat`, `delete_job_compat`) are replaced to drop
-- their `available_count` mutations; their signatures stay
-- byte-identical to v013's so CREATE OR REPLACE substitutes the
-- body rather than installing a parallel overload.
--
-- Admin-side DELETE of an unclaimed row leaves the derived count
-- transiently high (by 1 per deleted row): `next_seq` reflects the
-- enqueue and `claim_seq` has not advanced. delete_job_compat
-- advances claim_seq when the deleted lane is exactly at the head;
-- non-head deletes leave a gap that the dispatcher's gap-recovery
-- branch in `claim_ready_runtime` absorbs on the next claim
-- attempt that finds no rows at the head.

DO $$
DECLARE
    v_schema TEXT;
BEGIN
    SELECT schema_name INTO v_schema
    FROM awa.runtime_storage_backends
    WHERE backend = 'queue_storage'
    LIMIT 1;

    IF v_schema IS NOT NULL
       AND to_regclass(format('%I.%I', v_schema, 'queue_lanes')) IS NOT NULL THEN
        EXECUTE format(
            'ALTER TABLE %I.queue_lanes DROP COLUMN IF EXISTS available_count',
            v_schema
        );
    END IF;
END
$$ LANGUAGE plpgsql;

-- insert_job_compat: signature byte-identical to v013, body minus
-- the queue_lanes.available_count mutation. The enqueue's
-- contribution to the available count is now captured solely by
-- the queue_enqueue_heads.next_seq advance below.
CREATE OR REPLACE FUNCTION awa.insert_job_compat(
    p_kind TEXT,
    p_queue TEXT DEFAULT 'default',
    p_args JSONB DEFAULT '{}'::jsonb,
    p_state awa.job_state DEFAULT 'available',
    p_priority SMALLINT DEFAULT 2,
    p_max_attempts SMALLINT DEFAULT 25,
    p_run_at TIMESTAMPTZ DEFAULT NULL,
    p_metadata JSONB DEFAULT '{}'::jsonb,
    p_tags TEXT[] DEFAULT ARRAY[]::TEXT[],
    p_unique_key BYTEA DEFAULT NULL,
    p_unique_states BIT(8) DEFAULT NULL
)
RETURNS awa.jobs
AS $$
DECLARE
    v_schema TEXT;
    v_queue TEXT := COALESCE(p_queue, 'default');
    v_args JSONB := COALESCE(p_args, '{}'::jsonb);
    v_state awa.job_state := COALESCE(p_state, 'available'::awa.job_state);
    v_priority SMALLINT := COALESCE(p_priority, 2);
    v_max_attempts SMALLINT := COALESCE(p_max_attempts, 25);
    v_run_at TIMESTAMPTZ := COALESCE(p_run_at, clock_timestamp());
    v_metadata JSONB := COALESCE(p_metadata, '{}'::jsonb);
    v_tags TEXT[] := COALESCE(p_tags, '{}'::text[]);
    v_created_at TIMESTAMPTZ := clock_timestamp();
    v_job_id BIGINT;
    v_ready_slot INT;
    v_ready_generation BIGINT;
    v_lane_seq BIGINT;
    v_payload JSONB;
    v_unique_states_text TEXT := CASE
        WHEN p_unique_states IS NULL THEN NULL
        ELSE p_unique_states::TEXT
    END;
    v_old_search_path TEXT;
    inserted awa.jobs%ROWTYPE;
BEGIN
    IF length(p_kind) > 200 THEN
        RAISE EXCEPTION 'job kind length must be <= 200 characters'
            USING ERRCODE = '23514';
    END IF;

    IF length(v_queue) > 200 THEN
        RAISE EXCEPTION 'queue name length must be <= 200 characters'
            USING ERRCODE = '23514';
    END IF;

    IF v_priority < 1 OR v_priority > 4 THEN
        RAISE EXCEPTION 'priority must be between 1 and 4'
            USING ERRCODE = '23514';
    END IF;

    IF v_max_attempts < 1 OR v_max_attempts > 1000 THEN
        RAISE EXCEPTION 'max_attempts must be between 1 and 1000'
            USING ERRCODE = '23514';
    END IF;

    IF cardinality(v_tags) > 20 THEN
        RAISE EXCEPTION 'job tags must contain at most 20 values'
            USING ERRCODE = '23514';
    END IF;

    v_schema := awa.active_queue_storage_schema();

    IF v_schema IS NULL THEN
        IF v_state IN ('scheduled'::awa.job_state, 'retryable'::awa.job_state) THEN
            INSERT INTO awa.scheduled_jobs AS jobs (
                kind,
                queue,
                args,
                state,
                priority,
                max_attempts,
                run_at,
                metadata,
                tags,
                unique_key,
                unique_states
            )
            VALUES (
                p_kind,
                v_queue,
                v_args,
                v_state,
                v_priority,
                v_max_attempts,
                v_run_at,
                v_metadata,
                v_tags,
                p_unique_key,
                p_unique_states
            )
            RETURNING * INTO inserted;
            RETURN inserted;
        END IF;

        INSERT INTO awa.jobs_hot AS jobs (
            kind,
            queue,
            args,
            state,
            priority,
            max_attempts,
            run_at,
            metadata,
            tags,
            unique_key,
            unique_states
        )
        VALUES (
            p_kind,
            v_queue,
            v_args,
            v_state,
            v_priority,
            v_max_attempts,
            v_run_at,
            v_metadata,
            v_tags,
            p_unique_key,
            p_unique_states
        )
        RETURNING * INTO inserted;
        RETURN inserted;
    END IF;

    IF v_state NOT IN (
        'available'::awa.job_state,
        'scheduled'::awa.job_state,
        'retryable'::awa.job_state
    ) THEN
        RAISE EXCEPTION 'queue storage does not support initial state %', v_state
            USING ERRCODE = '22023';
    END IF;

    v_old_search_path := current_setting('search_path');
    PERFORM set_config('search_path', format('%I,awa,public', v_schema), true);

    SELECT nextval(format('%I.job_id_seq', v_schema)::regclass)::bigint
    INTO v_job_id;

    IF p_unique_key IS NOT NULL
        AND p_unique_states IS NOT NULL
        AND awa.job_state_in_bitmask(p_unique_states, v_state)
    THEN
        INSERT INTO awa.job_unique_claims (unique_key, job_id)
        VALUES (p_unique_key, v_job_id);
    END IF;

    v_payload := jsonb_build_object(
        'metadata',
        v_metadata,
        'tags',
        to_jsonb(v_tags),
        'errors',
        '[]'::jsonb,
        'progress',
        NULL
    );

    IF v_state = 'available'::awa.job_state THEN
        INSERT INTO queue_lanes (queue, priority)
        VALUES (v_queue, v_priority)
        ON CONFLICT ON CONSTRAINT queue_lanes_pkey DO NOTHING;

        INSERT INTO queue_enqueue_heads (queue, priority)
        VALUES (v_queue, v_priority)
        ON CONFLICT (queue, priority) DO NOTHING;

        INSERT INTO queue_claim_heads (queue, priority)
        VALUES (v_queue, v_priority)
        ON CONFLICT (queue, priority) DO NOTHING;

        UPDATE queue_enqueue_heads AS heads
        SET next_seq = heads.next_seq + 1
        WHERE heads.queue = v_queue
          AND heads.priority = v_priority
        RETURNING heads.next_seq - 1
        INTO v_lane_seq;

        SELECT ring.current_slot, ring.generation
        INTO v_ready_slot, v_ready_generation
        FROM queue_ring_state AS ring
        WHERE ring.singleton = TRUE;

        INSERT INTO ready_entries (
            ready_slot,
            ready_generation,
            job_id,
            kind,
            queue,
            args,
            priority,
            attempt,
            run_lease,
            max_attempts,
            lane_seq,
            run_at,
            attempted_at,
            created_at,
            unique_key,
            unique_states,
            payload
        ) VALUES (
            v_ready_slot,
            v_ready_generation,
            v_job_id,
            p_kind,
            v_queue,
            v_args,
            v_priority,
            0,
            0,
            v_max_attempts,
            v_lane_seq,
            v_run_at,
            NULL,
            v_created_at,
            p_unique_key,
            v_unique_states_text,
            v_payload
        );

        PERFORM pg_notify('awa:' || v_queue, '');
        PERFORM set_config('search_path', v_old_search_path, true);

        SELECT * INTO inserted
        FROM awa.jobs AS jobs
        WHERE jobs.id = v_job_id;

        RETURN inserted;
    END IF;

    INSERT INTO deferred_jobs (
        job_id,
        kind,
        queue,
        args,
        state,
        priority,
        attempt,
        run_lease,
        max_attempts,
        run_at,
        attempted_at,
        finalized_at,
        created_at,
        unique_key,
        unique_states,
        payload
    ) VALUES (
        v_job_id,
        p_kind,
        v_queue,
        v_args,
        v_state,
        v_priority,
        0,
        0,
        v_max_attempts,
        v_run_at,
        NULL,
        NULL,
        v_created_at,
        p_unique_key,
        v_unique_states_text,
        v_payload
    );

    PERFORM set_config('search_path', v_old_search_path, true);

    SELECT * INTO inserted
    FROM awa.jobs AS jobs
    WHERE jobs.id = v_job_id;

    RETURN inserted;
END;
$$ LANGUAGE plpgsql VOLATILE
SET search_path = pg_catalog, awa, public;

-- delete_job_compat: signature unchanged from v013, body adjusted
-- so that admin-side deletion of an unclaimed row keeps the
-- derived `next_seq - claim_seq` count honest:
--   * head-lane delete → advance claim_seq past the deleted row;
--   * non-head delete  → leave a gap; the dispatcher's gap-recovery
--     branch in claim_ready_runtime closes it on the next attempt
--     that finds no rows at the head.
CREATE OR REPLACE FUNCTION awa.delete_job_compat(p_id BIGINT)
RETURNS BOOLEAN AS $$
DECLARE
    v_schema TEXT;
    v_queue TEXT;
    v_priority SMALLINT;
    v_lane_seq BIGINT;
    v_state awa.job_state;
    v_unique_key BYTEA;
    v_unique_states TEXT;
    v_rows INT;
BEGIN
    v_schema := awa.active_queue_storage_schema();

    IF v_schema IS NULL THEN
        RAISE EXCEPTION 'queue storage is not active'
            USING ERRCODE = '55000';
    END IF;

    EXECUTE format(
        'DELETE FROM %I.ready_entries
         WHERE job_id = $1
         RETURNING queue, priority, lane_seq, ''available''::awa.job_state, unique_key, unique_states',
        v_schema
    )
    INTO v_queue, v_priority, v_lane_seq, v_state, v_unique_key, v_unique_states
    USING p_id;
    GET DIAGNOSTICS v_rows = ROW_COUNT;

    IF v_rows > 0 THEN
        -- Keep the derived `next_seq - claim_seq` count honest when
        -- the deleted lane was *exactly* at the claim head: advance
        -- claim_seq past it. Non-head deletes leave a gap that the
        -- gap-recovery branch in claim_ready_runtime closes.
        EXECUTE format(
            'UPDATE %I.queue_claim_heads
             SET claim_seq = claim_seq + 1
             WHERE queue = $1
               AND priority = $2
               AND claim_seq = $3',
            v_schema
        )
        USING v_queue, v_priority, v_lane_seq;
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
          AND ready.lane_seq = deleted.lane_seq',
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
$$ LANGUAGE plpgsql
SET search_path = pg_catalog, awa, public;

INSERT INTO awa.schema_version (version, description)
VALUES (16, 'Drop redundant queue_lanes.available_count cache; reader derives from heads')
ON CONFLICT (version) DO NOTHING;

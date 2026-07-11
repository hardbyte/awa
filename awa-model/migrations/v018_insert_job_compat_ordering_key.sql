-- v018: Ensure upgraded databases get the ordering-key aware
-- insert_job_compat signature. Later queue-storage migrations re-apply this
-- function too, so it also carries compatibility fixes for active queue-storage
-- producers while preserving a fallback for historical migration replay before
-- reserve_enqueue_seq exists.

DROP FUNCTION IF EXISTS awa.insert_job_compat(
    TEXT, TEXT, JSONB, awa.job_state, SMALLINT, SMALLINT,
    TIMESTAMPTZ, JSONB, TEXT[], BYTEA, BIT(8)
);

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
    p_unique_states BIT(8) DEFAULT NULL,
    p_ordering_key BYTEA DEFAULT NULL
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
    v_enqueue_shards SMALLINT;
    v_enqueue_shard SMALLINT;
    v_hash NUMERIC;
    v_i INT;
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
        SELECT COALESCE(meta.enqueue_shards, 1)
        INTO v_enqueue_shards
        FROM awa.queue_meta AS meta
        WHERE meta.queue = v_queue;
        v_enqueue_shards := COALESCE(v_enqueue_shards, 1);

        IF p_ordering_key IS NOT NULL THEN
            v_hash := 14695981039346656037;
            IF length(p_ordering_key) > 0 THEN
                FOR v_i IN 0..length(p_ordering_key) - 1 LOOP
                    v_hash := MOD(
                        (v_hash * 1099511628211) + get_byte(p_ordering_key, v_i),
                        18446744073709551616
                    );
                END LOOP;
            END IF;
            v_enqueue_shard := MOD(v_hash, v_enqueue_shards)::smallint;
        ELSE
            v_enqueue_shard := MOD(pg_backend_pid()::bigint, v_enqueue_shards)::smallint;
        END IF;

        INSERT INTO queue_lanes (queue, priority)
        VALUES (v_queue, v_priority)
        ON CONFLICT ON CONSTRAINT queue_lanes_pkey DO NOTHING;

        INSERT INTO queue_enqueue_heads (queue, priority, enqueue_shard)
        VALUES (v_queue, v_priority, v_enqueue_shard)
        ON CONFLICT (queue, priority, enqueue_shard) DO NOTHING;

        INSERT INTO queue_claim_heads (queue, priority, enqueue_shard)
        VALUES (v_queue, v_priority, v_enqueue_shard)
        ON CONFLICT (queue, priority, enqueue_shard) DO NOTHING;

        IF to_regprocedure(format(
            '%I.reserve_enqueue_seq(text,smallint,smallint,bigint)',
            v_schema
        )) IS NOT NULL THEN
            EXECUTE format(
                'SELECT %I.reserve_enqueue_seq($1, $2, $3, $4)',
                v_schema
            )
            INTO v_lane_seq
            USING v_queue, v_priority, v_enqueue_shard, 1::bigint;
        ELSE
            UPDATE queue_enqueue_heads AS heads
            SET next_seq = heads.next_seq + 1
            WHERE heads.queue = v_queue
              AND heads.priority = v_priority
              AND heads.enqueue_shard = v_enqueue_shard
            RETURNING heads.next_seq - 1
            INTO v_lane_seq;
        END IF;

        -- #371: the queue-ring cursor is the max-generation row of the
        -- append-only rotation ledger (resolved against the active
        -- queue-storage schema via the search_path set above).
        SELECT ledger.slot, ledger.generation
        INTO v_ready_slot, v_ready_generation
        FROM queue_ring_rotations AS ledger
        ORDER BY ledger.generation DESC
        LIMIT 1;

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
            enqueue_shard,
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
            v_enqueue_shard,
            v_run_at,
            NULL,
            v_created_at,
            p_unique_key,
            v_unique_states_text,
            v_payload
        );

        IF to_regclass(format('%I.%I', v_schema, 'ready_segments')) IS NOT NULL THEN
            INSERT INTO ready_segments (
                ready_slot,
                ready_generation,
                queue,
                priority,
                enqueue_shard,
                first_lane_seq,
                next_lane_seq,
                first_run_at
            ) VALUES (
                v_ready_slot,
                v_ready_generation,
                v_queue,
                v_priority,
                v_enqueue_shard,
                v_lane_seq,
                v_lane_seq + 1,
                v_run_at
            )
            ON CONFLICT (ready_slot, ready_generation, queue, priority, enqueue_shard, first_lane_seq) DO NOTHING;
        END IF;

        PERFORM set_config('search_path', v_old_search_path, true);

        SELECT
            v_job_id,
            p_kind,
            v_queue,
            v_args,
            v_state,
            v_priority,
            0::smallint,
            v_max_attempts,
            v_run_at,
            NULL::timestamptz,
            NULL::timestamptz,
            NULL::timestamptz,
            NULL::timestamptz,
            v_created_at,
            ARRAY[]::jsonb[],
            v_metadata,
            v_tags,
            p_unique_key,
            p_unique_states,
            NULL::uuid,
            NULL::timestamptz,
            NULL::text,
            NULL::text,
            NULL::text,
            NULL::text,
            0::bigint,
            NULL::jsonb
        INTO inserted;

        PERFORM pg_notify('awa:' || v_queue, '');
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

    SELECT
        v_job_id,
        p_kind,
        v_queue,
        v_args,
        v_state,
        v_priority,
        0::smallint,
        v_max_attempts,
        v_run_at,
        NULL::timestamptz,
        NULL::timestamptz,
        NULL::timestamptz,
        NULL::timestamptz,
        v_created_at,
        ARRAY[]::jsonb[],
        v_metadata,
        v_tags,
        p_unique_key,
        p_unique_states,
        NULL::uuid,
        NULL::timestamptz,
        NULL::text,
        NULL::text,
        NULL::text,
        NULL::text,
        0::bigint,
        NULL::jsonb
    INTO inserted;

    RETURN inserted;
END;
$$ LANGUAGE plpgsql VOLATILE
SET search_path = pg_catalog, awa, public;

INSERT INTO awa.schema_version (version, description)
VALUES (
    18,
    'Thread ordering_key through insert_job_compat for queue-storage producers'
)
ON CONFLICT (version) DO NOTHING;

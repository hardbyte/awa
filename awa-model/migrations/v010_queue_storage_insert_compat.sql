CREATE TABLE IF NOT EXISTS awa.runtime_storage_backends (
    backend     TEXT PRIMARY KEY,
    schema_name TEXT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION awa.active_queue_storage_schema()
RETURNS TEXT AS $$
DECLARE
    schema_name TEXT;
BEGIN
    IF to_regclass('awa.runtime_storage_backends') IS NULL THEN
        RETURN NULL;
    END IF;

    SELECT rsb.schema_name
    INTO schema_name
    FROM awa.runtime_storage_backends AS rsb
    WHERE rsb.backend = 'queue_storage';

    RETURN schema_name;
END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION awa.insert_job_compat(
    p_kind TEXT,
    p_queue TEXT,
    p_args JSONB,
    p_state awa.job_state,
    p_priority SMALLINT,
    p_max_attempts SMALLINT,
    p_run_at TIMESTAMPTZ,
    p_metadata JSONB,
    p_tags TEXT[],
    p_unique_key BYTEA,
    p_unique_states BIT(8)
)
RETURNS TABLE (
    id BIGINT,
    kind TEXT,
    queue TEXT,
    args JSONB,
    state awa.job_state,
    priority SMALLINT,
    attempt SMALLINT,
    run_lease BIGINT,
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
    callback_id UUID,
    callback_timeout_at TIMESTAMPTZ,
    callback_filter TEXT,
    callback_on_complete TEXT,
    callback_on_fail TEXT,
    callback_transform TEXT,
    progress JSONB
) AS $$
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
            RETURN QUERY
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
            RETURNING
                jobs.id,
                jobs.kind,
                jobs.queue,
                jobs.args,
                jobs.state,
                jobs.priority,
                jobs.attempt,
                jobs.run_lease,
                jobs.max_attempts,
                jobs.run_at,
                jobs.heartbeat_at,
                jobs.deadline_at,
                jobs.attempted_at,
                jobs.finalized_at,
                jobs.created_at,
                jobs.errors,
                jobs.metadata,
                jobs.tags,
                jobs.unique_key,
                jobs.callback_id,
                jobs.callback_timeout_at,
                jobs.callback_filter,
                jobs.callback_on_complete,
                jobs.callback_on_fail,
                jobs.callback_transform,
                jobs.progress;
            RETURN;
        END IF;

        RETURN QUERY
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
        RETURNING
            jobs.id,
            jobs.kind,
            jobs.queue,
            jobs.args,
            jobs.state,
            jobs.priority,
            jobs.attempt,
            jobs.run_lease,
            jobs.max_attempts,
            jobs.run_at,
            jobs.heartbeat_at,
            jobs.deadline_at,
            jobs.attempted_at,
            jobs.finalized_at,
            jobs.created_at,
            jobs.errors,
            jobs.metadata,
            jobs.tags,
            jobs.unique_key,
            jobs.callback_id,
            jobs.callback_timeout_at,
            jobs.callback_filter,
            jobs.callback_on_complete,
            jobs.callback_on_fail,
            jobs.callback_transform,
            jobs.progress;
        RETURN;
    END IF;

    IF v_state NOT IN (
        'available'::awa.job_state,
        'scheduled'::awa.job_state,
        'retryable'::awa.job_state
    ) THEN
        RAISE EXCEPTION 'queue storage does not support initial state %', v_state
            USING ERRCODE = '22023';
    END IF;

    EXECUTE format(
        'SELECT nextval(%L::regclass)::bigint',
        v_schema || '.job_id_seq'
    )
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
        EXECUTE format(
            'INSERT INTO %I.queue_lanes (queue, priority)
             VALUES ($1, $2)
             ON CONFLICT (queue, priority) DO NOTHING',
            v_schema
        )
        USING v_queue, v_priority;

        EXECUTE format(
            'UPDATE %I.queue_lanes
             SET next_seq = next_seq + 1,
                 available_count = available_count + 1
             WHERE queue = $1 AND priority = $2
             RETURNING next_seq - 1',
            v_schema
        )
        INTO v_lane_seq
        USING v_queue, v_priority;

        EXECUTE format(
            'SELECT current_slot, generation
             FROM %I.queue_ring_state
             WHERE singleton = TRUE',
            v_schema
        )
        INTO v_ready_slot, v_ready_generation;

        EXECUTE format(
            'INSERT INTO %I.ready_entries (
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
                 $1, $2, $3, $4, $5, $6, $7, 0, 0, $8, $9, $10, NULL, $11, $12, $13, $14
             )',
            v_schema
        )
        USING
            v_ready_slot,
            v_ready_generation,
            v_job_id,
            p_kind,
            v_queue,
            v_args,
            v_priority,
            v_max_attempts,
            v_lane_seq,
            v_run_at,
            v_created_at,
            p_unique_key,
            v_unique_states_text,
            v_payload;

        PERFORM pg_notify('awa:' || v_queue, '');

        RETURN QUERY
        SELECT
            v_job_id,
            p_kind,
            v_queue,
            v_args,
            v_state,
            v_priority,
            0::SMALLINT,
            0::BIGINT,
            v_max_attempts,
            v_run_at,
            NULL::TIMESTAMPTZ,
            NULL::TIMESTAMPTZ,
            NULL::TIMESTAMPTZ,
            NULL::TIMESTAMPTZ,
            v_created_at,
            NULL::JSONB[],
            v_metadata,
            v_tags,
            p_unique_key,
            NULL::UUID,
            NULL::TIMESTAMPTZ,
            NULL::TEXT,
            NULL::TEXT,
            NULL::TEXT,
            NULL::TEXT,
            NULL::JSONB;
        RETURN;
    END IF;

    EXECUTE format(
        'INSERT INTO %I.deferred_jobs (
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
             $1, $2, $3, $4, $5, $6, 0, 0, $7, $8, NULL, NULL, $9, $10, $11, $12
         )',
        v_schema
    )
    USING
        v_job_id,
        p_kind,
        v_queue,
        v_args,
        v_state,
        v_priority,
        v_max_attempts,
        v_run_at,
        v_created_at,
        p_unique_key,
        v_unique_states_text,
        v_payload;

    RETURN QUERY
    SELECT
        v_job_id,
        p_kind,
        v_queue,
        v_args,
        v_state,
        v_priority,
        0::SMALLINT,
        0::BIGINT,
        v_max_attempts,
        v_run_at,
        NULL::TIMESTAMPTZ,
        NULL::TIMESTAMPTZ,
        NULL::TIMESTAMPTZ,
        NULL::TIMESTAMPTZ,
        v_created_at,
        NULL::JSONB[],
        v_metadata,
        v_tags,
        p_unique_key,
        NULL::UUID,
        NULL::TIMESTAMPTZ,
        NULL::TEXT,
        NULL::TEXT,
        NULL::TEXT,
        NULL::TEXT,
        NULL::JSONB;
END;
$$ LANGUAGE plpgsql VOLATILE;

INSERT INTO awa.schema_version (version, description)
VALUES (10, 'Queue storage compatibility layer and active backend selection')
ON CONFLICT (version) DO NOTHING;

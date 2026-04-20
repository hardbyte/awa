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
$$ LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, awa, public;

CREATE OR REPLACE FUNCTION awa.canonical_live_backlog()
RETURNS BIGINT
LANGUAGE sql
STABLE
SET search_path = pg_catalog, awa
AS $$
    SELECT
        COALESCE((
            SELECT count(*)::bigint
            FROM awa.jobs_hot
            WHERE state NOT IN ('completed', 'failed', 'cancelled')
        ), 0)
        +
        COALESCE((
            SELECT count(*)::bigint
            FROM awa.scheduled_jobs
        ), 0)
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
BEGIN
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

CREATE OR REPLACE FUNCTION awa.storage_enter_mixed_transition()
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
    v_prepared_engine TEXT;
    v_state TEXT;
    v_details JSONB;
    v_schema TEXT;
    v_live_canonical_count BIGINT;
    v_live_queue_storage_count BIGINT;
BEGIN
    SELECT sts.prepared_engine, sts.state, sts.details
    INTO v_prepared_engine, v_state, v_details
    FROM awa.storage_transition_state AS sts
    WHERE sts.singleton
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'storage transition state row is missing'
            USING ERRCODE = '55000';
    END IF;

    IF v_state <> 'prepared' THEN
        RAISE EXCEPTION 'storage enter-mixed-transition is only allowed from prepared state'
            USING ERRCODE = '55000';
    END IF;

    IF v_prepared_engine IS DISTINCT FROM 'queue_storage' THEN
        RAISE EXCEPTION 'prepared engine "%" is not supported by this release', v_prepared_engine
            USING ERRCODE = '22023';
    END IF;

    v_schema := COALESCE(NULLIF(v_details->>'schema', ''), 'awa_exp');

    IF to_regclass(format('%I.%I', v_schema, 'queue_ring_state')) IS NULL
        OR to_regclass(format('%I.%I', v_schema, 'ready_entries')) IS NULL
        OR to_regclass(format('%I.%I', v_schema, 'leases')) IS NULL
    THEN
        RAISE EXCEPTION 'queue storage schema "%" is not prepared', v_schema
            USING ERRCODE = '55000';
    END IF;

    SELECT count(*)::bigint
    INTO v_live_canonical_count
    FROM awa.runtime_instances AS runtime
    WHERE runtime.storage_capability = 'canonical'
      AND runtime.last_seen_at + make_interval(
            secs => GREATEST(((GREATEST(runtime.snapshot_interval_ms, 1000) / 1000) * 3)::int, 30)
          ) >= now();

    IF v_live_canonical_count > 0 THEN
        RAISE EXCEPTION 'cannot enter mixed transition while % canonical-only runtime(s) are still live', v_live_canonical_count
            USING ERRCODE = '55000';
    END IF;

    SELECT count(*)::bigint
    INTO v_live_queue_storage_count
    FROM awa.runtime_instances AS runtime
    WHERE runtime.storage_capability = 'queue_storage'
      AND runtime.last_seen_at + make_interval(
            secs => GREATEST(((GREATEST(runtime.snapshot_interval_ms, 1000) / 1000) * 3)::int, 30)
          ) >= now();

    IF v_live_queue_storage_count = 0 THEN
        RAISE EXCEPTION 'cannot enter mixed transition without a live queue-storage-capable runtime'
            USING ERRCODE = '55000';
    END IF;

    INSERT INTO awa.runtime_storage_backends (backend, schema_name, updated_at)
    VALUES ('queue_storage', v_schema, now())
    ON CONFLICT (backend)
    DO UPDATE SET schema_name = EXCLUDED.schema_name, updated_at = EXCLUDED.updated_at;

    UPDATE awa.storage_transition_state AS sts
    SET
        state = 'mixed_transition',
        transition_epoch = sts.transition_epoch + 1,
        entered_at = now(),
        updated_at = now(),
        finalized_at = NULL
    WHERE sts.singleton
      AND sts.state = 'prepared';

    RETURN QUERY
    SELECT * FROM awa.storage_status();
END;
$$;

CREATE OR REPLACE FUNCTION awa.storage_finalize()
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
    v_prepared_engine TEXT;
    v_state TEXT;
    v_backlog BIGINT;
    v_live_drain_count BIGINT;
BEGIN
    SELECT sts.prepared_engine, sts.state
    INTO v_prepared_engine, v_state
    FROM awa.storage_transition_state AS sts
    WHERE sts.singleton
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'storage transition state row is missing'
            USING ERRCODE = '55000';
    END IF;

    IF v_state <> 'mixed_transition' THEN
        RAISE EXCEPTION 'storage finalize is only allowed from mixed_transition state'
            USING ERRCODE = '55000';
    END IF;

    IF v_prepared_engine IS DISTINCT FROM 'queue_storage' THEN
        RAISE EXCEPTION 'prepared engine "%" is not supported by this release', v_prepared_engine
            USING ERRCODE = '22023';
    END IF;

    SELECT awa.canonical_live_backlog()
    INTO v_backlog;

    IF v_backlog > 0 THEN
        RAISE EXCEPTION 'cannot finalize while canonical live backlog is %', v_backlog
            USING ERRCODE = '55000';
    END IF;

    SELECT count(*)::bigint
    INTO v_live_drain_count
    FROM awa.runtime_instances AS runtime
    WHERE runtime.storage_capability IN ('canonical', 'canonical_drain_only')
      AND runtime.last_seen_at + make_interval(
            secs => GREATEST(((GREATEST(runtime.snapshot_interval_ms, 1000) / 1000) * 3)::int, 30)
          ) >= now();

    IF v_live_drain_count > 0 THEN
        RAISE EXCEPTION 'cannot finalize while % canonical or drain-only runtime(s) are still live', v_live_drain_count
            USING ERRCODE = '55000';
    END IF;

    UPDATE awa.storage_transition_state AS sts
    SET
        current_engine = sts.prepared_engine,
        prepared_engine = NULL,
        state = 'active',
        transition_epoch = sts.transition_epoch + 1,
        entered_at = now(),
        updated_at = now(),
        finalized_at = now()
    WHERE sts.singleton
      AND sts.state = 'mixed_transition';

    RETURN QUERY
    SELECT * FROM awa.storage_status();
END;
$$;

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

        UPDATE queue_lanes AS lanes
        SET next_seq = lanes.next_seq + 1,
            available_count = lanes.available_count + 1
        WHERE lanes.queue = v_queue
          AND lanes.priority = v_priority
        RETURNING lanes.next_seq - 1
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

CREATE OR REPLACE FUNCTION awa.queue_storage_payload_tags(p_payload JSONB)
RETURNS TEXT[] AS $$
    SELECT ARRAY(
        SELECT jsonb_array_elements_text(
            COALESCE(NULLIF(p_payload->'tags', 'null'::jsonb), '[]'::jsonb)
        )
    )
$$ LANGUAGE sql IMMUTABLE
SET search_path = pg_catalog;

CREATE OR REPLACE FUNCTION awa.queue_storage_payload_errors(p_payload JSONB)
RETURNS JSONB[] AS $$
    SELECT CASE
        WHEN jsonb_typeof(
            COALESCE(NULLIF(p_payload->'errors', 'null'::jsonb), '[]'::jsonb)
        ) <> 'array'
            THEN NULL::JSONB[]
        WHEN jsonb_array_length(
            COALESCE(NULLIF(p_payload->'errors', 'null'::jsonb), '[]'::jsonb)
        ) = 0
            THEN NULL::JSONB[]
        ELSE ARRAY(
            SELECT jsonb_array_elements(
                COALESCE(NULLIF(p_payload->'errors', 'null'::jsonb), '[]'::jsonb)
            )
        )
    END
$$ LANGUAGE sql IMMUTABLE
SET search_path = pg_catalog;

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
) AS $$
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
            JOIN %1$I.queue_lanes AS lanes
              ON lanes.queue = ready.queue
             AND lanes.priority = ready.priority
            WHERE ready.lane_seq >= lanes.claim_seq
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
        FROM %1$I.done_entries AS done
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
$$ LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, awa, public;

CREATE OR REPLACE VIEW awa.jobs AS
SELECT
    id,
    kind,
    queue,
    args,
    state,
    priority,
    attempt,
    max_attempts,
    run_at,
    heartbeat_at,
    deadline_at,
    attempted_at,
    finalized_at,
    created_at,
    errors,
    metadata,
    tags,
    unique_key,
    unique_states::bit(8) AS unique_states,
    callback_id,
    callback_timeout_at,
    callback_filter,
    callback_on_complete,
    callback_on_fail,
    callback_transform,
    run_lease,
    progress
FROM awa.jobs_compat();

CREATE OR REPLACE FUNCTION awa.release_queue_storage_unique_claim(
    p_job_id BIGINT,
    p_unique_key BYTEA,
    p_unique_states TEXT,
    p_state awa.job_state
) RETURNS VOID AS $$
BEGIN
    IF p_unique_key IS NULL OR p_unique_states IS NULL THEN
        RETURN;
    END IF;

    IF awa.job_state_in_bitmask(p_unique_states::bit(8), p_state) THEN
        DELETE FROM awa.job_unique_claims
        WHERE unique_key = p_unique_key
          AND job_id = p_job_id;
    END IF;
END;
$$ LANGUAGE plpgsql
SET search_path = pg_catalog, awa, public;

CREATE OR REPLACE FUNCTION awa.delete_job_compat(p_id BIGINT)
RETURNS BOOLEAN AS $$
DECLARE
    v_schema TEXT;
    v_queue TEXT;
    v_priority SMALLINT;
    v_state awa.job_state;
    v_unique_key BYTEA;
    v_unique_states TEXT;
BEGIN
    v_schema := awa.active_queue_storage_schema();

    IF v_schema IS NULL THEN
        RAISE EXCEPTION 'queue storage is not active'
            USING ERRCODE = '55000';
    END IF;

    EXECUTE format(
        'DELETE FROM %I.ready_entries
         WHERE job_id = $1
         RETURNING queue, priority, ''available''::awa.job_state, unique_key, unique_states',
        v_schema
    )
    INTO v_queue, v_priority, v_state, v_unique_key, v_unique_states
    USING p_id;

    IF FOUND THEN
        EXECUTE format(
            'UPDATE %I.queue_lanes
             SET available_count = GREATEST(0, available_count - 1)
             WHERE queue = $1 AND priority = $2',
            v_schema
        )
        USING v_queue, v_priority;
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

    IF FOUND THEN
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

    IF FOUND THEN
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

    IF FOUND THEN
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

    IF FOUND THEN
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

CREATE OR REPLACE FUNCTION awa.write_jobs_view() RETURNS trigger AS $$
DECLARE
    v_queue_storage_schema TEXT;
    target_table TEXT;
    source_table TEXT;
    rows_deleted INTEGER;
    compat_row RECORD;
BEGIN
    v_queue_storage_schema := awa.active_queue_storage_schema();

    IF v_queue_storage_schema IS NOT NULL THEN
        IF TG_OP = 'INSERT' THEN
            NEW.queue := COALESCE(NEW.queue, 'default');
            NEW.args := COALESCE(NEW.args, '{}'::jsonb);
            NEW.state := COALESCE(NEW.state, 'available'::awa.job_state);
            NEW.priority := COALESCE(NEW.priority, 2);
            NEW.max_attempts := COALESCE(NEW.max_attempts, 25);
            NEW.run_at := COALESCE(NEW.run_at, now());
            NEW.metadata := COALESCE(NEW.metadata, '{}'::jsonb);
            NEW.tags := COALESCE(NEW.tags, '{}'::text[]);

            IF NEW.state NOT IN (
                'available'::awa.job_state,
                'scheduled'::awa.job_state,
                'retryable'::awa.job_state
            ) THEN
                RAISE EXCEPTION
                    'awa.jobs compatibility inserts only support available, scheduled, or retryable while queue_storage is active'
                    USING ERRCODE = '0A000';
            END IF;

            IF NEW.id IS NOT NULL
                OR COALESCE(NEW.attempt, 0) <> 0
                OR NEW.heartbeat_at IS NOT NULL
                OR NEW.deadline_at IS NOT NULL
                OR NEW.attempted_at IS NOT NULL
                OR NEW.finalized_at IS NOT NULL
                OR NEW.created_at IS NOT NULL
                OR NEW.errors IS NOT NULL
                OR COALESCE(NEW.run_lease, 0) <> 0
                OR NEW.callback_id IS NOT NULL
                OR NEW.callback_timeout_at IS NOT NULL
                OR NEW.callback_filter IS NOT NULL
                OR NEW.callback_on_complete IS NOT NULL
                OR NEW.callback_on_fail IS NOT NULL
                OR NEW.callback_transform IS NOT NULL
                OR NEW.progress IS NOT NULL
            THEN
                RAISE EXCEPTION
                    'awa.jobs compatibility inserts only support producer-style fields while queue_storage is active'
                    USING ERRCODE = '0A000';
            END IF;

            SELECT *
            INTO compat_row
            FROM awa.insert_job_compat(
                NEW.kind,
                NEW.queue,
                NEW.args,
                NEW.state,
                NEW.priority,
                NEW.max_attempts,
                NEW.run_at,
                NEW.metadata,
                NEW.tags,
                NEW.unique_key,
                NEW.unique_states
            );

            NEW.id := compat_row.id;
            NEW.kind := compat_row.kind;
            NEW.queue := compat_row.queue;
            NEW.args := compat_row.args;
            NEW.state := compat_row.state;
            NEW.priority := compat_row.priority;
            NEW.attempt := compat_row.attempt;
            NEW.max_attempts := compat_row.max_attempts;
            NEW.run_at := compat_row.run_at;
            NEW.heartbeat_at := compat_row.heartbeat_at;
            NEW.deadline_at := compat_row.deadline_at;
            NEW.attempted_at := compat_row.attempted_at;
            NEW.finalized_at := compat_row.finalized_at;
            NEW.created_at := compat_row.created_at;
            NEW.errors := compat_row.errors;
            NEW.metadata := compat_row.metadata;
            NEW.tags := compat_row.tags;
            NEW.unique_key := compat_row.unique_key;
            NEW.callback_id := compat_row.callback_id;
            NEW.callback_timeout_at := compat_row.callback_timeout_at;
            NEW.callback_filter := compat_row.callback_filter;
            NEW.callback_on_complete := compat_row.callback_on_complete;
            NEW.callback_on_fail := compat_row.callback_on_fail;
            NEW.callback_transform := compat_row.callback_transform;
            NEW.run_lease := compat_row.run_lease;
            NEW.progress := compat_row.progress;
            RETURN NEW;
        ELSIF TG_OP = 'DELETE' THEN
            IF awa.delete_job_compat(OLD.id) THEN
                RETURN OLD;
            END IF;
            RETURN NULL;
        END IF;

        RAISE EXCEPTION
            'UPDATE awa.jobs is not supported while queue_storage is active; use Awa admin/runtime APIs'
            USING ERRCODE = '0A000';
    END IF;

    IF TG_OP = 'INSERT' THEN
        NEW.id := COALESCE(NEW.id, nextval('awa.jobs_id_seq'));
        NEW.queue := COALESCE(NEW.queue, 'default');
        NEW.args := COALESCE(NEW.args, '{}'::jsonb);
        NEW.state := COALESCE(NEW.state, 'available'::awa.job_state);
        NEW.priority := COALESCE(NEW.priority, 2);
        NEW.attempt := COALESCE(NEW.attempt, 0);
        NEW.max_attempts := COALESCE(NEW.max_attempts, 25);
        NEW.run_at := COALESCE(NEW.run_at, now());
        NEW.created_at := COALESCE(NEW.created_at, now());
        NEW.errors := COALESCE(NEW.errors, '{}'::jsonb[]);
        NEW.metadata := COALESCE(NEW.metadata, '{}'::jsonb);
        NEW.tags := COALESCE(NEW.tags, '{}'::text[]);
        NEW.run_lease := COALESCE(NEW.run_lease, 0);
    END IF;

    IF TG_OP = 'DELETE' THEN
        source_table := CASE
            WHEN OLD.state IN ('scheduled'::awa.job_state, 'retryable'::awa.job_state)
                THEN 'awa.scheduled_jobs'
            ELSE 'awa.jobs_hot'
        END;
        EXECUTE format('DELETE FROM %s WHERE id = $1', source_table) USING OLD.id;
        RETURN OLD;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        source_table := CASE
            WHEN OLD.state IN ('scheduled'::awa.job_state, 'retryable'::awa.job_state)
                THEN 'awa.scheduled_jobs'
            ELSE 'awa.jobs_hot'
        END;
        EXECUTE format(
            'DELETE FROM %s WHERE id = $1 AND state = $2 AND run_lease = $3 AND callback_id IS NOT DISTINCT FROM $4',
            source_table
        ) USING OLD.id, OLD.state, OLD.run_lease, OLD.callback_id;
        GET DIAGNOSTICS rows_deleted = ROW_COUNT;

        IF rows_deleted = 0 THEN
            RETURN NULL;
        END IF;
    END IF;

    target_table := CASE
        WHEN NEW.state IN ('scheduled'::awa.job_state, 'retryable'::awa.job_state)
            THEN 'awa.scheduled_jobs'
        ELSE 'awa.jobs_hot'
    END;

    EXECUTE format(
        'INSERT INTO %s (
            id, kind, queue, args, state, priority, attempt, max_attempts,
            run_at, heartbeat_at, deadline_at, attempted_at, finalized_at,
            created_at, errors, metadata, tags, unique_key, unique_states,
            callback_id, callback_timeout_at, callback_filter, callback_on_complete,
            callback_on_fail, callback_transform, run_lease, progress
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8,
            $9, $10, $11, $12, $13,
            $14, $15, $16, $17, $18, $19,
            $20, $21, $22, $23,
            $24, $25, $26, $27
        )',
        target_table
    )
    USING
        NEW.id, NEW.kind, NEW.queue, NEW.args, NEW.state, NEW.priority, NEW.attempt,
        NEW.max_attempts, NEW.run_at, NEW.heartbeat_at, NEW.deadline_at, NEW.attempted_at,
        NEW.finalized_at, NEW.created_at, NEW.errors, NEW.metadata, NEW.tags,
        NEW.unique_key, NEW.unique_states, NEW.callback_id, NEW.callback_timeout_at,
        NEW.callback_filter, NEW.callback_on_complete, NEW.callback_on_fail,
        NEW.callback_transform, NEW.run_lease, NEW.progress;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql
SET search_path = pg_catalog, awa, public;

DROP TRIGGER IF EXISTS trg_awa_jobs_view_insert ON awa.jobs;
CREATE TRIGGER trg_awa_jobs_view_insert
    INSTEAD OF INSERT ON awa.jobs
    FOR EACH ROW
    EXECUTE FUNCTION awa.write_jobs_view();

DROP TRIGGER IF EXISTS trg_awa_jobs_view_update ON awa.jobs;
CREATE TRIGGER trg_awa_jobs_view_update
    INSTEAD OF UPDATE ON awa.jobs
    FOR EACH ROW
    EXECUTE FUNCTION awa.write_jobs_view();

DROP TRIGGER IF EXISTS trg_awa_jobs_view_delete ON awa.jobs;
CREATE TRIGGER trg_awa_jobs_view_delete
    INSTEAD OF DELETE ON awa.jobs
    FOR EACH ROW
    EXECUTE FUNCTION awa.write_jobs_view();

INSERT INTO awa.schema_version (version, description)
VALUES (11, 'Queue storage compatibility layer and active backend selection')
ON CONFLICT (version) DO NOTHING;

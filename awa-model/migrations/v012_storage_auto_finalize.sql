-- v012: storage auto-finalize for fresh installs and queue-storage count maintenance.
--
-- Closes the 0.6 release-readiness UX gap (issue #197): on a fresh
-- cluster with no canonical jobs and no historical workers, the
-- operator should not need to run prepare → enter-mixed-transition →
-- finalize manually. The Auto worker role's existing storage_status
-- check (awa-worker/src/client.rs:893) bails to canonical unless
-- state ∈ {mixed_transition, active}, so without this fast-path a
-- fresh DB sits on canonical until somebody runs the staged commands.
--
-- This migration adds awa.storage_auto_finalize_if_fresh(p_schema TEXT)
-- which atomically advances state from `canonical` directly to
-- `active` when the fresh-install conditions hold:
--
--   * state = 'canonical' (default after install)
--   * prepared_engine IS NULL (no operator commands have run)
--   * awa.jobs is empty (no canonical work has ever been enqueued)
--   * no recently-heartbeating runtime_instances (no live worker fleet)
--
-- It does NOT skip the staged transition for upgrades. Once any
-- canonical work exists or any runtime has heartbeated within the
-- liveness window, the function returns FALSE and the caller falls
-- back to the manual prepare/enter-mixed-transition/finalize path.
--
-- The schema-install side (prepare_schema in Rust) is the caller's
-- responsibility — auto-finalize assumes the queue-storage tables are
-- already present, mirroring the contract of storage_finalize().
-- Workers should call prepare_schema() before invoking this function;
-- prepare_schema is idempotent so concurrent workers are safe.
--
-- The FOR UPDATE on the singleton row serialises with concurrent
-- callers: first worker's call returns TRUE and flips state to
-- `active`; subsequent calls see state = 'active' and return FALSE.

CREATE OR REPLACE FUNCTION awa.storage_auto_finalize_if_fresh(p_schema TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql
SET search_path = pg_catalog, awa
AS $$
DECLARE
    v_state           TEXT;
    v_prepared_engine TEXT;
    v_canonical_count BIGINT;
    v_runtime_count   BIGINT;
    v_schema          TEXT := COALESCE(NULLIF(btrim(p_schema), ''), 'awa');
BEGIN
    SELECT state, prepared_engine
    INTO v_state, v_prepared_engine
    FROM awa.storage_transition_state
    WHERE singleton
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'storage transition state row is missing'
            USING ERRCODE = '55000';
    END IF;

    -- Already past canonical: nothing to do.
    IF v_state IS DISTINCT FROM 'canonical' THEN
        RETURN FALSE;
    END IF;

    -- Operator has explicitly prepared a target engine; respect that
    -- and let the staged path run.
    IF v_prepared_engine IS NOT NULL THEN
        RETURN FALSE;
    END IF;

    -- Any canonical work means this isn't a fresh install. Including
    -- terminal-state rows is intentional: even one finalized job means
    -- canonical was the active engine at some point and the operator
    -- should opt into the staged transition.
    SELECT count(*) INTO v_canonical_count FROM awa.jobs;
    IF v_canonical_count > 0 THEN
        RETURN FALSE;
    END IF;

    -- Any recently-live runtime means a worker fleet is heartbeating
    -- already — we should not skip the staged transition under them.
    -- The 90-second window matches the heartbeat-staleness gate used
    -- by storage_enter_mixed_transition.
    SELECT count(*) INTO v_runtime_count
    FROM awa.runtime_instances
    WHERE last_seen_at + make_interval(secs => 90) >= now();
    IF v_runtime_count > 0 THEN
        RETURN FALSE;
    END IF;

    -- Conditions hold. Promote directly to active. Skipping the
    -- intermediate states is safe specifically because there's no
    -- canonical work to drain and no live workers to coordinate.
    --
    -- NOTE: `active_engine` is derived in `awa.storage_status()` /
    -- `awa.active_storage_engine()` — it's not a column. To land in
    -- `active_engine = 'queue_storage'` we set `current_engine =
    -- 'queue_storage'` plus `state = 'active'`; the derivation does
    -- the rest.
    UPDATE awa.storage_transition_state
    SET
        state            = 'active',
        current_engine   = 'queue_storage',
        prepared_engine  = NULL,
        details          = jsonb_build_object(
                              'schema', v_schema,
                              'auto_finalized', true
                           ),
        transition_epoch = transition_epoch + 1,
        entered_at       = now(),
        updated_at       = now(),
        finalized_at     = now()
    WHERE singleton
      AND state = 'canonical';

    INSERT INTO awa.runtime_storage_backends (backend, schema_name, updated_at)
    VALUES ('queue_storage', v_schema, now())
    ON CONFLICT (backend) DO UPDATE
    SET schema_name = EXCLUDED.schema_name,
        updated_at  = EXCLUDED.updated_at;

    RETURN TRUE;
END;
$$;

GRANT EXECUTE ON FUNCTION awa.storage_auto_finalize_if_fresh(TEXT) TO PUBLIC;

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

        UPDATE queue_lanes
        SET available_count = available_count + 1
        WHERE queue = v_queue
          AND priority = v_priority;

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
    -- NOTE: PL/pgSQL's `FOUND` is NOT set by `EXECUTE` even when an INTO
    -- clause captures values from a `DELETE ... RETURNING`. This is a
    -- well-known asymmetry between static and dynamic DML in PL/pgSQL.
    -- Every branch below uses `GET DIAGNOSTICS v_rows = ROW_COUNT` to
    -- decide whether the delete actually happened. The previous
    -- `IF FOUND THEN` shape silently fell through to the next branch
    -- and made delete_job_compat return FALSE for every successful
    -- delete — masking the available_count decrement and the unique
    -- claim release.
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
        EXECUTE format(
            'UPDATE %I.queue_lanes
             SET available_count = GREATEST(0, available_count - 1)
             WHERE queue = $1
               AND priority = $2
               AND $3 >= (
                   SELECT claim_seq
                   FROM %I.queue_claim_heads
                   WHERE queue = $1 AND priority = $2
               )',
            v_schema,
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

DO $$
DECLARE
    v_schema TEXT;
BEGIN
    SELECT COALESCE(
        (
            SELECT schema_name
            FROM awa.runtime_storage_backends
            WHERE backend = 'queue_storage'
        ),
        NULLIF(awa.storage_transition_state.details->>'schema', ''),
        'awa'
    )
    INTO v_schema
    FROM awa.storage_transition_state
    WHERE singleton;

    IF v_schema IS NOT NULL
       AND to_regclass(format('%I.%I', v_schema, 'queue_lanes')) IS NOT NULL
       AND to_regclass(format('%I.%I', v_schema, 'ready_entries')) IS NOT NULL
       AND to_regclass(format('%I.%I', v_schema, 'queue_claim_heads')) IS NOT NULL
    THEN
        EXECUTE format(
            'UPDATE %1$I.queue_lanes AS lanes
             SET available_count = COALESCE(live_ready.available_count, 0)
             FROM (
                 SELECT
                     ready.queue,
                     ready.priority,
                     count(*)::bigint AS available_count
                 FROM %1$I.ready_entries AS ready
                 JOIN %1$I.queue_claim_heads AS claims
                   ON claims.queue = ready.queue
                  AND claims.priority = ready.priority
                 WHERE ready.lane_seq >= claims.claim_seq
                 GROUP BY ready.queue, ready.priority
             ) AS live_ready
             WHERE lanes.queue = live_ready.queue
               AND lanes.priority = live_ready.priority',
            v_schema
        );

        EXECUTE format(
            'UPDATE %1$I.queue_lanes AS lanes
             SET available_count = 0
             WHERE available_count <> 0
               AND NOT EXISTS (
                   SELECT 1
                   FROM %1$I.ready_entries AS ready
                   JOIN %1$I.queue_claim_heads AS claims
                     ON claims.queue = ready.queue
                    AND claims.priority = ready.priority
                   WHERE ready.queue = lanes.queue
                     AND ready.priority = lanes.priority
                     AND ready.lane_seq >= claims.claim_seq
               )',
            v_schema
        );
    END IF;
END;
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (12, 'Storage auto-finalize and queue-storage count maintenance')
ON CONFLICT (version) DO NOTHING;

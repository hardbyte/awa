-- v017: Shard queue_enqueue_heads / queue_claim_heads / ready_entries by
-- enqueue_shard to spread row-lock contention across N rows per
-- (queue, priority).
--
-- Background:
--   Wait-event analysis at 16 producers, single queue showed 93% of producer
--   time waiting on Lock:transactionid / Lock:tuple, almost all of it on the
--   `UPDATE queue_enqueue_heads SET next_seq = next_seq + $3` advance. With a
--   single head row per (queue, priority), every concurrent enqueue serialises
--   through that row. Sharding the head into S independent rows breaks that
--   serialisation: producers rotate across shards and contend with at most
--   `concurrency / S` peers per row.
--
-- Schema reshape:
--   * awa.queue_meta.enqueue_shards SMALLINT (1..=64), default 1.
--   * queue_enqueue_heads, queue_claim_heads, ready_entries gain
--     `enqueue_shard SMALLINT NOT NULL DEFAULT 0`. The primary keys extend
--     to include `enqueue_shard` so multiple shards coexist per
--     (queue, priority).
--
-- FIFO semantics:
--   `lane_seq` continues to be allocated per head row; with the PK extended
--   to include the shard, each shard has its own independent sequence. FIFO
--   ordering is preserved within a shard. With the default S=1, only shard 0
--   exists and behaviour is observationally identical to v016. FIFO across
--   shards is approximate at S>1; that is the explicit trade-off this
--   migration enables.
--
-- Transition gate:
--   This migration must not run mid-cutover. If the storage transition state
--   is `mixed_transition` (canonical and queue_storage running in parallel)
--   and the new columns are not already present, the migration raises rather
--   than rewriting hot-path PKs while a parallel engine is reading them.
--   `active` is the steady state after a finalized cutover (no parallel
--   engine reads), so the gate does not fire there even though the spec
--   lists it.

DO $$
DECLARE
    v_schema TEXT;
    v_state TEXT;
    v_has_col BOOLEAN;
BEGIN
    SELECT state INTO v_state
    FROM awa.storage_transition_state
    WHERE singleton;

    FOR v_schema IN
        SELECT schema_name
        FROM awa.runtime_storage_backends
        WHERE schema_name IS NOT NULL
    LOOP
        IF to_regclass(format('%I.%I', v_schema, 'queue_enqueue_heads')) IS NULL THEN
            CONTINUE;
        END IF;

        EXECUTE format(
            'SELECT EXISTS (
                 SELECT 1
                 FROM pg_attribute a
                 JOIN pg_class c ON c.oid = a.attrelid
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = %L
                   AND c.relname = %L
                   AND a.attname = %L
                   AND NOT a.attisdropped
             )',
            v_schema, 'queue_enqueue_heads', 'enqueue_shard'
        ) INTO v_has_col;

        IF NOT v_has_col AND v_state = 'mixed_transition' THEN
            RAISE EXCEPTION
                'v017 cannot reshape queue_enqueue_heads PK while storage_transition_state=mixed_transition (schema=%); finalize or abort the transition first',
                v_schema
                USING ERRCODE = '55000';
        END IF;
    END LOOP;
END
$$ LANGUAGE plpgsql;

-- 1. Per-queue tunable on awa.queue_meta. SMALLINT, default 1,
-- bounded 1..=64. Each row in `queue_meta` describes one logical
-- queue; raising `enqueue_shards` for that queue spreads producer
-- writes across N independent shard rows in queue_enqueue_heads /
-- queue_claim_heads, breaking the single-row lock-contention point
-- on enqueue. Lowering the value is safe only once `ready_entries`
-- has no rows in shards above the new bound; see ADR-025 for the
-- semantic contract.
ALTER TABLE awa.queue_meta
    ADD COLUMN IF NOT EXISTS enqueue_shards SMALLINT NOT NULL DEFAULT 1;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'awa'
          AND t.relname = 'queue_meta'
          AND c.conname = 'queue_meta_enqueue_shards_range'
    ) THEN
        ALTER TABLE awa.queue_meta
            ADD CONSTRAINT queue_meta_enqueue_shards_range
            CHECK (enqueue_shards BETWEEN 1 AND 64);
    END IF;
END
$$ LANGUAGE plpgsql;

-- 2. For each queue storage backend schema, reshape the three hot tables.
DO $$
DECLARE
    v_schema TEXT;
    v_has_col BOOLEAN;
    v_pk_name TEXT;
BEGIN
    FOR v_schema IN
        SELECT schema_name
        FROM awa.runtime_storage_backends
        WHERE schema_name IS NOT NULL
    LOOP
        -- queue_enqueue_heads
        IF to_regclass(format('%I.%I', v_schema, 'queue_enqueue_heads')) IS NOT NULL THEN
            EXECUTE format(
                'SELECT EXISTS (
                     SELECT 1 FROM pg_attribute a
                     JOIN pg_class c ON c.oid = a.attrelid
                     JOIN pg_namespace n ON n.oid = c.relnamespace
                     WHERE n.nspname = %L AND c.relname = %L
                       AND a.attname = %L AND NOT a.attisdropped
                 )',
                v_schema, 'queue_enqueue_heads', 'enqueue_shard'
            ) INTO v_has_col;

            IF NOT v_has_col THEN
                EXECUTE format(
                    'ALTER TABLE %I.queue_enqueue_heads
                         ADD COLUMN enqueue_shard SMALLINT NOT NULL DEFAULT 0',
                    v_schema
                );

                SELECT c.conname INTO v_pk_name
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE n.nspname = v_schema
                  AND t.relname = 'queue_enqueue_heads'
                  AND c.contype = 'p';

                IF v_pk_name IS NOT NULL THEN
                    EXECUTE format(
                        'ALTER TABLE %I.queue_enqueue_heads DROP CONSTRAINT %I',
                        v_schema, v_pk_name
                    );
                END IF;

                EXECUTE format(
                    'ALTER TABLE %I.queue_enqueue_heads
                         ADD PRIMARY KEY (queue, priority, enqueue_shard)',
                    v_schema
                );
            END IF;
        END IF;

        -- queue_claim_heads
        IF to_regclass(format('%I.%I', v_schema, 'queue_claim_heads')) IS NOT NULL THEN
            EXECUTE format(
                'SELECT EXISTS (
                     SELECT 1 FROM pg_attribute a
                     JOIN pg_class c ON c.oid = a.attrelid
                     JOIN pg_namespace n ON n.oid = c.relnamespace
                     WHERE n.nspname = %L AND c.relname = %L
                       AND a.attname = %L AND NOT a.attisdropped
                 )',
                v_schema, 'queue_claim_heads', 'enqueue_shard'
            ) INTO v_has_col;

            IF NOT v_has_col THEN
                EXECUTE format(
                    'ALTER TABLE %I.queue_claim_heads
                         ADD COLUMN enqueue_shard SMALLINT NOT NULL DEFAULT 0',
                    v_schema
                );

                SELECT c.conname INTO v_pk_name
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE n.nspname = v_schema
                  AND t.relname = 'queue_claim_heads'
                  AND c.contype = 'p';

                IF v_pk_name IS NOT NULL THEN
                    EXECUTE format(
                        'ALTER TABLE %I.queue_claim_heads DROP CONSTRAINT %I',
                        v_schema, v_pk_name
                    );
                END IF;

                EXECUTE format(
                    'ALTER TABLE %I.queue_claim_heads
                         ADD PRIMARY KEY (queue, priority, enqueue_shard)',
                    v_schema
                );
            END IF;
        END IF;

        -- ready_entries (partitioned by ready_slot). Dropping/adding a PK
        -- on the partitioned parent cascades to every leaf partition, so
        -- the parent operations cover all children; ADD COLUMN cascades
        -- the same way. The parent PK must include `enqueue_shard` —
        -- without it, two shards writing the same `lane_seq` collide on
        -- the parent's unique constraint even when each leaf permits
        -- the row.
        IF to_regclass(format('%I.%I', v_schema, 'ready_entries')) IS NOT NULL THEN
            EXECUTE format(
                'SELECT EXISTS (
                     SELECT 1 FROM pg_attribute a
                     JOIN pg_class c ON c.oid = a.attrelid
                     JOIN pg_namespace n ON n.oid = c.relnamespace
                     WHERE n.nspname = %L AND c.relname = %L
                       AND a.attname = %L AND NOT a.attisdropped
                 )',
                v_schema, 'ready_entries', 'enqueue_shard'
            ) INTO v_has_col;

            IF NOT v_has_col THEN
                -- Drop the parent PK first so the children's inherited PKs
                -- come along; you can't drop a child's inherited PK while
                -- the parent's PK still exists.
                SELECT c.conname INTO v_pk_name
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE n.nspname = v_schema
                  AND t.relname = 'ready_entries'
                  AND c.contype = 'p';

                IF v_pk_name IS NOT NULL THEN
                    EXECUTE format(
                        'ALTER TABLE %I.ready_entries DROP CONSTRAINT %I',
                        v_schema, v_pk_name
                    );
                END IF;

                EXECUTE format(
                    'ALTER TABLE %I.ready_entries
                         ADD COLUMN enqueue_shard SMALLINT NOT NULL DEFAULT 0',
                    v_schema
                );

                EXECUTE format(
                    'ALTER TABLE %I.ready_entries
                         ADD PRIMARY KEY (ready_slot, queue, priority, enqueue_shard, lane_seq)',
                    v_schema
                );
            END IF;
        END IF;

        -- done_entries (partitioned by ready_slot). Carries enqueue_shard
        -- in its PK so terminal rows for two shards at the same
        -- (ready_slot, queue, priority, lane_seq) don't collide on
        -- completion.
        IF to_regclass(format('%I.%I', v_schema, 'done_entries')) IS NOT NULL THEN
            EXECUTE format(
                'SELECT EXISTS (
                     SELECT 1 FROM pg_attribute a
                     JOIN pg_class c ON c.oid = a.attrelid
                     JOIN pg_namespace n ON n.oid = c.relnamespace
                     WHERE n.nspname = %L AND c.relname = %L
                       AND a.attname = %L AND NOT a.attisdropped
                 )',
                v_schema, 'done_entries', 'enqueue_shard'
            ) INTO v_has_col;

            IF NOT v_has_col THEN
                SELECT c.conname INTO v_pk_name
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE n.nspname = v_schema
                  AND t.relname = 'done_entries'
                  AND c.contype = 'p';

                IF v_pk_name IS NOT NULL THEN
                    EXECUTE format(
                        'ALTER TABLE %I.done_entries DROP CONSTRAINT %I',
                        v_schema, v_pk_name
                    );
                END IF;

                EXECUTE format(
                    'ALTER TABLE %I.done_entries
                         ADD COLUMN enqueue_shard SMALLINT NOT NULL DEFAULT 0',
                    v_schema
                );

                EXECUTE format(
                    'ALTER TABLE %I.done_entries
                         ADD PRIMARY KEY (ready_slot, queue, priority, enqueue_shard, lane_seq)',
                    v_schema
                );
            END IF;
        END IF;

        -- leases (partitioned by lease_slot). Same shape as
        -- ready_entries / done_entries: shard joins the PK so two
        -- shards' running rows don't collide.
        IF to_regclass(format('%I.%I', v_schema, 'leases')) IS NOT NULL THEN
            EXECUTE format(
                'SELECT EXISTS (
                     SELECT 1 FROM pg_attribute a
                     JOIN pg_class c ON c.oid = a.attrelid
                     JOIN pg_namespace n ON n.oid = c.relnamespace
                     WHERE n.nspname = %L AND c.relname = %L
                       AND a.attname = %L AND NOT a.attisdropped
                 )',
                v_schema, 'leases', 'enqueue_shard'
            ) INTO v_has_col;

            IF NOT v_has_col THEN
                SELECT c.conname INTO v_pk_name
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE n.nspname = v_schema
                  AND t.relname = 'leases'
                  AND c.contype = 'p';

                IF v_pk_name IS NOT NULL THEN
                    EXECUTE format(
                        'ALTER TABLE %I.leases DROP CONSTRAINT %I',
                        v_schema, v_pk_name
                    );
                END IF;

                EXECUTE format(
                    'ALTER TABLE %I.leases
                         ADD COLUMN enqueue_shard SMALLINT NOT NULL DEFAULT 0',
                    v_schema
                );

                EXECUTE format(
                    'ALTER TABLE %I.leases
                         ADD PRIMARY KEY (lease_slot, queue, priority, enqueue_shard, lane_seq)',
                    v_schema
                );
            END IF;
        END IF;

        -- lease_claims (partitioned by claim_slot). PK is
        -- (claim_slot, job_id, run_lease) and stays that way — job_id
        -- is globally unique, so the PK is already correct. The shard
        -- is added as a column so the cancel-from-receipt path can
        -- route the synthesized done_entries write to the right
        -- shard.
        IF to_regclass(format('%I.%I', v_schema, 'lease_claims')) IS NOT NULL THEN
            EXECUTE format(
                'SELECT EXISTS (
                     SELECT 1 FROM pg_attribute a
                     JOIN pg_class c ON c.oid = a.attrelid
                     JOIN pg_namespace n ON n.oid = c.relnamespace
                     WHERE n.nspname = %L AND c.relname = %L
                       AND a.attname = %L AND NOT a.attisdropped
                 )',
                v_schema, 'lease_claims', 'enqueue_shard'
            ) INTO v_has_col;

            IF NOT v_has_col THEN
                EXECUTE format(
                    'ALTER TABLE %I.lease_claims
                         ADD COLUMN enqueue_shard SMALLINT NOT NULL DEFAULT 0',
                    v_schema
                );
            END IF;
        END IF;
    END LOOP;
END
$$ LANGUAGE plpgsql;

-- 3. Reshape insert_job_compat to thread enqueue_shard. With S=1 (the default)
--    the picked shard is always 0, so behaviour is unchanged. The SQL compat
--    path picks the shard by `MOD(pg_backend_pid()::bigint, v_enqueue_shards)`
--    rather than a cross-call atomic; backend-pid rotation is stable enough
--    to spread connection-pinned producers across shards.
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
    v_enqueue_shards SMALLINT;
    v_enqueue_shard SMALLINT;
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
        -- Resolve per-queue shard count once, default to 1.
        SELECT COALESCE(meta.enqueue_shards, 1)
        INTO v_enqueue_shards
        FROM awa.queue_meta AS meta
        WHERE meta.queue = v_queue;
        v_enqueue_shards := COALESCE(v_enqueue_shards, 1);

        -- Backend-pid rotation: stable within a connection, distributed across
        -- a producer pool. With v_enqueue_shards=1 this always picks shard 0.
        v_enqueue_shard := MOD(pg_backend_pid()::bigint, v_enqueue_shards)::smallint;

        INSERT INTO queue_lanes (queue, priority)
        VALUES (v_queue, v_priority)
        ON CONFLICT ON CONSTRAINT queue_lanes_pkey DO NOTHING;

        INSERT INTO queue_enqueue_heads (queue, priority, enqueue_shard)
        VALUES (v_queue, v_priority, v_enqueue_shard)
        ON CONFLICT (queue, priority, enqueue_shard) DO NOTHING;

        INSERT INTO queue_claim_heads (queue, priority, enqueue_shard)
        VALUES (v_queue, v_priority, v_enqueue_shard)
        ON CONFLICT (queue, priority, enqueue_shard) DO NOTHING;

        UPDATE queue_enqueue_heads AS heads
        SET next_seq = heads.next_seq + 1
        WHERE heads.queue = v_queue
          AND heads.priority = v_priority
          AND heads.enqueue_shard = v_enqueue_shard
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

-- 4. Reshape delete_job_compat. The head-lane delete branch now scopes the
--    claim_seq bump to the deleted row's enqueue_shard. The leases branch
--    propagates enqueue_shard through the deleted CTE and joins
--    ready_entries on (ready_slot, ready_generation, queue, priority,
--    enqueue_shard, lane_seq, job_id) — at S>1 the older 5-column tuple
--    is no longer unique across shards, so omitting enqueue_shard would
--    let the JOIN pick another shard's row and release the wrong
--    unique_key. With S=1 the WHERE clauses are unchanged from v016 in
--    effect.
CREATE OR REPLACE FUNCTION awa.delete_job_compat(p_id BIGINT)
RETURNS BOOLEAN AS $$
DECLARE
    v_schema TEXT;
    v_queue TEXT;
    v_priority SMALLINT;
    v_lane_seq BIGINT;
    v_enqueue_shard SMALLINT;
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
         RETURNING queue, priority, lane_seq, enqueue_shard,
                   ''available''::awa.job_state, unique_key, unique_states',
        v_schema
    )
    INTO v_queue, v_priority, v_lane_seq, v_enqueue_shard,
         v_state, v_unique_key, v_unique_states
    USING p_id;
    GET DIAGNOSTICS v_rows = ROW_COUNT;

    IF v_rows > 0 THEN
        EXECUTE format(
            'UPDATE %I.queue_claim_heads
             SET claim_seq = claim_seq + 1
             WHERE queue = $1
               AND priority = $2
               AND enqueue_shard = $3
               AND claim_seq = $4',
            v_schema
        )
        USING v_queue, v_priority, v_enqueue_shard, v_lane_seq;
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
VALUES (17, 'Shard queue_enqueue_heads/queue_claim_heads/ready_entries by enqueue_shard')
ON CONFLICT (version) DO NOTHING;

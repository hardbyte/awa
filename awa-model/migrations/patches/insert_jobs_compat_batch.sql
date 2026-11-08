-- Schema patch insert_jobs_compat_batch: a batch producer insert that holds
-- each enqueue lane lock only for the lane write (awa 0.7 migration v055,
-- minus the schema_version row).
--
-- awa.insert_job_compat takes the (queue, priority, shard) lane lock in
-- reserve_enqueue_seq on a batch's first row and, because the lock is
-- transaction-scoped, holds it while every remaining row is processed. Many
-- concurrent producers on one lane therefore queue behind whole batches. This
-- patch adds awa.insert_jobs_compat_batch, which does the lane-independent
-- work first (job ids, unique claims in key order, scheduled rows, result
-- rows), then, in (queue, priority, shard) order, registers each new lane and
-- reserves each lane's sequence range once, and writes the ready rows and
-- segments in two statements. The lock is held across the reservation, those
-- two statements and the commit.
--
-- The column arrays are parallel and must have equal length. Every element is
-- required except run_at, unique_key, unique_states and ordering_hash;
-- insert_job_compat's per-row defaults are the caller's to apply.
--
-- p_skip_unique_conflicts inserts a batch while skipping rows whose unique key
-- is already claimed (the rows are absent from the result) instead of failing
-- the whole batch with a unique violation.
--
-- Live load: one CREATE OR REPLACE FUNCTION, no table DDL, no table locks.
--
-- 0.6 delivery: the 0.6 series cannot add a migration version, so this ships as
-- an idempotent schema patch. `awa migrate` applies it when the schema is
-- exactly at v040 and the probe in migrations.rs reports it missing;
-- `awa migrate --sql` prints it as a repeatable R__ script for external
-- runners. Released 0.6 binaries never call the function: they keep calling
-- awa.insert_job_compat, which this patch does not touch, and both take the
-- same lane lock, so producers on either path share a lane in lane_seq order.
-- Binaries that know the function: insert_many_copy probes its own connection
-- on every call and uses the per-row function until this patch is present;
-- generic insert_many always uses the per-row function;
-- insert_many_skip_unique_conflicts requires this patch. The ready ring cursor
-- is read through ring_cursor() where a later schema provides it, so the
-- function stays correct until a 0.7 migration replaces it.

CREATE OR REPLACE FUNCTION awa.insert_jobs_compat_batch(
    p_kind TEXT[],
    p_queue TEXT[],
    p_args JSONB[],
    p_state TEXT[],
    p_priority SMALLINT[],
    p_max_attempts SMALLINT[],
    p_run_at TIMESTAMPTZ[],
    p_metadata JSONB[],
    p_tags JSONB[],
    p_unique_key BYTEA[],
    p_unique_states TEXT[],
    p_ordering_hash TEXT[],
    p_skip_unique_conflicts BOOLEAN DEFAULT FALSE
)
RETURNS SETOF awa.jobs
AS $$
DECLARE
    v_count INT := COALESCE(cardinality(p_kind), 0);
    v_schema TEXT;
    v_old_search_path TEXT;
    v_now TIMESTAMPTZ := clock_timestamp();
    v_job_ids BIGINT[];
    v_skipped BIGINT[] := ARRAY[]::BIGINT[];
    v_lane RECORD;
    v_lane_queues TEXT[] := ARRAY[]::TEXT[];
    v_lane_priorities SMALLINT[] := ARRAY[]::SMALLINT[];
    v_lane_shards SMALLINT[] := ARRAY[]::SMALLINT[];
    v_lane_starts BIGINT[] := ARRAY[]::BIGINT[];
    v_start BIGINT;
    v_ready_slot INT;
    v_ready_generation BIGINT;
    v_row RECORD;
    v_shards SMALLINT[];
BEGIN
    IF v_count = 0 THEN
        RETURN;
    END IF;

    IF cardinality(p_queue) IS DISTINCT FROM v_count
        OR cardinality(p_args) IS DISTINCT FROM v_count
        OR cardinality(p_state) IS DISTINCT FROM v_count
        OR cardinality(p_priority) IS DISTINCT FROM v_count
        OR cardinality(p_max_attempts) IS DISTINCT FROM v_count
        OR cardinality(p_run_at) IS DISTINCT FROM v_count
        OR cardinality(p_metadata) IS DISTINCT FROM v_count
        OR cardinality(p_tags) IS DISTINCT FROM v_count
        OR cardinality(p_unique_key) IS DISTINCT FROM v_count
        OR cardinality(p_unique_states) IS DISTINCT FROM v_count
        OR cardinality(p_ordering_hash) IS DISTINCT FROM v_count
    THEN
        RAISE EXCEPTION 'insert_jobs_compat_batch: column arrays must have equal length'
            USING ERRCODE = '22023';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM unnest(p_kind, p_queue, p_args, p_state, p_priority, p_max_attempts, p_metadata, p_tags)
            AS input(kind, queue, args, state, priority, max_attempts, metadata, tags)
        WHERE input.kind IS NULL OR input.queue IS NULL OR input.args IS NULL
           OR input.state IS NULL OR input.priority IS NULL OR input.max_attempts IS NULL
           OR input.metadata IS NULL OR input.tags IS NULL
    ) THEN
        RAISE EXCEPTION 'insert_jobs_compat_batch: kind, queue, args, state, priority, max_attempts, metadata and tags must not be NULL'
            USING ERRCODE = '22004';
    END IF;

    IF EXISTS (SELECT 1 FROM unnest(p_kind) AS kind WHERE length(kind) > 200) THEN
        RAISE EXCEPTION 'job kind length must be <= 200 characters'
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (SELECT 1 FROM unnest(p_queue) AS queue WHERE length(queue) > 200) THEN
        RAISE EXCEPTION 'queue name length must be <= 200 characters'
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1 FROM unnest(p_priority) AS priority
        WHERE priority < 1 OR priority > 4
    ) THEN
        RAISE EXCEPTION 'priority must be between 1 and 4'
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1 FROM unnest(p_max_attempts) AS max_attempts
        WHERE max_attempts < 1 OR max_attempts > 1000
    ) THEN
        RAISE EXCEPTION 'max_attempts must be between 1 and 1000'
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1 FROM unnest(p_tags) AS tags
        WHERE jsonb_array_length(tags) > 20
    ) THEN
        RAISE EXCEPTION 'job tags must contain at most 20 values'
            USING ERRCODE = '23514';
    END IF;

    v_schema := awa.active_queue_storage_schema();

    -- Canonical storage has no lane lock to shorten: insert row by row.
    IF v_schema IS NULL THEN
        IF NOT p_skip_unique_conflicts THEN
            RETURN QUERY
            SELECT inserted.*
            FROM unnest(
                p_kind, p_queue, p_args, p_state, p_priority, p_max_attempts,
                p_run_at, p_metadata, p_tags, p_unique_key, p_unique_states
            ) WITH ORDINALITY AS input(
                kind, queue, args, state, priority, max_attempts, run_at,
                metadata, tags, unique_key, unique_states, ord
            )
            CROSS JOIN LATERAL awa.insert_job_compat(
                input.kind,
                input.queue,
                input.args,
                input.state::awa.job_state,
                input.priority,
                input.max_attempts,
                input.run_at,
                input.metadata,
                ARRAY(
                    SELECT tag.value
                    FROM jsonb_array_elements_text(input.tags) WITH ORDINALITY AS tag(value, tag_ord)
                    ORDER BY tag.tag_ord
                ),
                input.unique_key,
                input.unique_states::bit(8),
                NULL
            ) AS inserted
            ORDER BY input.ord;
            RETURN;
        END IF;

        -- Skipping needs a subtransaction per row.
        FOR v_row IN
            SELECT
                input.ord,
                input.kind,
                input.queue,
                input.args,
                input.state,
                input.priority,
                input.max_attempts,
                input.run_at,
                input.metadata,
                ARRAY(
                    SELECT tag.value
                    FROM jsonb_array_elements_text(input.tags) WITH ORDINALITY AS tag(value, tag_ord)
                    ORDER BY tag.tag_ord
                ) AS tags,
                input.unique_key,
                input.unique_states
            FROM unnest(
                p_kind, p_queue, p_args, p_state, p_priority, p_max_attempts,
                p_run_at, p_metadata, p_tags, p_unique_key, p_unique_states
            ) WITH ORDINALITY AS input(
                kind, queue, args, state, priority, max_attempts, run_at,
                metadata, tags, unique_key, unique_states, ord
            )
            ORDER BY input.ord
        LOOP
            BEGIN
                RETURN NEXT awa.insert_job_compat(
                    v_row.kind,
                    v_row.queue,
                    v_row.args,
                    v_row.state::awa.job_state,
                    v_row.priority,
                    v_row.max_attempts,
                    v_row.run_at,
                    v_row.metadata,
                    v_row.tags,
                    v_row.unique_key,
                    v_row.unique_states::bit(8),
                    NULL
                );
            EXCEPTION WHEN unique_violation THEN
                NULL;
            END;
        END LOOP;
        RETURN;
    END IF;

    IF EXISTS (
        SELECT 1 FROM unnest(p_state) AS state
        WHERE state NOT IN ('available', 'scheduled', 'retryable')
    ) THEN
        RAISE EXCEPTION 'queue storage does not support initial state %',
            (SELECT state FROM unnest(p_state) AS state
             WHERE state NOT IN ('available', 'scheduled', 'retryable') LIMIT 1)
            USING ERRCODE = '22023';
    END IF;

    v_old_search_path := current_setting('search_path');
    PERFORM set_config('search_path', format('%I,awa,public', v_schema), true);

    -- Everything up to the lane reservation is lane-independent and runs
    -- without a lane lock.
    EXECUTE format(
        'SELECT array_agg(nextval(%L::regclass)::bigint) FROM generate_series(1, $1)',
        format('%I.job_id_seq', v_schema)
    )
    INTO v_job_ids
    USING v_count;

    -- Rows that enqueue as `available` go to a lane. Their shard is derived as
    -- insert_job_compat derives it: ordering-key rows from the key hash, the
    -- rest from the backend pid.
    SELECT array_agg(
        CASE WHEN input.state = 'available' THEN
            MOD(
                CASE WHEN input.ordering_hash IS NULL
                     THEN pg_backend_pid()::numeric
                     ELSE input.ordering_hash::numeric
                END,
                COALESCE(meta.enqueue_shards, 1)::numeric
            )::smallint
        END
        ORDER BY input.ord
    )
    INTO v_shards
    FROM unnest(p_queue, p_state, p_ordering_hash)
        WITH ORDINALITY AS input(queue, state, ordering_hash, ord)
    LEFT JOIN awa.queue_meta AS meta ON meta.queue = input.queue;

    -- Claims go in unique-key order, so two producers with overlapping keys
    -- cannot wait on each other in opposite orders.
    IF p_skip_unique_conflicts THEN
        WITH claimed AS (
            INSERT INTO awa.job_unique_claims (unique_key, job_id)
            SELECT input.unique_key, v_job_ids[input.ord]
            FROM unnest(p_unique_key, p_unique_states, p_state)
                WITH ORDINALITY AS input(unique_key, unique_states, state, ord)
            WHERE input.unique_key IS NOT NULL
              AND input.unique_states IS NOT NULL
              AND awa.job_state_in_bitmask(input.unique_states::bit(8), input.state::awa.job_state)
            ORDER BY input.unique_key, input.ord
            ON CONFLICT (unique_key) DO NOTHING
            RETURNING job_id
        )
        SELECT COALESCE(array_agg(wanted.job_id), ARRAY[]::BIGINT[])
        INTO v_skipped
        FROM (
            SELECT v_job_ids[input.ord] AS job_id
            FROM unnest(p_unique_key, p_unique_states, p_state)
                WITH ORDINALITY AS input(unique_key, unique_states, state, ord)
            WHERE input.unique_key IS NOT NULL
              AND input.unique_states IS NOT NULL
              AND awa.job_state_in_bitmask(input.unique_states::bit(8), input.state::awa.job_state)
        ) AS wanted
        WHERE NOT EXISTS (SELECT 1 FROM claimed WHERE claimed.job_id = wanted.job_id);
    ELSE
        INSERT INTO awa.job_unique_claims (unique_key, job_id)
        SELECT input.unique_key, v_job_ids[input.ord]
        FROM unnest(p_unique_key, p_unique_states, p_state)
            WITH ORDINALITY AS input(unique_key, unique_states, state, ord)
        WHERE input.unique_key IS NOT NULL
          AND input.unique_states IS NOT NULL
          AND awa.job_state_in_bitmask(input.unique_states::bit(8), input.state::awa.job_state)
        ORDER BY input.unique_key, input.ord;
    END IF;

    -- Re-enqueues that are not yet due carry no lane.
    INSERT INTO deferred_jobs (
        job_id, kind, queue, args, state, priority, attempt, run_lease,
        max_attempts, run_at, attempted_at, finalized_at, created_at,
        unique_key, unique_states, payload
    )
    SELECT
        v_job_ids[input.ord],
        input.kind,
        input.queue,
        input.args,
        input.state::awa.job_state,
        input.priority,
        0,
        0,
        input.max_attempts,
        COALESCE(input.run_at, v_now),
        NULL,
        NULL,
        v_now,
        input.unique_key,
        input.unique_states,
        jsonb_build_object(
            'metadata', input.metadata,
            'tags', input.tags,
            'errors', '[]'::jsonb,
            'progress', NULL
        )
    FROM unnest(
        p_kind, p_queue, p_args, p_state, p_priority, p_max_attempts, p_run_at,
        p_metadata, p_tags, p_unique_key, p_unique_states
    ) WITH ORDINALITY AS input(
        kind, queue, args, state, priority, max_attempts, run_at,
        metadata, tags, unique_key, unique_states, ord
    )
    WHERE input.state <> 'available'
      AND v_job_ids[input.ord] <> ALL (v_skipped);

    -- The result rows do not depend on the lanes, so build them before the
    -- lock window opens.
    RETURN QUERY
    SELECT
        v_job_ids[input.ord],
        input.kind,
        input.queue,
        input.args,
        input.state::awa.job_state,
        input.priority,
        0::smallint,
        input.max_attempts,
        COALESCE(input.run_at, v_now),
        NULL::timestamptz,
        NULL::timestamptz,
        NULL::timestamptz,
        NULL::timestamptz,
        v_now,
        ARRAY[]::jsonb[],
        input.metadata,
        ARRAY(
            SELECT tag.value
            FROM jsonb_array_elements_text(input.tags) WITH ORDINALITY AS tag(value, tag_ord)
            ORDER BY tag.tag_ord
        ),
        input.unique_key,
        input.unique_states::bit(8),
        NULL::uuid,
        NULL::timestamptz,
        NULL::text,
        NULL::text,
        NULL::text,
        NULL::text,
        0::bigint,
        NULL::jsonb
    FROM unnest(
        p_kind, p_queue, p_args, p_state, p_priority, p_max_attempts, p_run_at,
        p_metadata, p_tags, p_unique_key, p_unique_states
    ) WITH ORDINALITY AS input(
        kind, queue, args, state, priority, max_attempts, run_at,
        metadata, tags, unique_key, unique_states, ord
    )
    WHERE v_job_ids[input.ord] <> ALL (v_skipped)
    ORDER BY input.ord;

    -- Lane lock window: in (queue, priority, shard) order, register each lane
    -- the batch is the first to use and reserve its sequence range once, then
    -- write the ready rows. The locks are held to the end of the transaction,
    -- which keeps the rows of a lane visible in lane_seq order.
    --
    -- A new lane is registered only when the loop reaches it, as every other
    -- producer does: the head tables' row trigger provisions the lane under
    -- its lock, and the uncommitted head row blocks other producers of that
    -- lane, so registering a later lane before reserving an earlier one would
    -- hold it out of order. Only missing rows are proposed, because the
    -- trigger fires before the conflict check.
    FOR v_lane IN
        SELECT lanes.queue, lanes.priority, lanes.shard, lanes.row_count
        FROM (
            SELECT input.queue, input.priority, v_shards[input.ord] AS shard,
                   count(*) AS row_count
            FROM unnest(p_queue, p_priority, p_state)
                WITH ORDINALITY AS input(queue, priority, state, ord)
            WHERE input.state = 'available'
              AND v_job_ids[input.ord] <> ALL (v_skipped)
            GROUP BY input.queue, input.priority, v_shards[input.ord]
        ) AS lanes
        ORDER BY lanes.queue COLLATE "C", lanes.priority, lanes.shard
    LOOP
        INSERT INTO queue_lanes (queue, priority)
        SELECT v_lane.queue, v_lane.priority
        WHERE NOT EXISTS (
            SELECT 1 FROM queue_lanes AS existing
            WHERE existing.queue = v_lane.queue AND existing.priority = v_lane.priority
        )
        ON CONFLICT ON CONSTRAINT queue_lanes_pkey DO NOTHING;

        INSERT INTO queue_enqueue_heads (queue, priority, enqueue_shard)
        SELECT v_lane.queue, v_lane.priority, v_lane.shard
        WHERE NOT EXISTS (
            SELECT 1 FROM queue_enqueue_heads AS existing
            WHERE existing.queue = v_lane.queue
              AND existing.priority = v_lane.priority
              AND existing.enqueue_shard = v_lane.shard
        )
        ON CONFLICT (queue, priority, enqueue_shard) DO NOTHING;

        INSERT INTO queue_claim_heads (queue, priority, enqueue_shard)
        SELECT v_lane.queue, v_lane.priority, v_lane.shard
        WHERE NOT EXISTS (
            SELECT 1 FROM queue_claim_heads AS existing
            WHERE existing.queue = v_lane.queue
              AND existing.priority = v_lane.priority
              AND existing.enqueue_shard = v_lane.shard
        )
        ON CONFLICT (queue, priority, enqueue_shard) DO NOTHING;

        EXECUTE format('SELECT %I.reserve_enqueue_seq($1, $2, $3, $4)', v_schema)
        INTO v_start
        USING v_lane.queue, v_lane.priority, v_lane.shard, v_lane.row_count::BIGINT;

        v_lane_queues := v_lane_queues || v_lane.queue;
        v_lane_priorities := v_lane_priorities || v_lane.priority;
        v_lane_shards := v_lane_shards || v_lane.shard;
        v_lane_starts := v_lane_starts || v_start;
    END LOOP;

    IF cardinality(v_lane_queues) > 0 THEN
        IF to_regprocedure(format('%I.ring_cursor(text)', v_schema)) IS NOT NULL THEN
            EXECUTE format('SELECT slot, generation FROM %I.ring_cursor(''queue'')', v_schema)
            INTO v_ready_slot, v_ready_generation;
        ELSE
            SELECT ring.current_slot, ring.generation
            INTO v_ready_slot, v_ready_generation
            FROM queue_ring_state AS ring
            WHERE ring.singleton = TRUE;
        END IF;

        INSERT INTO ready_entries (
            ready_slot, ready_generation, job_id, kind, queue, args, priority,
            attempt, run_lease, max_attempts, lane_seq, enqueue_shard, run_at,
            attempted_at, created_at, unique_key, unique_states, payload
        )
        SELECT
            v_ready_slot,
            v_ready_generation,
            v_job_ids[placed.ord],
            placed.kind,
            placed.queue,
            placed.args,
            placed.priority,
            0,
            0,
            placed.max_attempts,
            lane.start_seq + placed.lane_rank - 1,
            lane.shard,
            COALESCE(placed.run_at, v_now),
            NULL,
            v_now,
            placed.unique_key,
            placed.unique_states,
            jsonb_build_object(
                'metadata', placed.metadata,
                'tags', placed.tags,
                'errors', '[]'::jsonb,
                'progress', NULL
            )
        FROM (
            SELECT
                input.*,
                v_shards[input.ord] AS shard,
                row_number() OVER (
                    PARTITION BY input.queue, input.priority, v_shards[input.ord]
                    ORDER BY input.ord
                ) AS lane_rank
            FROM unnest(
                p_kind, p_queue, p_args, p_state, p_priority, p_max_attempts, p_run_at,
                p_metadata, p_tags, p_unique_key, p_unique_states
            ) WITH ORDINALITY AS input(
                kind, queue, args, state, priority, max_attempts, run_at,
                metadata, tags, unique_key, unique_states, ord
            )
            WHERE input.state = 'available'
              AND v_job_ids[input.ord] <> ALL (v_skipped)
        ) AS placed
        JOIN unnest(v_lane_queues, v_lane_priorities, v_lane_shards, v_lane_starts)
            AS lane(queue, priority, shard, start_seq)
          ON lane.queue = placed.queue
         AND lane.priority = placed.priority
         AND lane.shard = placed.shard;

        -- One segment per run of consecutive lane_seq values sharing a run_at.
        INSERT INTO ready_segments (
            ready_slot, ready_generation, queue, priority, enqueue_shard,
            first_lane_seq, next_lane_seq, first_run_at
        )
        SELECT
            v_ready_slot,
            v_ready_generation,
            runs.queue,
            runs.priority,
            runs.shard,
            min(runs.lane_seq),
            max(runs.lane_seq) + 1,
            runs.run_at
        FROM (
            SELECT
                placed.*,
                placed.lane_seq - row_number() OVER (
                    PARTITION BY placed.queue, placed.priority, placed.shard, placed.run_at
                    ORDER BY placed.lane_seq
                ) AS run_id
            FROM (
                SELECT
                    input.queue,
                    input.priority,
                    v_shards[input.ord] AS shard,
                    COALESCE(input.run_at, v_now) AS run_at,
                    lane.start_seq + row_number() OVER (
                        PARTITION BY input.queue, input.priority, v_shards[input.ord]
                        ORDER BY input.ord
                    ) - 1 AS lane_seq
                FROM unnest(p_queue, p_priority, p_state, p_run_at)
                    WITH ORDINALITY AS input(queue, priority, state, run_at, ord)
                JOIN unnest(v_lane_queues, v_lane_priorities, v_lane_shards, v_lane_starts)
                    AS lane(queue, priority, shard, start_seq)
                  ON lane.queue = input.queue
                 AND lane.priority = input.priority
                 AND lane.shard = v_shards[input.ord]
                WHERE input.state = 'available'
                  AND v_job_ids[input.ord] <> ALL (v_skipped)
            ) AS placed
        ) AS runs
        GROUP BY runs.queue, runs.priority, runs.shard, runs.run_at, runs.run_id
        ON CONFLICT (ready_slot, ready_generation, queue, priority, enqueue_shard, first_lane_seq)
            DO NOTHING;

        PERFORM pg_notify('awa:' || notified.queue, '')
        FROM (SELECT DISTINCT unnest(v_lane_queues) AS queue) AS notified;
    END IF;

    PERFORM set_config('search_path', v_old_search_path, true);
END;
$$ LANGUAGE plpgsql VOLATILE
SET search_path = pg_catalog, awa, public;

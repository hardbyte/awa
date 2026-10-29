-- v053: keep aged running jobs visible through awa.jobs.
-- Issue: https://github.com/hardbyte/awa/issues/569.
--
-- Claims and leases store effective priority after aging; ready entries keep
-- their original lane priority. Match the job and claimed attempt instead of
-- comparing those two priority domains. Exclude tombstoned ready entries so
-- reprioritization cannot duplicate a running row, and match run_lease so a
-- retained ready entry from an earlier attempt cannot supply its body.
--
-- N-1 compatibility: only refreshes awa.jobs_compat(), preserving its return
-- type and all storage/write protocols. Existing binaries keep using the same
-- view and claim functions; no new runtime capability or preflight is needed.
--
-- Lock impact: brief function-catalog and schema-version writes only, with no
-- hot-table DDL or data scan. Execution time is independent of queue depth.
-- Re-runnable and transaction-safe. External runners apply this bundle normally.

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
        WITH claim_cursors AS MATERIALIZED (
            -- One catalog read per lane instead of one per ready row
            -- (#422). sequence_next_value is VOLATILE, so spelled inline in
            -- the available-branch filter the planner could neither use it
            -- as an index bound nor cache it across rows: every
            -- ready_entries child was seq-scanned in full and each surviving
            -- row paid a dynamic-SQL catalog round-trip (~15x slower on a
            -- 20k-row backlog).
            SELECT
                claims.queue,
                claims.priority,
                claims.enqueue_shard,
                %1$I.sequence_next_value(claims.seq_name) AS claim_seq
            FROM %1$I.queue_claim_heads AS claims
        ),
        current_available AS (
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
            JOIN claim_cursors AS claims
              ON claims.queue = ready.queue
             AND claims.priority = ready.priority
             AND claims.enqueue_shard = ready.enqueue_shard
            WHERE ready.lane_seq >= claims.claim_seq
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
        -- Receipt claims that have not materialised into `leases` are
        -- running too (#246 / #416 / #422): legacy row-local `lease_claims`
        -- below, default compact `lease_claim_batches` members next. Each
        -- leg anti-joins every durable closure/supersession shape so a job
        -- reports at most one state. Mirrors admin::state_counts (#410) and
        -- queue_storage::open_receipt_running_claims_sql.
        SELECT
            claims.job_id AS id,
            ready.kind,
            ready.queue,
            ready.args,
            'running'::awa.job_state AS state,
            ready.priority,
            claims.attempt,
            claims.max_attempts,
            ready.run_at,
            attempt.heartbeat_at,
            claims.deadline_at,
            claims.claimed_at AS attempted_at,
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
            claims.run_lease,
            COALESCE(NULLIF(attempt.progress, 'null'::jsonb), NULLIF(ready.payload->'progress', 'null'::jsonb))
        FROM %1$I.lease_claims AS claims
        JOIN %1$I.ready_entries AS ready
          ON ready.ready_slot = claims.ready_slot
         AND ready.ready_generation = claims.ready_generation
         AND ready.queue = claims.queue
         AND ready.run_lease + 1 = claims.run_lease
         AND ready.enqueue_shard = claims.enqueue_shard
         AND ready.lane_seq = claims.lane_seq
         AND ready.job_id = claims.job_id
         AND NOT EXISTS (
             SELECT 1 FROM %1$I.ready_tombstones AS tomb
             WHERE tomb.ready_slot = ready.ready_slot
               AND tomb.ready_generation = ready.ready_generation
               AND tomb.queue = ready.queue
               AND tomb.priority = ready.priority
               AND tomb.enqueue_shard = ready.enqueue_shard
               AND tomb.lane_seq = ready.lane_seq
         )
        LEFT JOIN %1$I.attempt_state AS attempt
          ON attempt.job_id = claims.job_id
         AND attempt.run_lease = claims.run_lease
        WHERE claims.closed_at IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM %1$I.lease_claim_closures AS cx
              WHERE cx.claim_slot = claims.claim_slot
                AND cx.job_id = claims.job_id
                AND cx.run_lease = claims.run_lease
          )
          AND NOT EXISTS (
              SELECT 1 FROM %1$I.lease_claim_closure_batches AS cb
              WHERE cb.receipt_ranges @> claims.receipt_id
          )
          AND NOT EXISTS (
              SELECT 1 FROM %1$I.leases AS lease
              WHERE lease.job_id = claims.job_id
                AND lease.run_lease = claims.run_lease
          )
          AND NOT EXISTS (
              SELECT 1 FROM %1$I.done_entries AS done
              WHERE done.job_id = claims.job_id
                AND done.run_lease = claims.run_lease
          )
          AND NOT EXISTS (
              SELECT 1 FROM %1$I.deferred_jobs AS deferred
              WHERE deferred.job_id = claims.job_id
                AND deferred.run_lease = claims.run_lease
          )
          AND NOT EXISTS (
              SELECT 1 FROM %1$I.dlq_entries AS dlq
              WHERE dlq.job_id = claims.job_id
                AND dlq.run_lease = claims.run_lease
          )
        UNION ALL
        SELECT
            items.job_id AS id,
            ready.kind,
            ready.queue,
            ready.args,
            'running'::awa.job_state AS state,
            ready.priority,
            batches.attempts[items.ord],
            batches.max_attempts[items.ord],
            ready.run_at,
            attempt.heartbeat_at,
            batches.deadline_at,
            batches.claimed_at AS attempted_at,
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
            items.run_lease,
            COALESCE(NULLIF(attempt.progress, 'null'::jsonb), NULLIF(ready.payload->'progress', 'null'::jsonb))
        FROM %1$I.lease_claim_batches AS batches
        CROSS JOIN LATERAL unnest(
            batches.job_ids,
            batches.run_leases,
            batches.receipt_ids,
            batches.lane_seqs
        ) WITH ORDINALITY AS items(job_id, run_lease, receipt_id, lane_seq, ord)
        JOIN %1$I.ready_entries AS ready
          ON ready.ready_slot = batches.ready_slot
         AND ready.ready_generation = batches.ready_generation
         AND ready.queue = batches.queue
         AND ready.run_lease + 1 = items.run_lease
         AND ready.enqueue_shard = batches.enqueue_shard
         AND ready.lane_seq = items.lane_seq
         AND ready.job_id = items.job_id
         AND NOT EXISTS (
             SELECT 1 FROM %1$I.ready_tombstones AS tomb
             WHERE tomb.ready_slot = ready.ready_slot
               AND tomb.ready_generation = ready.ready_generation
               AND tomb.queue = ready.queue
               AND tomb.priority = ready.priority
               AND tomb.enqueue_shard = ready.enqueue_shard
               AND tomb.lane_seq = ready.lane_seq
         )
        LEFT JOIN %1$I.attempt_state AS attempt
          ON attempt.job_id = items.job_id
         AND attempt.run_lease = items.run_lease
        WHERE NOT EXISTS (
              SELECT 1 FROM %1$I.lease_claim_closures AS cx
              WHERE cx.claim_slot = batches.claim_slot
                AND cx.job_id = items.job_id
                AND cx.run_lease = items.run_lease
          )
          AND NOT EXISTS (
              SELECT 1 FROM %1$I.lease_claim_closure_batches AS cb
              WHERE cb.claim_slot = batches.claim_slot
                AND cb.receipt_ranges @> items.receipt_id
          )
          AND NOT EXISTS (
              SELECT 1 FROM %1$I.leases AS lease
              WHERE lease.job_id = items.job_id
                AND lease.run_lease = items.run_lease
          )
          AND NOT EXISTS (
              SELECT 1 FROM %1$I.done_entries AS done
              WHERE done.job_id = items.job_id
                AND done.run_lease = items.run_lease
          )
          AND NOT EXISTS (
              SELECT 1 FROM %1$I.deferred_jobs AS deferred
              WHERE deferred.job_id = items.job_id
                AND deferred.run_lease = items.run_lease
          )
          AND NOT EXISTS (
              SELECT 1 FROM %1$I.dlq_entries AS dlq
              WHERE dlq.job_id = items.job_id
                AND dlq.run_lease = items.run_lease
          )
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
         AND ready.run_lease + 1 = leases.run_lease
         AND ready.enqueue_shard = leases.enqueue_shard
         AND ready.lane_seq = leases.lane_seq
         AND ready.job_id = leases.job_id
         AND NOT EXISTS (
             SELECT 1 FROM %1$I.ready_tombstones AS tomb
             WHERE tomb.ready_slot = ready.ready_slot
               AND tomb.ready_generation = ready.ready_generation
               AND tomb.queue = ready.queue
               AND tomb.priority = ready.priority
               AND tomb.enqueue_shard = ready.enqueue_shard
               AND tomb.lane_seq = ready.lane_seq
         )
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

INSERT INTO awa.schema_version (version, description)
VALUES (53, 'Keep aged running jobs visible in jobs_compat()')
ON CONFLICT (version) DO NOTHING;

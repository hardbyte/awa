CREATE OR REPLACE FUNCTION @SCHEMA@.claim_ready_slot_@SLOT@(p_queue text, p_target_slot integer, p_target_generation bigint, p_lane_priority smallint, p_lane_shard smallint, p_lane_claim_seq bigint, p_claim_limit bigint, p_aging_secs double precision, p_claimed_at timestamp with time zone, p_deadline_at timestamp with time zone, p_deadline_secs double precision, p_lane_next_seq bigint)
 RETURNS TABLE(ready_slot integer, ready_generation bigint, lane_seq bigint, enqueue_shard smallint, lease_slot integer, lease_generation bigint, claim_slot integer, receipt_id bigint, claim_batch_id bigint, claim_batch_index integer, job_id bigint, kind text, queue text, args jsonb, lane_priority smallint, priority smallint, attempt smallint, run_lease bigint, max_attempts smallint, run_at timestamp with time zone, heartbeat_at timestamp with time zone, deadline_at timestamp with time zone, attempted_at timestamp with time zone, created_at timestamp with time zone, unique_key bytea, unique_states text, payload jsonb, lane_drained boolean)
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog', 'awa', 'pg_temp'
AS $function$
        DECLARE
            v_lane_shard SMALLINT := p_lane_shard;
            v_claimed_at TIMESTAMPTZ := p_claimed_at;
            v_deadline_at TIMESTAMPTZ := p_deadline_at;
        BEGIN
            RETURN QUERY
            WITH lease_ring AS MATERIALIZED (
                -- #371: current lease cursor via the authority resolver
                -- (columns in compat mode, max-generation ledger row in
                -- ledger mode). MATERIALIZED pins it to a single evaluation:
                -- this single-reference CTE would otherwise be inlined and
                -- re-run the resolver once per claimed row (loops=N) on the
                -- inner side of the final CROSS JOIN. The pre-ledger
                -- lease_ring_state singleton was one cached read, so per-row
                -- re-execution regressed claim yield; materialized, loops=1.
                SELECT slot AS lease_slot, generation AS lease_generation
                FROM @SCHEMA@.ring_cursor('lease')
            ),
            selected AS (
                SELECT
                    ready.ready_slot,
                    ready.ready_generation,
                    ready.job_id,
                    ready.kind,
                    ready.queue,
                    ready.args,
                    ready.priority AS lane_priority,
                    CASE
                        WHEN p_aging_secs > 0 THEN GREATEST(
                            1,
                            ready.priority - FLOOR(
                                EXTRACT(EPOCH FROM (clock_timestamp() - ready.run_at)) / p_aging_secs
                            )::smallint
                        )::smallint
                        ELSE ready.priority
                    END AS effective_priority,
                    ready.attempt,
                    ready.run_lease,
                    ready.max_attempts,
                    ready.lane_seq,
                    ready.run_at,
                    ready.created_at,
                    ready.unique_key,
                    ready.unique_states,
                    COALESCE(ready.payload, '{}'::jsonb) AS payload,
                    EXISTS (
                        SELECT 1 FROM @SCHEMA@.ready_tombstones_@SLOT@ AS tomb
                        WHERE tomb.ready_slot = p_target_slot
                          AND tomb.ready_generation = p_target_generation
                          AND tomb.queue = ready.queue
                          AND tomb.priority = ready.priority
                          AND tomb.enqueue_shard = ready.enqueue_shard
                          AND tomb.lane_seq = ready.lane_seq
                        -- Keep this bounded claim on indexed probes, not a full-partition hash.
                        OFFSET 0
                    ) AS is_tombstone
                FROM @SCHEMA@.ready_entries_@SLOT@ AS ready
                WHERE ready.queue = p_queue
                  AND ready.priority = p_lane_priority
                  AND ready.enqueue_shard = p_lane_shard
                  AND ready.ready_slot = p_target_slot
                  AND ready.ready_generation = p_target_generation
                  AND ready.lane_seq >= p_lane_claim_seq
                ORDER BY ready.lane_seq ASC
                LIMIT p_claim_limit
            ),
            selected_with_spent AS (
                SELECT
                    selected.*,
                    selected.is_tombstone AS attempt_spent
                FROM selected
            ),
            
            claim_ring AS MATERIALIZED (
                -- #371: current claim cursor via the authority resolver
                -- (columns in compat mode, max-generation ledger row in
                -- ledger mode). MATERIALIZED pins it to a single evaluation:
                -- this single-reference CTE would otherwise be inlined and
                -- re-run the resolver once per selected row (loops=N) on the
                -- inner side of the claim_attempt_batches nested loop. The
                -- pre-ledger claim_ring_state singleton was one cached read,
                -- so the per-row re-execution regressed claim yield; one
                -- authority-row read + one O(1) cursor read, materialized,
                -- keeps loops=1.
                SELECT slot AS claim_slot
                FROM @SCHEMA@.ring_cursor('claim')
            ),
            claim_attempt_batches AS (
                INSERT INTO @SCHEMA@.ready_claim_attempt_batches_@SLOT@ AS attempt_rows (
                    ready_slot,
                    ready_generation,
                    queue,
                    priority,
                    enqueue_shard,
                    claim_slot,
                    first_lane_seq,
                    next_lane_seq,
                    claimed_count,
                    lane_ranges,
                    claimed_at
                )
                SELECT
                    selected.ready_slot,
                    selected.ready_generation,
                    selected.queue,
                    selected.lane_priority,
                    v_lane_shard,
                    claim_ring.claim_slot,
                    min(selected.lane_seq)::bigint AS first_lane_seq,
                    (max(selected.lane_seq) + 1)::bigint AS next_lane_seq,
                    count(*)::int AS claimed_count,
                    range_agg(int8range(selected.lane_seq, selected.lane_seq + 1, '[)') ORDER BY selected.lane_seq)::int8multirange AS lane_ranges,
                    v_claimed_at
                FROM selected_with_spent AS selected
                CROSS JOIN claim_ring
                WHERE NOT selected.attempt_spent
                GROUP BY
                    selected.ready_slot,
                    selected.ready_generation,
                    selected.queue,
                    selected.lane_priority,
                    claim_ring.claim_slot
                RETURNING
                    attempt_rows.claim_slot,
                    attempt_rows.ready_slot,
                    attempt_rows.ready_generation,
                    attempt_rows.queue,
                    attempt_rows.priority AS lane_priority,
                    attempt_rows.enqueue_shard,
                    attempt_rows.first_lane_seq,
                    attempt_rows.next_lane_seq,
                    attempt_rows.lane_ranges
            ),
            claim_items AS MATERIALIZED (
                SELECT
                    attempts.claim_slot,
                    selected.ready_slot,
                    selected.ready_generation,
                    selected.job_id,
                    selected.queue,
                    selected.effective_priority AS priority,
                    selected.lane_seq,
                    v_lane_shard AS enqueue_shard,
                    (selected.attempt + 1)::smallint AS attempt,
                    (selected.run_lease + 1)::bigint AS run_lease,
                    selected.max_attempts::smallint AS max_attempts,
                    nextval('@SCHEMA@.lease_claim_receipt_id_seq'::regclass)::bigint AS receipt_id,
                    row_number() OVER (
                        PARTITION BY
                            attempts.claim_slot,
                            selected.ready_slot,
                            selected.ready_generation,
                            selected.queue,
                            selected.effective_priority,
                            v_lane_shard
                        ORDER BY selected.lane_seq, selected.job_id
                    )::int AS claim_batch_index
                FROM selected_with_spent AS selected
                JOIN claim_attempt_batches AS attempts
                  ON attempts.ready_slot = selected.ready_slot
                 AND attempts.ready_generation = selected.ready_generation
                 AND attempts.queue = selected.queue
                 AND attempts.lane_priority = selected.lane_priority
                 AND attempts.enqueue_shard = v_lane_shard
                 AND attempts.first_lane_seq <= selected.lane_seq
                 AND attempts.next_lane_seq > selected.lane_seq
                 AND attempts.lane_ranges @> int8range(selected.lane_seq, selected.lane_seq + 1, '[)')
                WHERE NOT selected.attempt_spent
            ),
            compact_claim_batches AS (
                INSERT INTO @SCHEMA@.lease_claim_batches AS claim_batches (
                    claim_slot,
                    ready_slot,
                    ready_generation,
                    queue,
                    priority,
                    enqueue_shard,
                    claimed_count,
                    job_ids,
                    run_leases,
                    receipt_ids,
                    receipt_ranges,
                    lane_seqs,
                    attempts,
                    max_attempts,
                    deadline_at,
                    claimed_at
                )
                SELECT
                    claim_items.claim_slot,
                    claim_items.ready_slot,
                    claim_items.ready_generation,
                    claim_items.queue,
                    claim_items.priority,
                    claim_items.enqueue_shard,
                    count(*)::int AS claimed_count,
                    array_agg(claim_items.job_id ORDER BY claim_items.lane_seq, claim_items.job_id),
                    array_agg(claim_items.run_lease ORDER BY claim_items.lane_seq, claim_items.job_id),
                    array_agg(claim_items.receipt_id ORDER BY claim_items.lane_seq, claim_items.job_id),
                    range_agg(int8range(claim_items.receipt_id, claim_items.receipt_id + 1, '[)') ORDER BY claim_items.receipt_id),
                    array_agg(claim_items.lane_seq ORDER BY claim_items.lane_seq, claim_items.job_id),
                    array_agg(claim_items.attempt ORDER BY claim_items.lane_seq, claim_items.job_id),
                    array_agg(claim_items.max_attempts ORDER BY claim_items.lane_seq, claim_items.job_id),
                    v_deadline_at,
                    v_claimed_at
                FROM claim_items
                GROUP BY
                    claim_items.claim_slot,
                    claim_items.ready_slot,
                    claim_items.ready_generation,
                    claim_items.queue,
                    claim_items.priority,
                    claim_items.enqueue_shard
                RETURNING
                    claim_batches.claim_slot,
                    claim_batches.batch_id,
                    claim_batches.ready_slot,
                    claim_batches.ready_generation,
                    claim_batches.queue,
                    claim_batches.priority,
                    claim_batches.enqueue_shard
            ),
            claimed AS (
                SELECT
                    claim_items.claim_slot,
                    claim_items.receipt_id,
                    batches.batch_id AS claim_batch_id,
                    claim_items.claim_batch_index,
                    claim_items.ready_slot,
                    claim_items.ready_generation,
                    claim_items.job_id,
                    claim_items.queue,
                    claim_items.priority,
                    claim_items.lane_seq,
                    claim_items.attempt,
                    claim_items.run_lease,
                    claim_items.max_attempts
                FROM claim_items
                JOIN compact_claim_batches AS batches
                  ON batches.claim_slot = claim_items.claim_slot
                 AND batches.ready_slot = claim_items.ready_slot
                 AND batches.ready_generation = claim_items.ready_generation
                 AND batches.queue = claim_items.queue
                 AND batches.priority = claim_items.priority
                 AND batches.enqueue_shard = claim_items.enqueue_shard
            )
            
            SELECT
                claimed.ready_slot,
                claimed.ready_generation,
                claimed.lane_seq,
                p_lane_shard AS enqueue_shard,
                lease_ring.lease_slot,
                lease_ring.lease_generation,
                claimed.claim_slot,
                claimed.receipt_id,
                claimed.claim_batch_id,
                claimed.claim_batch_index,
                selected.job_id,
                selected.kind,
                selected.queue,
                selected.args,
                selected.lane_priority,
                selected.effective_priority,
                claimed.attempt,
                claimed.run_lease,
                claimed.max_attempts,
                selected.run_at,
                CASE
                    WHEN p_deadline_secs > 0 THEN p_claimed_at
                    ELSE NULL::timestamptz
                END AS heartbeat_at,
                p_deadline_at AS deadline_at,
                CASE
                    WHEN p_deadline_secs > 0 THEN p_claimed_at
                    ELSE NULL::timestamptz
                END AS attempted_at,
                selected.created_at,
                selected.unique_key,
                selected.unique_states,
                selected.payload,
                (p_lane_claim_seq + p_claim_limit >= p_lane_next_seq) AS lane_drained
            FROM claimed
            CROSS JOIN lease_ring
            JOIN selected
             ON selected.ready_slot = claimed.ready_slot
             AND selected.ready_generation = claimed.ready_generation
             AND selected.queue = claimed.queue
             AND selected.effective_priority = claimed.priority
             AND selected.lane_seq = claimed.lane_seq
            ORDER BY selected.lane_seq ASC;

        END;
        $function$

CREATE OR REPLACE FUNCTION __SCHEMA__.claim_ready_runtime(p_queue text, p_max_batch bigint, p_deadline_secs double precision, p_aging_secs double precision)
 RETURNS TABLE(ready_slot integer, ready_generation bigint, lane_seq bigint, enqueue_shard smallint, lease_slot integer, lease_generation bigint, claim_slot integer, receipt_id bigint, claim_batch_id bigint, claim_batch_index integer, job_id bigint, kind text, queue text, args jsonb, lane_priority smallint, priority smallint, attempt smallint, run_lease bigint, max_attempts smallint, run_at timestamp with time zone, heartbeat_at timestamp with time zone, deadline_at timestamp with time zone, attempted_at timestamp with time zone, created_at timestamp with time zone, unique_key bytea, unique_states text, payload jsonb)
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog', 'awa', 'public'
AS $function$
        DECLARE
            v_lane_priority SMALLINT;
            v_lane_shard SMALLINT;
            v_claim_seq_name TEXT;
            v_lane_claim_seq BIGINT;
            v_lane_next_seq BIGINT;
            v_claim_limit BIGINT;
            v_claimed_count BIGINT;
            v_target_slot INT;
            v_target_generation BIGINT;
            v_target_next_lane_seq BIGINT;
            v_head_job_id BIGINT;
            v_head_run_lease BIGINT;
            v_head_lane_seq BIGINT;
            v_claimed_at TIMESTAMPTZ;
            v_deadline_at TIMESTAMPTZ;
            v_head_attempt_spent BOOLEAN;
            v_spent_next_seq BIGINT;
        BEGIN
            SELECT
                claims.priority,
                claims.enqueue_shard,
                claims.seq_name,
                cursors.claim_seq,
                cursors.next_seq,
                candidate.ready_slot,
                candidate.ready_generation,
                candidate.next_lane_seq
            INTO
                v_lane_priority,
                v_lane_shard,
                v_claim_seq_name,
                v_lane_claim_seq,
                v_lane_next_seq,
                v_target_slot,
                v_target_generation,
                v_target_next_lane_seq
            FROM __SCHEMA__.queue_claim_heads AS claims
            JOIN __SCHEMA__.queue_enqueue_heads AS enqueues
              ON enqueues.queue = claims.queue
             AND enqueues.priority = claims.priority
             AND enqueues.enqueue_shard = claims.enqueue_shard
            CROSS JOIN LATERAL (
                SELECT
                    __SCHEMA__.sequence_next_value(claims.seq_name) AS claim_seq,
                    __SCHEMA__.sequence_next_value(enqueues.seq_name) AS next_seq
            ) AS cursors
            LEFT JOIN LATERAL (
                WITH first_segment AS MATERIALIZED (
                    SELECT
                        segment.ready_slot,
                        segment.ready_generation,
                        segment.first_run_at
                    FROM __SCHEMA__.ready_segments AS segment
                    WHERE segment.queue = p_queue
                      AND segment.priority = claims.priority
                      AND segment.enqueue_shard = claims.enqueue_shard
                      AND segment.next_lane_seq > cursors.claim_seq
                    -- Ready segments are non-overlapping and visibility-ordered
                    -- (aborted sequence reservations can leave gaps, so they are
                    -- not strictly contiguous). The smallest next_lane_seq >
                    -- claim_seq is therefore the next segment covering or after
                    -- the cursor; the later ready_entries lookup validates the
                    -- target row, so a gap just falls through to the next real
                    -- entry rather than emitting phantom work. Ordering by
                    -- next_lane_seq lets the
                    -- (queue, priority, enqueue_shard, next_lane_seq, ...) index
                    -- short-circuit at LIMIT 1 instead of materialising and
                    -- sorting the whole tail (which grows with the claim cursor).
                    ORDER BY segment.next_lane_seq ASC
                    LIMIT 1
                )
                SELECT
                    first_segment.ready_slot,
                    first_segment.ready_generation,
                    slot_bound.next_lane_seq,
                    first_segment.first_run_at AS run_at
                FROM first_segment
                CROSS JOIN LATERAL (
                    SELECT max(segment.next_lane_seq)::bigint AS next_lane_seq
                    FROM __SCHEMA__.ready_segments AS segment
                    WHERE segment.ready_slot = first_segment.ready_slot
                      AND segment.ready_generation = first_segment.ready_generation
                      AND segment.queue = p_queue
                      AND segment.priority = claims.priority
                      AND segment.enqueue_shard = claims.enqueue_shard
                ) AS slot_bound
            ) AS segment_candidate ON TRUE
            CROSS JOIN LATERAL (
                SELECT
                    segment_candidate.ready_slot AS ready_slot,
                    segment_candidate.ready_generation AS ready_generation,
                    segment_candidate.next_lane_seq AS next_lane_seq,
                    segment_candidate.run_at AS run_at
            ) AS candidate
            WHERE claims.queue = p_queue
              AND NOT EXISTS (
                  SELECT 1
                  FROM awa.queue_meta AS meta
                  WHERE meta.queue = p_queue
                    AND meta.paused = TRUE
              )
              AND cursors.claim_seq < cursors.next_seq
            ORDER BY
                CASE
                    WHEN candidate.run_at IS NULL THEN NULL::smallint
                    WHEN p_aging_secs > 0 THEN GREATEST(
                        1,
                        claims.priority - FLOOR(
                            EXTRACT(EPOCH FROM (clock_timestamp() - candidate.run_at)) / p_aging_secs
                        )::smallint
                    )::smallint
                    ELSE claims.priority
                END ASC NULLS LAST,
                candidate.run_at ASC NULLS LAST,
                claims.priority ASC
            LIMIT 1
            FOR UPDATE OF claims SKIP LOCKED;

            IF NOT FOUND THEN
                RETURN;
            END IF;

            IF v_target_slot IS NULL THEN
                SELECT
                    segment.ready_slot,
                    segment.ready_generation,
                    slot_bound.next_lane_seq
                INTO
                    v_target_slot,
                    v_target_generation,
                    v_target_next_lane_seq
                FROM (
                    SELECT
                        segment.ready_slot,
                        segment.ready_generation,
                        segment.first_lane_seq
                    FROM __SCHEMA__.ready_segments AS segment
                    WHERE segment.queue = p_queue
                      AND segment.priority = v_lane_priority
                      AND segment.enqueue_shard = v_lane_shard
                      AND segment.next_lane_seq > v_lane_claim_seq
                    -- Order by next_lane_seq (non-overlapping segments, so this
                    -- is the next segment covering or after the cursor) for the
                    -- index short-circuit; see the inline routing probe above.
                    ORDER BY segment.next_lane_seq ASC
                    LIMIT 1
                ) AS segment
                CROSS JOIN LATERAL (
                    SELECT max(later_segment.next_lane_seq)::bigint AS next_lane_seq
                    FROM __SCHEMA__.ready_segments AS later_segment
                    WHERE later_segment.ready_slot = segment.ready_slot
                      AND later_segment.ready_generation = segment.ready_generation
                      AND later_segment.queue = p_queue
                      AND later_segment.priority = v_lane_priority
                      AND later_segment.enqueue_shard = v_lane_shard
                ) AS slot_bound;
            END IF;

            IF v_target_slot IS NULL THEN
                -- No committed ready segment is visible yet. The enqueue
                -- sequence may include uncommitted reservations, so do not
                -- advance the claim cursor.
                RETURN;
            END IF;

            SELECT
                ready.job_id,
                ready.run_lease,
                ready.lane_seq
            INTO
                v_head_job_id,
                v_head_run_lease,
                v_head_lane_seq
            FROM __SCHEMA__.ready_entries AS ready
            WHERE ready.queue = p_queue
              AND ready.priority = v_lane_priority
              AND ready.enqueue_shard = v_lane_shard
              AND ready.ready_slot = v_target_slot
              AND ready.ready_generation = v_target_generation
              AND ready.lane_seq >= v_lane_claim_seq
            ORDER BY ready.lane_seq ASC
            LIMIT 1;

            IF NOT FOUND THEN
                -- The segment row is committed by the same transaction as
                -- the ready rows, so this should only happen during a rolling
                -- upgrade or after manual catalog damage. Keep the claim
                -- cursor conservative rather than advancing over unseen work.
                RETURN;
            END IF;

            -- The claim head stays a cold lane registry: the ready
            -- slot/generation is resolved from ready_segments on every claim
            -- rather than cached back onto this row. Caching it here
            -- re-introduced a per-claim UPDATE whose dead tuples could not be
            -- reclaimed under a pinned MVCC snapshot, making queue_claim_heads
            -- the dominant dead-tuple accumulator during a long-running
            -- transaction. The ready_segments scan above is the sole routing
            -- source now.

            v_claim_limit := LEAST(GREATEST(v_lane_next_seq - v_lane_claim_seq, 0), p_max_batch);
            IF v_claim_limit <= 0 THEN
                RETURN;
            END IF;

            v_claimed_at := clock_timestamp();
            IF p_deadline_secs > 0 THEN
                v_deadline_at := v_claimed_at + make_interval(secs => p_deadline_secs);
            ELSE
                v_deadline_at := NULL::timestamptz;
            END IF;

            SELECT EXISTS (
                SELECT 1 FROM __SCHEMA__.ready_tombstones AS tomb
                WHERE tomb.ready_slot = v_target_slot
                  AND tomb.ready_generation = v_target_generation
                  AND tomb.queue = p_queue
                  AND tomb.priority = v_lane_priority
                  AND tomb.enqueue_shard = v_lane_shard
                  AND tomb.lane_seq = v_head_lane_seq
                LIMIT 1
            )
            INTO v_head_attempt_spent;

            IF NOT v_head_attempt_spent THEN
                SELECT EXISTS (
                    SELECT 1
                    FROM __SCHEMA__.ready_claim_attempt_batches AS existing_attempts
                    WHERE existing_attempts.ready_slot = v_target_slot
                      AND existing_attempts.ready_generation = v_target_generation
                      AND existing_attempts.queue = p_queue
                      AND existing_attempts.priority = v_lane_priority
                      AND existing_attempts.enqueue_shard = v_lane_shard
                      AND existing_attempts.first_lane_seq <= v_head_lane_seq
                      AND existing_attempts.next_lane_seq > v_head_lane_seq
                      AND existing_attempts.lane_ranges @> int8range(v_head_lane_seq, v_head_lane_seq + 1, '[)')
                    LIMIT 1
                )
                INTO v_head_attempt_spent;
            END IF;

            IF NOT v_head_attempt_spent AND NOT 't'::boolean THEN
                SELECT EXISTS (
                    SELECT 1
                    FROM __SCHEMA__.lease_claims AS existing_claims
                    WHERE existing_claims.job_id = v_head_job_id
                      AND existing_claims.run_lease = v_head_run_lease + 1
                    LIMIT 1
                )
                INTO v_head_attempt_spent;

                IF NOT v_head_attempt_spent THEN
                    SELECT EXISTS (
                        SELECT 1
                        FROM __SCHEMA__.lease_claim_closures AS existing_closures
                        WHERE existing_closures.job_id = v_head_job_id
                          AND existing_closures.run_lease = v_head_run_lease + 1
                        LIMIT 1
                    )
                    INTO v_head_attempt_spent;
                END IF;

                IF NOT v_head_attempt_spent THEN
                    SELECT EXISTS (
                        SELECT 1
                        FROM __SCHEMA__.receipt_completion_batches AS existing_batches
                        WHERE existing_batches.ready_slot = v_target_slot
                          AND existing_batches.ready_generation = v_target_generation
                          AND existing_batches.queue = p_queue
                          AND existing_batches.priority = v_lane_priority
                          AND existing_batches.enqueue_shard = v_lane_shard
                          AND existing_batches.job_ids @> ARRAY[v_head_job_id]::bigint[]
                          AND EXISTS (
                              SELECT 1
                              FROM unnest(
                                  existing_batches.job_ids,
                                  existing_batches.run_leases
                              ) AS completed(job_id, run_lease)
                              WHERE completed.job_id = v_head_job_id
                                AND completed.run_lease = v_head_run_lease + 1
                          )
                        LIMIT 1
                    )
                    INTO v_head_attempt_spent;
                END IF;

                IF NOT v_head_attempt_spent THEN
                    SELECT EXISTS (
                        SELECT 1
                        FROM __SCHEMA__.leases AS existing_leases
                        WHERE existing_leases.job_id = v_head_job_id
                          AND existing_leases.run_lease = v_head_run_lease + 1
                        LIMIT 1
                    )
                    INTO v_head_attempt_spent;
                END IF;

                IF NOT v_head_attempt_spent THEN
                    SELECT EXISTS (
                        SELECT 1
                        FROM __SCHEMA__.done_entries AS existing_done
                        WHERE existing_done.job_id = v_head_job_id
                          AND existing_done.run_lease = v_head_run_lease + 1
                        LIMIT 1
                    )
                    INTO v_head_attempt_spent;
                END IF;

                IF NOT v_head_attempt_spent THEN
                    SELECT EXISTS (
                        SELECT 1
                        FROM __SCHEMA__.deferred_jobs AS existing_deferred
                        WHERE existing_deferred.job_id = v_head_job_id
                          AND existing_deferred.run_lease = v_head_run_lease + 1
                        LIMIT 1
                    )
                    INTO v_head_attempt_spent;
                END IF;

                IF NOT v_head_attempt_spent THEN
                    SELECT EXISTS (
                        SELECT 1
                        FROM __SCHEMA__.dlq_entries AS existing_dlq
                        WHERE existing_dlq.job_id = v_head_job_id
                          AND existing_dlq.run_lease = v_head_run_lease + 1
                        LIMIT 1
                    )
                    INTO v_head_attempt_spent;
                END IF;

                IF NOT v_head_attempt_spent THEN
                    SELECT EXISTS (
                        SELECT 1
                        FROM __SCHEMA__.ready_entries AS existing_ready
                        WHERE existing_ready.job_id = v_head_job_id
                          AND existing_ready.run_lease = v_head_run_lease + 1
                        LIMIT 1
                    )
                    INTO v_head_attempt_spent;
                END IF;
            END IF;

            IF COALESCE(v_head_attempt_spent, FALSE) THEN
                WITH selected AS (
                SELECT
                    ready.job_id,
                    ready.run_lease,
                    ready.lane_seq,
                    EXISTS (
                        SELECT 1 FROM __SCHEMA__.ready_tombstones AS tomb
                        WHERE tomb.ready_slot = v_target_slot
                          AND tomb.ready_generation = v_target_generation
                          AND tomb.queue = ready.queue
                          AND tomb.priority = ready.priority
                          AND tomb.enqueue_shard = ready.enqueue_shard
                          AND tomb.lane_seq = ready.lane_seq
                        -- Keep this bounded claim on indexed probes, not a full-partition hash.
                        OFFSET 0
                    ) AS is_tombstone
                FROM __SCHEMA__.ready_entries AS ready
                WHERE ready.queue = p_queue
                  AND ready.priority = v_lane_priority
                  AND ready.enqueue_shard = v_lane_shard
                  AND ready.ready_slot = v_target_slot
                  AND ready.ready_generation = v_target_generation
                  AND ready.lane_seq >= v_lane_claim_seq
                ORDER BY ready.lane_seq ASC
                LIMIT v_claim_limit
            ),
            selected_with_spent AS (
                SELECT
                    selected.*,
                    (
                        selected.is_tombstone
                        OR EXISTS (
                            SELECT 1
                            FROM __SCHEMA__.ready_claim_attempt_batches AS existing_attempts
                            WHERE existing_attempts.ready_slot = v_target_slot
                              AND existing_attempts.ready_generation = v_target_generation
                              AND existing_attempts.queue = p_queue
                              AND existing_attempts.priority = v_lane_priority
                              AND existing_attempts.enqueue_shard = v_lane_shard
                              AND existing_attempts.first_lane_seq <= selected.lane_seq
                              AND existing_attempts.next_lane_seq > selected.lane_seq
                              AND existing_attempts.lane_ranges @> int8range(selected.lane_seq, selected.lane_seq + 1, '[)')
                        )
                        OR (
                            NOT 't'::boolean
                            AND (
                                EXISTS (
                                    SELECT 1
                                    FROM __SCHEMA__.lease_claims AS existing_claims
                                    WHERE existing_claims.job_id = selected.job_id
                                      AND existing_claims.run_lease = selected.run_lease + 1
                                )
                                OR EXISTS (
                                    SELECT 1
                                    FROM __SCHEMA__.lease_claim_closures AS existing_closures
                                    WHERE existing_closures.job_id = selected.job_id
                                      AND existing_closures.run_lease = selected.run_lease + 1
                                )
                                OR EXISTS (
                                    SELECT 1
                                    FROM __SCHEMA__.receipt_completion_batches AS existing_batches
                                    WHERE existing_batches.ready_slot = v_target_slot
                                      AND existing_batches.ready_generation = v_target_generation
                                      AND existing_batches.queue = p_queue
                                      AND existing_batches.priority = v_lane_priority
                                      AND existing_batches.enqueue_shard = v_lane_shard
                                      AND existing_batches.job_ids @> ARRAY[selected.job_id]::bigint[]
                                      AND EXISTS (
                                          SELECT 1
                                          FROM unnest(
                                              existing_batches.job_ids,
                                              existing_batches.run_leases
                                          ) AS completed(job_id, run_lease)
                                          WHERE completed.job_id = selected.job_id
                                            AND completed.run_lease = selected.run_lease + 1
                                      )
                                )
                                OR EXISTS (
                                    SELECT 1
                                    FROM __SCHEMA__.leases AS existing_leases
                                    WHERE existing_leases.job_id = selected.job_id
                                      AND existing_leases.run_lease = selected.run_lease + 1
                                )
                                OR EXISTS (
                                    SELECT 1
                                    FROM __SCHEMA__.done_entries AS existing_done
                                    WHERE existing_done.job_id = selected.job_id
                                      AND existing_done.run_lease = selected.run_lease + 1
                                )
                                OR EXISTS (
                                    SELECT 1
                                    FROM __SCHEMA__.deferred_jobs AS existing_deferred
                                    WHERE existing_deferred.job_id = selected.job_id
                                      AND existing_deferred.run_lease = selected.run_lease + 1
                                )
                                OR EXISTS (
                                    SELECT 1
                                    FROM __SCHEMA__.dlq_entries AS existing_dlq
                                    WHERE existing_dlq.job_id = selected.job_id
                                      AND existing_dlq.run_lease = selected.run_lease + 1
                                )
                                OR EXISTS (
                                    SELECT 1
                                    FROM __SCHEMA__.ready_entries AS existing_ready
                                    WHERE existing_ready.job_id = selected.job_id
                                      AND existing_ready.run_lease = selected.run_lease + 1
                                )
                            )
                        )
                    ) AS attempt_spent
                FROM selected
            ),
            spent_prefix AS (
                SELECT
                    selected_with_spent.lane_seq,
                    selected_with_spent.lane_seq
                        - row_number() OVER (ORDER BY selected_with_spent.lane_seq)::bigint AS prefix_group
                FROM selected_with_spent
                WHERE attempt_spent
            )
            SELECT max(spent_prefix.lane_seq) + 1
            INTO v_spent_next_seq
            FROM spent_prefix
            WHERE prefix_group = v_lane_claim_seq - 1;

                IF v_spent_next_seq IS NOT NULL THEN
                    -- Safe in-transaction sequence movement: only skip a
                    -- contiguous prefix of attempts with already-committed
                    -- evidence. Later spent/tombstoned lanes cannot move the
                    -- cursor past an earlier live row, because the surrounding
                    -- claim transaction could still abort.
                    PERFORM __SCHEMA__.set_sequence_next(v_claim_seq_name, v_spent_next_seq);
                END IF;

                -- The spent-prefix path is a recovery/maintenance path for
                -- stale claim cursors, tombstones, or already-emitted
                -- attempts. Return after moving the cursor so the common
                -- claim query can stay narrow: it only needs to filter
                -- ready_tombstones and does not plan every terminal evidence
                -- relation on every hot-path claim.
                RETURN;
            END IF;

            RETURN QUERY
            WITH lease_ring AS (
                SELECT current_slot AS lease_slot, generation AS lease_generation
                FROM __SCHEMA__.lease_ring_state
                WHERE singleton = TRUE
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
                        SELECT 1 FROM __SCHEMA__.ready_tombstones AS tomb
                        WHERE tomb.ready_slot = v_target_slot
                          AND tomb.ready_generation = v_target_generation
                          AND tomb.queue = ready.queue
                          AND tomb.priority = ready.priority
                          AND tomb.enqueue_shard = ready.enqueue_shard
                          AND tomb.lane_seq = ready.lane_seq
                        -- Keep this bounded claim on indexed probes, not a full-partition hash.
                        OFFSET 0
                    ) AS is_tombstone
                FROM __SCHEMA__.ready_entries AS ready
                WHERE ready.queue = p_queue
                  AND ready.priority = v_lane_priority
                  AND ready.enqueue_shard = v_lane_shard
                  AND ready.ready_slot = v_target_slot
                  AND ready.ready_generation = v_target_generation
                  AND ready.lane_seq >= v_lane_claim_seq
                ORDER BY ready.lane_seq ASC
                LIMIT v_claim_limit
            ),
            selected_with_spent AS (
                SELECT
                    selected.*,
                    selected.is_tombstone AS attempt_spent
                FROM selected
            ),
            
            claim_ring AS (
                SELECT current_slot AS claim_slot
                FROM __SCHEMA__.claim_ring_state
                WHERE singleton = TRUE
            ),
            claim_attempt_batches AS (
                INSERT INTO __SCHEMA__.ready_claim_attempt_batches AS attempt_rows (
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
                    nextval('__SCHEMA__.lease_claim_receipt_id_seq'::regclass)::bigint AS receipt_id,
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
                INSERT INTO __SCHEMA__.lease_claim_batches AS claim_batches (
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
                    NULL::timestamptz,
                    v_claimed_at
                FROM claim_items
                WHERE p_deadline_secs <= 0
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
            row_claims AS (
                INSERT INTO __SCHEMA__.lease_claims AS claim_rows (
                    claim_slot,
                    receipt_id,
                    job_id,
                    run_lease,
                    ready_slot,
                    ready_generation,
                    queue,
                    priority,
                    attempt,
                    max_attempts,
                    lane_seq,
                    enqueue_shard,
                    deadline_at
                )
                SELECT
                    claim_items.claim_slot,
                    claim_items.receipt_id,
                    claim_items.job_id,
                    claim_items.run_lease,
                    claim_items.ready_slot,
                    claim_items.ready_generation,
                    claim_items.queue,
                    claim_items.priority,
                    claim_items.attempt,
                    claim_items.max_attempts,
                    claim_items.lane_seq,
                    claim_items.enqueue_shard,
                    v_deadline_at
                FROM claim_items
                WHERE p_deadline_secs > 0
                RETURNING
                    claim_rows.claim_slot,
                    claim_rows.receipt_id,
                    claim_rows.ready_slot,
                    claim_rows.ready_generation,
                    claim_rows.job_id,
                    claim_rows.queue,
                    claim_rows.priority,
                    claim_rows.lane_seq,
                    claim_rows.attempt,
                    claim_rows.run_lease,
                    claim_rows.max_attempts,
                    NULL::bigint AS claim_batch_id,
                    NULL::int AS claim_batch_index
            ),
            claimed AS (
                SELECT
                    row_claims.claim_slot,
                    row_claims.receipt_id,
                    row_claims.claim_batch_id,
                    row_claims.claim_batch_index,
                    row_claims.ready_slot,
                    row_claims.ready_generation,
                    row_claims.job_id,
                    row_claims.queue,
                    row_claims.priority,
                    row_claims.lane_seq,
                    row_claims.attempt,
                    row_claims.run_lease,
                    row_claims.max_attempts
                FROM row_claims
                UNION ALL
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
                v_lane_shard AS enqueue_shard,
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
                    WHEN p_deadline_secs > 0 THEN v_claimed_at
                    ELSE NULL::timestamptz
                END AS heartbeat_at,
                v_deadline_at AS deadline_at,
                CASE
                    WHEN p_deadline_secs > 0 THEN v_claimed_at
                    ELSE NULL::timestamptz
                END AS attempted_at,
                selected.created_at,
                selected.unique_key,
                selected.unique_states,
                selected.payload
            FROM claimed
            CROSS JOIN lease_ring
            JOIN selected
             ON selected.ready_slot = claimed.ready_slot
             AND selected.ready_generation = claimed.ready_generation
             AND selected.queue = claimed.queue
             AND selected.effective_priority = claimed.priority
             AND selected.lane_seq = claimed.lane_seq
            ORDER BY selected.lane_seq ASC;

            GET DIAGNOSTICS v_claimed_count = ROW_COUNT;

            -- If a target ready row existed but the claim CTE produced no
            -- rows, the spent-attempt pre-pass may still have moved the
            -- sequence cursor over committed spent attempts. It deliberately
            -- does not advance over unspent rows: newly emitted claims still
            -- move the sequence only after the surrounding transaction
            -- commits, because sequence movement is non-transactional and
            -- cannot be recovered by rollback.
            IF v_claimed_count = 0 THEN
                RETURN;
            END IF;
        END;
        $function$


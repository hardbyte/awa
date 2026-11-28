CREATE OR REPLACE FUNCTION @SCHEMA@.claim_ready_runtime(p_queue text, p_max_batch bigint, p_deadline_secs double precision, p_aging_secs double precision)
 RETURNS TABLE(ready_slot integer, ready_generation bigint, lane_seq bigint, enqueue_shard smallint, lease_slot integer, lease_generation bigint, claim_slot integer, receipt_id bigint, claim_batch_id bigint, claim_batch_index integer, job_id bigint, kind text, queue text, args jsonb, lane_priority smallint, priority smallint, attempt smallint, run_lease bigint, max_attempts smallint, run_at timestamp with time zone, heartbeat_at timestamp with time zone, deadline_at timestamp with time zone, attempted_at timestamp with time zone, created_at timestamp with time zone, unique_key bytea, unique_states text, payload jsonb, lane_drained boolean)
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog', 'awa', 'pg_temp'
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
            v_evidence_next_seq BIGINT;
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
            FROM @SCHEMA@.queue_claim_heads AS claims
            JOIN @SCHEMA@.queue_enqueue_heads AS enqueues
              ON enqueues.queue = claims.queue
             AND enqueues.priority = claims.priority
             AND enqueues.enqueue_shard = claims.enqueue_shard
            CROSS JOIN LATERAL (
                SELECT
                    @SCHEMA@.sequence_next_value(claims.seq_name) AS claim_seq,
                    @SCHEMA@.sequence_next_value(enqueues.seq_name) AS next_seq
            ) AS cursors
            LEFT JOIN LATERAL (
                WITH first_segment AS MATERIALIZED (
                    SELECT
                        segment.ready_slot,
                        segment.ready_generation,
                        segment.first_run_at
                    FROM @SCHEMA@.ready_segments AS segment
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
                    FROM @SCHEMA@.ready_segments AS segment
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
                    FROM @SCHEMA@.ready_segments AS segment
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
                    FROM @SCHEMA@.ready_segments AS later_segment
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

            -- Catch the lane cursor up over committed claim evidence. Every
            -- attempt batch was emitted by a claim that started at a cursor
            -- already covered by evidence and recorded the contiguous rows
            -- it took, so the largest recorded next_lane_seq is a safe
            -- cursor position whether or not the claiming runtime's
            -- post-commit advance ever ran. Reading only committed rows
            -- keeps an abort of this transaction from stranding live work.
            SELECT max(attempts.next_lane_seq)
            INTO v_evidence_next_seq
            FROM @SCHEMA@.ready_claim_attempt_batches AS attempts
            WHERE attempts.ready_slot = v_target_slot
              AND attempts.ready_generation = v_target_generation
              AND attempts.queue = p_queue
              AND attempts.priority = v_lane_priority
              AND attempts.enqueue_shard = v_lane_shard
              AND attempts.next_lane_seq > v_lane_claim_seq;

            IF v_evidence_next_seq IS NOT NULL THEN
                PERFORM @SCHEMA@.set_sequence_next(v_claim_seq_name, v_evidence_next_seq);
                v_lane_claim_seq := v_evidence_next_seq;
                v_target_slot := NULL;
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
                    FROM @SCHEMA@.ready_segments AS segment
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
                    FROM @SCHEMA@.ready_segments AS later_segment
                    WHERE later_segment.ready_slot = segment.ready_slot
                      AND later_segment.ready_generation = segment.ready_generation
                      AND later_segment.queue = p_queue
                      AND later_segment.priority = v_lane_priority
                      AND later_segment.enqueue_shard = v_lane_shard
                ) AS slot_bound;
                IF v_target_slot IS NULL THEN
                    RETURN;
                END IF;
            END IF;

            SELECT
                ready.job_id,
                ready.run_lease,
                ready.lane_seq
            INTO
                v_head_job_id,
                v_head_run_lease,
                v_head_lane_seq
            FROM @SCHEMA@.ready_entries AS ready
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

            -- A later committed ready row proves earlier reservations are
            -- permanently absent: reserve_enqueue_seq holds the per-lane
            -- transaction advisory lock until commit/abort. Move TO the first
            -- visible row, never past it, so this sequence change is safe even
            -- if the claim aborts. Otherwise a gap before a tombstone can keep
            -- the bounded spent-prefix window empty on every future poll.
            IF v_head_lane_seq > v_lane_claim_seq THEN
                PERFORM @SCHEMA@.set_sequence_next(v_claim_seq_name, v_head_lane_seq);
                v_lane_claim_seq := v_head_lane_seq;
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
                SELECT 1 FROM @SCHEMA@.ready_tombstones AS tomb
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
                    FROM @SCHEMA@.ready_claim_attempt_batches AS existing_attempts
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
                    FROM @SCHEMA@.lease_claims AS existing_claims
                    WHERE existing_claims.job_id = v_head_job_id
                      AND existing_claims.run_lease = v_head_run_lease + 1
                    LIMIT 1
                )
                INTO v_head_attempt_spent;

                IF NOT v_head_attempt_spent THEN
                    SELECT EXISTS (
                        SELECT 1
                        FROM @SCHEMA@.lease_claim_closures AS existing_closures
                        WHERE existing_closures.job_id = v_head_job_id
                          AND existing_closures.run_lease = v_head_run_lease + 1
                        LIMIT 1
                    )
                    INTO v_head_attempt_spent;
                END IF;

                IF NOT v_head_attempt_spent THEN
                    SELECT EXISTS (
                        SELECT 1
                        FROM @SCHEMA@.receipt_completion_batches AS existing_batches
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
                        FROM @SCHEMA@.leases AS existing_leases
                        WHERE existing_leases.job_id = v_head_job_id
                          AND existing_leases.run_lease = v_head_run_lease + 1
                        LIMIT 1
                    )
                    INTO v_head_attempt_spent;
                END IF;

                IF NOT v_head_attempt_spent THEN
                    SELECT EXISTS (
                        SELECT 1
                        FROM @SCHEMA@.done_entries AS existing_done
                        WHERE existing_done.job_id = v_head_job_id
                          AND existing_done.run_lease = v_head_run_lease + 1
                        LIMIT 1
                    )
                    INTO v_head_attempt_spent;
                END IF;

                IF NOT v_head_attempt_spent THEN
                    SELECT EXISTS (
                        SELECT 1
                        FROM @SCHEMA@.deferred_jobs AS existing_deferred
                        WHERE existing_deferred.job_id = v_head_job_id
                          AND existing_deferred.run_lease = v_head_run_lease + 1
                        LIMIT 1
                    )
                    INTO v_head_attempt_spent;
                END IF;

                IF NOT v_head_attempt_spent THEN
                    SELECT EXISTS (
                        SELECT 1
                        FROM @SCHEMA@.dlq_entries AS existing_dlq
                        WHERE existing_dlq.job_id = v_head_job_id
                          AND existing_dlq.run_lease = v_head_run_lease + 1
                        LIMIT 1
                    )
                    INTO v_head_attempt_spent;
                END IF;

                IF NOT v_head_attempt_spent THEN
                    SELECT EXISTS (
                        SELECT 1
                        FROM @SCHEMA@.ready_entries AS existing_ready
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
                        SELECT 1 FROM @SCHEMA@.ready_tombstones AS tomb
                        WHERE tomb.ready_slot = v_target_slot
                          AND tomb.ready_generation = v_target_generation
                          AND tomb.queue = ready.queue
                          AND tomb.priority = ready.priority
                          AND tomb.enqueue_shard = ready.enqueue_shard
                          AND tomb.lane_seq = ready.lane_seq
                        -- Keep this bounded claim on indexed probes, not a full-partition hash.
                        OFFSET 0
                    ) AS is_tombstone
                FROM @SCHEMA@.ready_entries AS ready
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
                            FROM @SCHEMA@.ready_claim_attempt_batches AS existing_attempts
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
                                    FROM @SCHEMA@.lease_claims AS existing_claims
                                    WHERE existing_claims.job_id = selected.job_id
                                      AND existing_claims.run_lease = selected.run_lease + 1
                                )
                                OR EXISTS (
                                    SELECT 1
                                    FROM @SCHEMA@.lease_claim_closures AS existing_closures
                                    WHERE existing_closures.job_id = selected.job_id
                                      AND existing_closures.run_lease = selected.run_lease + 1
                                )
                                OR EXISTS (
                                    SELECT 1
                                    FROM @SCHEMA@.receipt_completion_batches AS existing_batches
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
                                    FROM @SCHEMA@.leases AS existing_leases
                                    WHERE existing_leases.job_id = selected.job_id
                                      AND existing_leases.run_lease = selected.run_lease + 1
                                )
                                OR EXISTS (
                                    SELECT 1
                                    FROM @SCHEMA@.done_entries AS existing_done
                                    WHERE existing_done.job_id = selected.job_id
                                      AND existing_done.run_lease = selected.run_lease + 1
                                )
                                OR EXISTS (
                                    SELECT 1
                                    FROM @SCHEMA@.deferred_jobs AS existing_deferred
                                    WHERE existing_deferred.job_id = selected.job_id
                                      AND existing_deferred.run_lease = selected.run_lease + 1
                                )
                                OR EXISTS (
                                    SELECT 1
                                    FROM @SCHEMA@.dlq_entries AS existing_dlq
                                    WHERE existing_dlq.job_id = selected.job_id
                                      AND existing_dlq.run_lease = selected.run_lease + 1
                                )
                                OR EXISTS (
                                    SELECT 1
                                    FROM @SCHEMA@.ready_entries AS existing_ready
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
                    count(*) FILTER (WHERE NOT selected_with_spent.attempt_spent)
                        OVER (ORDER BY selected_with_spent.lane_seq) AS unspent_through
                FROM selected_with_spent
            )
            SELECT LEAST(
                max(spent_prefix.lane_seq) + 1,
                (
                    SELECT min(other.first_lane_seq)
                    FROM @SCHEMA@.ready_segments AS other
                    WHERE other.queue = p_queue
                      AND other.priority = v_lane_priority
                      AND other.enqueue_shard = v_lane_shard
                      AND other.next_lane_seq > v_lane_claim_seq
                      AND other.next_lane_seq <= max(spent_prefix.lane_seq)
                      AND (other.ready_slot, other.ready_generation)
                          IS DISTINCT FROM (v_target_slot, v_target_generation)
                )
            )
            INTO v_spent_next_seq
            FROM spent_prefix
            WHERE unspent_through = 0;

                IF v_spent_next_seq IS NOT NULL THEN
                    -- Safe in-transaction sequence movement: only skip the
                    -- leading run of committed rows with already-committed
                    -- spent evidence, and the rolled-back reservations before
                    -- and between them. Every enqueue reserves its lane_seq
                    -- through reserve_enqueue_seq, which holds the lane's
                    -- reservation lock until the enqueue ends, so a committed
                    -- row follows only ended reservations. A path that
                    -- writes ready rows from any other allocation (such as
                    -- the enqueue-head trigger) would break this. A position
                    -- outside this generation's window can still hold a
                    -- committed row of another generation (a producer whose
                    -- snapshot predates a ring rotation), so the prefix stops
                    -- at the first segment of another generation that ends
                    -- inside it; segments are contiguous per enqueue and
                    -- never span a position of this generation. The next
                    -- claim targets that segment. Later
                    -- spent/tombstoned lanes cannot move the
                    -- cursor past an earlier live row, because the surrounding
                    -- claim transaction could still abort.
                    PERFORM @SCHEMA@.set_sequence_next(v_claim_seq_name, v_spent_next_seq);
                END IF;

                -- The spent-prefix path is a recovery/maintenance path for
                -- stale claim cursors, tombstones, or already-emitted
                -- attempts. Return after moving the cursor so the common
                -- claim query can stay narrow: it only needs to filter
                -- ready_tombstones and does not plan every terminal evidence
                -- relation on every hot-path claim.
                RETURN;
            END IF;

            -- The slot functions carry the claim CTE; the mode marker below
            -- keeps the substrate reinstall loops' receipts detection working.
            -- receipts mode: claim_ready_slot_N runs INSERT INTO @SCHEMA@.lease_claim_batches
            RETURN QUERY EXECUTE format(
                'SELECT * FROM %I.claim_ready_slot_%s($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)',
                '@SCHEMA@',
                v_target_slot
            )
            USING
                p_queue,
                v_target_slot,
                v_target_generation,
                v_lane_priority,
                v_lane_shard,
                v_lane_claim_seq,
                v_claim_limit,
                p_aging_secs,
                v_claimed_at,
                v_deadline_at,
                p_deadline_secs,
                v_lane_next_seq;

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

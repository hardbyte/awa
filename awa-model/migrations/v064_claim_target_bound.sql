-- v064: bound a claim's read of its target slot.
-- Tracking: https://github.com/hardbyte/awa/issues/885
-- Design: docs/adr/054-claim-target-bound.md
--
-- claim_ready_runtime locks a lane's claim head in the statement that
-- resolves the target slot, finds the first visible row of that slot at or
-- after the lane's cursor and moves the cursor to it, and claim_ready_slot_<n>
-- reads the slot's rows from the cursor in lane order. Neither read has an
-- upper bound, so the claim is correct only while a lane's unclaimed rows in
-- its target slot are one contiguous run and none of them leaves the slot
-- between the statement that chose it and the statements that read it. A lane
-- whose rows are interleaved across slots, or a transaction that moves rows
-- between slots and commits after the claim's snapshot and before its head
-- lock, makes the cursor pass rows that are then never claimed and are pruned
-- with their slot. #847 was that layout, written by a producer above READ
-- COMMITTED, and binaries before v062 can still write it; moving rows between
-- slots (#779) would also produce it.
--
-- The claim now resolves its target again after the head lock, in a fresh
-- statement, and both reads stop below the smallest first_lane_seq of any
-- segment of the lane past the cursor that is in another (slot, generation).
-- The head step, which reads one row, looks at the first two segments in
-- next_lane_seq order; claim_ready_slot_<n> looks at the nearest max_batch + 1.
-- The bound is computed in the statement that reads the rows. At worst a claim
-- returns fewer rows, or none, and the next claim re-resolves.
--
-- V23_UP writes the new bodies into new substrates. This file rewrites
-- <schema>.claim_ready_runtime and each <schema>.claim_ready_slot_<n> in the
-- default and every discovered custom queue-storage schema, found by their
-- exact signatures, from their own definitions, so signatures, owners,
-- privileges and settings are unchanged. A function that already carries a
-- change (a fresh install or a replay) is left alone. No table DDL, no
-- trigger change and no substrate reinstallation. A function is rewritten only
-- when every one of its changes applies; a body that does not match the
-- released text is left exactly as it is and a warning names it.
--
-- N-1 binaries call the same functions with the same arguments and results
-- and the claim protocol is unchanged, so they run the same before and after
-- the migration and rolling back binaries needs no schema change. An older
-- prepare_schema can reinstall an older claim body; anything that moves rows
-- between slots must check that the installed functions carry the bound.
--
-- Live load: function replacement only. It takes no lock on any awa or
-- queue-storage table, and its work scales with the number of claim
-- functions, not with job count. A claim in flight finishes on the old body;
-- the next call uses the new one. Each claim adds one probe of ready_segments
-- for the re-resolve and one bounded probe in each of the head step (two
-- segments) and the slot function (max_batch + 1), all on the
-- (queue, priority, enqueue_shard, next_lane_seq) index.
--
-- External runners apply the complete `awa migrate --sql --from 63 --to 64`
-- bundle, which refreshes trusted queue-storage discovery before this block.
-- Run it as the owner of the claim functions, or as a migrator that can
-- SET ROLE to it.

DO $$
DECLARE
    v_schema TEXT;
    v_queue_slots INT;
    v_function RECORD;
    v_definition TEXT;
    v_updated TEXT;
    v_complete BOOLEAN;
    v_head_marker TEXT := 'Rows of this slot at or after the next segment';
    v_resolve_marker TEXT := 'Resolve the target';
    v_slot_marker TEXT := 'LIMIT GREATEST(p_claim_limit, 0) + 1';
    v_resolve TEXT := $resolve$
            -- The head lock can be granted after the statement above took its
            -- snapshot, and a transaction that moved rows between slots and
            -- committed in between is not visible to it. Resolve the target
            -- again in a fresh statement, with the lock held.
            SELECT segment.ready_slot, segment.ready_generation
            INTO v_target_slot, v_target_generation
            FROM %1$I.ready_segments AS segment
            WHERE segment.queue = p_queue
              AND segment.priority = v_lane_priority
              AND segment.enqueue_shard = v_lane_shard
              AND segment.next_lane_seq > v_lane_claim_seq
            ORDER BY segment.next_lane_seq ASC
            LIMIT 1;$resolve$;
    v_head_bound TEXT := $head$
              -- Rows of this slot at or after the next segment of another
              -- slot are out of reach of this claim, whether to take or to
              -- move the cursor to.
              -- This reads one row, and every segment holds a row, so the
              -- first row at or after the cursor lies in the first segment
              -- past it when that is in this slot, and the second segment is
              -- all that is needed to see where another slot begins.
              AND ready.lane_seq < COALESCE((
                  SELECT min(bounding.first_lane_seq)
                  FROM (
                      SELECT
                          segment.ready_slot,
                          segment.ready_generation,
                          segment.first_lane_seq
                      FROM %1$I.ready_segments AS segment
                      WHERE segment.queue = p_queue
                        AND segment.priority = v_lane_priority
                        AND segment.enqueue_shard = v_lane_shard
                        AND segment.next_lane_seq > v_lane_claim_seq
                      ORDER BY segment.next_lane_seq ASC
                      LIMIT 2
                  ) AS bounding
                  WHERE (bounding.ready_slot, bounding.ready_generation)
                        IS DISTINCT FROM (v_target_slot, v_target_generation)
              ), 9223372036854775807)$head$;
    v_slot_bound TEXT := $slot$
                  -- Stop below the next segment of another slot, from the
                  -- same snapshot as the rows; see claim_ready_runtime.
                  AND ready.lane_seq < COALESCE((
                      SELECT min(bounding.first_lane_seq)
                      FROM (
                          SELECT
                              segment.ready_slot,
                              segment.ready_generation,
                              segment.first_lane_seq
                          FROM %1$I.ready_segments AS segment
                          WHERE segment.queue = p_queue
                            AND segment.priority = p_lane_priority
                            AND segment.enqueue_shard = p_lane_shard
                            AND segment.next_lane_seq > p_lane_claim_seq
                          ORDER BY segment.next_lane_seq ASC
                          LIMIT GREATEST(p_claim_limit, 0) + 1
                      ) AS bounding
                      WHERE (bounding.ready_slot, bounding.ready_generation)
                            IS DISTINCT FROM (p_target_slot, p_target_generation)
                  ), 9223372036854775807)$slot$;
BEGIN
    FOR v_schema IN
        SELECT discovered.schema_name FROM awa.queue_storage_schemas() AS discovered
    LOOP
        -- Serialize with a concurrent prepare_schema() of the same schema. This
        -- is an advisory lock, not a table lock: claims and completions do not
        -- take it.
        PERFORM pg_advisory_xact_lock(
            hashtextextended(format('awa.queue_storage.install:%I', v_schema), 0)
        );
        IF to_regclass(format('%I.queue_ring_state', v_schema)) IS NULL THEN
            CONTINUE;
        END IF;
        EXECUTE format(
            'SELECT slot_count FROM %I.queue_ring_state WHERE singleton = TRUE',
            v_schema
        )
        INTO v_queue_slots;

        -- Only the installer's functions, each by its exact signature.
        -- Another function that shares one of these names is not touched.
        FOR v_function IN
            SELECT candidate.proc, candidate.is_runtime
            FROM (
                SELECT to_regprocedure(format(
                    '%I.claim_ready_runtime(text,bigint,double precision,double precision)',
                    v_schema
                )) AS proc, TRUE AS is_runtime
                UNION ALL
                SELECT to_regprocedure(format(
                    '%I.claim_ready_slot_%s(text,int,bigint,smallint,smallint,bigint,bigint,double precision,timestamptz,timestamptz,double precision,bigint)',
                    v_schema,
                    slot
                )), FALSE
                FROM generate_series(0, COALESCE(v_queue_slots, 0) - 1) AS slot
            ) AS candidate
            WHERE candidate.proc IS NOT NULL
        LOOP
            v_definition := pg_get_functiondef(v_function.proc::oid);
            v_updated := v_definition;
            IF v_function.is_runtime THEN
                IF position(v_head_marker IN v_updated) = 0 THEN
                    v_updated := regexp_replace(
                        v_updated,
                        '(AND ready[.]ready_generation = v_target_generation[[:space:]]+AND ready[.]lane_seq >= v_lane_claim_seq)([[:space:]]+ORDER BY ready[.]lane_seq ASC[[:space:]]+LIMIT 1;)',
                        '\1' || format(v_head_bound, v_schema) || '\2'
                    );
                END IF;
                IF position(v_resolve_marker IN v_updated) = 0 THEN
                    v_updated := regexp_replace(
                        v_updated,
                        '(FOR UPDATE OF claims SKIP LOCKED;[[:space:]]+IF NOT FOUND THEN[[:space:]]+RETURN;[[:space:]]+END IF;)',
                        '\1' || chr(10) || format(v_resolve, v_schema)
                    );
                END IF;
                v_complete := position(v_head_marker IN v_updated) > 0
                          AND position(v_resolve_marker IN v_updated) > 0;
            ELSE
                IF position(v_slot_marker IN v_updated) = 0 THEN
                    v_updated := regexp_replace(
                        v_updated,
                        '(AND ready[.]lane_seq >= p_lane_claim_seq)([[:space:]]+ORDER BY ready[.]lane_seq ASC[[:space:]]+LIMIT p_claim_limit)',
                        '\1' || format(v_slot_bound, v_schema) || '\2'
                    );
                END IF;
                v_complete := position(v_slot_marker IN v_updated) > 0;
            END IF;
            IF NOT v_complete THEN
                RAISE WARNING 'awa v064: % does not match the released claim text and keeps its old read',
                    v_function.proc;
                CONTINUE;
            END IF;
            IF v_updated <> v_definition THEN
                EXECUTE v_updated;
            END IF;
        END LOOP;
    END LOOP;
END;
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (64, 'Bound a claim''s read of its target slot')
ON CONFLICT (version) DO NOTHING;

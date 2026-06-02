-- #290: the SQL compat layer's DELETE FROM awa.jobs WHERE id = $1 routes
-- to awa.delete_job_compat(p_id), which can land on `done_entries` for
-- terminal rows. The function previously deleted from done_entries
-- without touching `queue_terminal_live_counts`, so terminal-row deletes
-- via the SQL compat path drifted the counter from the underlying table.
--
-- Reproduced on the v021 head: `DELETE FROM awa.jobs WHERE id = <terminal>`
-- dropped done_entries from 5 → 4 while the live counter stayed at 5.
-- Once `queue_counts_exact` reads the counter (see #305) the drift becomes
-- operator-visible; before that, prune_oldest folding from the counter
-- could bake the drift into `queue_terminal_rollups` permanently.
--
-- This migration reshapes the done_entries branch of delete_job_compat to
-- also return ready_slot + enqueue_shard, and decrement the counter for
-- the deleted row inside the same statement. Other branches
-- (ready_entries / deferred_jobs / leases / dlq_entries) are unchanged —
-- the counter is keyed on done_entries only.

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
    v_ready_slot INT;
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

    -- #290: done_entries branch now returns ready_slot + enqueue_shard
    -- and decrements queue_terminal_live_counts for the deleted row.
    -- This is the SQL-side mirror of the Rust delete-path wiring
    -- (move_failed_to_dlq, bulk_move_failed_to_dlq, discard_failed_by_kind,
    -- retry_job_tx). Without it, DELETE FROM awa.jobs WHERE id = <terminal>
    -- drifts the counter from done_entries.
    EXECUTE format(
        'DELETE FROM %I.done_entries
         WHERE job_id = $1
         RETURNING queue, priority, state, unique_key, unique_states,
                   ready_slot, enqueue_shard',
        v_schema
    )
    INTO v_queue, v_priority, v_state, v_unique_key, v_unique_states,
         v_ready_slot, v_enqueue_shard
    USING p_id;
    GET DIAGNOSTICS v_rows = ROW_COUNT;

    IF v_rows > 0 THEN
        -- The counter table is created lazily by
        -- QueueStorage::prepare_schema() on first runtime boot, which
        -- can happen AFTER migrations have run (migrations don't
        -- prepare the per-schema queue-storage tables). Guard the
        -- decrement with `to_regclass` so a DELETE FROM awa.jobs that
        -- arrives during the boot window degrades to "drift, then
        -- rebuild" rather than "relation does not exist". Operators
        -- recover with `awa storage rebuild-terminal-counters` once the
        -- counter table catches up.
        IF to_regclass(format('%I.queue_terminal_live_counts', v_schema)) IS NOT NULL THEN
            EXECUTE format(
                'UPDATE %I.queue_terminal_live_counts AS counts
                 SET live_terminal_count = GREATEST(0, counts.live_terminal_count - 1)
                 WHERE counts.ready_slot     = $1
                   AND counts.queue          = $2
                   AND counts.priority       = $3
                   AND counts.enqueue_shard  = $4',
                v_schema
            )
            USING v_ready_slot, v_queue, v_priority, v_enqueue_shard;
        END IF;
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
VALUES (22, 'delete_job_compat decrements queue_terminal_live_counts for done_entries deletes (#290)')
ON CONFLICT (version) DO NOTHING;

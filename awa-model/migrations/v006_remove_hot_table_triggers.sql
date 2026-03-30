-- v006: Dirty-key statement triggers for deadlock-free admin metadata
--
-- The v005 statement-level triggers updated queue_state_counts directly
-- inside the same transaction as the jobs_hot mutation. Under high
-- concurrency, this created cross-table deadlocks: concurrent batch
-- completions held row locks on different jobs_hot rows while their
-- triggers competed for the same queue_state_counts row.
--
-- Fix: replace direct counter updates with dirty-key marking. The trigger
-- records which queues/kinds were touched (append to a small dirty-key
-- table), and the maintenance leader asynchronously recomputes the exact
-- cached row for each dirty key from the base tables.
--
-- This gives:
--   - Zero counter contention on the hot path (INSERT ... ON CONFLICT DO NOTHING)
--   - Exact (not approximate) cached values after recompute
--   - Bounded drift localized to recently-touched queues/kinds
--   - Full reconciliation via refresh_admin_metadata() as safety net

-- ── Step 1: Drop v005 statement-level triggers on jobs_hot ──────────

DROP TRIGGER IF EXISTS trg_jobs_hot_admin_metadata_insert_stmt ON awa.jobs_hot;
DROP TRIGGER IF EXISTS trg_jobs_hot_admin_metadata_update_stmt ON awa.jobs_hot;
DROP TRIGGER IF EXISTS trg_jobs_hot_admin_metadata_delete_stmt ON awa.jobs_hot;

-- Also drop the legacy v004 per-row trigger name if present
DROP TRIGGER IF EXISTS trg_jobs_hot_admin_metadata ON awa.jobs_hot;

-- Drop v005 statement-level triggers on scheduled_jobs too — both tables
-- must use the same dirty-key strategy to avoid conflicting update paths.
DROP TRIGGER IF EXISTS trg_scheduled_jobs_admin_metadata_insert_stmt ON awa.scheduled_jobs;
DROP TRIGGER IF EXISTS trg_scheduled_jobs_admin_metadata_update_stmt ON awa.scheduled_jobs;
DROP TRIGGER IF EXISTS trg_scheduled_jobs_admin_metadata_delete_stmt ON awa.scheduled_jobs;

-- Also drop legacy v004 per-row trigger on scheduled_jobs
DROP TRIGGER IF EXISTS trg_scheduled_jobs_admin_metadata ON awa.scheduled_jobs;

-- ── Step 2: Dirty-key tables ────────────────────────────────────────

CREATE TABLE IF NOT EXISTS awa.admin_dirty_queues (
    queue      TEXT PRIMARY KEY,
    touched_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS awa.admin_dirty_kinds (
    kind       TEXT PRIMARY KEY,
    touched_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Step 3: Covering indexes for targeted recompute ─────────────────
--
-- The recompute query filters by queue (or kind), so it needs an index
-- to avoid full table scans. These enable index-only scans for the
-- per-queue and per-kind count queries.

CREATE INDEX IF NOT EXISTS idx_awa_jobs_hot_queue_state
    ON awa.jobs_hot (queue, state);

CREATE INDEX IF NOT EXISTS idx_awa_scheduled_jobs_queue_state
    ON awa.scheduled_jobs (queue, state);

-- ── Step 4: Statement-level trigger that marks dirty keys ───────────

-- Trigger functions insert dirty keys in deterministic (sorted) order to
-- avoid lock-ordering deadlocks when multiple concurrent statements touch
-- overlapping sets of queues/kinds. ON CONFLICT DO NOTHING avoids heap
-- update churn — "dirty exists" is the only signal needed.
--
-- Queue and kind invalidation are kept in separate statements to minimize
-- lock types mixed in the same lock graph.

CREATE OR REPLACE FUNCTION awa.mark_dirty_keys_insert() RETURNS trigger AS $$
BEGIN
    INSERT INTO awa.admin_dirty_queues (queue)
    SELECT DISTINCT queue FROM new_rows ORDER BY queue
    ON CONFLICT (queue) DO NOTHING;

    INSERT INTO awa.admin_dirty_kinds (kind)
    SELECT DISTINCT kind FROM new_rows ORDER BY kind
    ON CONFLICT (kind) DO NOTHING;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION awa.mark_dirty_keys_delete() RETURNS trigger AS $$
BEGIN
    INSERT INTO awa.admin_dirty_queues (queue)
    SELECT DISTINCT queue FROM old_rows ORDER BY queue
    ON CONFLICT (queue) DO NOTHING;

    INSERT INTO awa.admin_dirty_kinds (kind)
    SELECT DISTINCT kind FROM old_rows ORDER BY kind
    ON CONFLICT (kind) DO NOTHING;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION awa.mark_dirty_keys_update() RETURNS trigger AS $$
BEGIN
    INSERT INTO awa.admin_dirty_queues (queue)
    SELECT DISTINCT queue FROM (
        SELECT queue FROM old_rows
        UNION
        SELECT queue FROM new_rows
    ) t ORDER BY queue
    ON CONFLICT (queue) DO NOTHING;

    INSERT INTO awa.admin_dirty_kinds (kind)
    SELECT DISTINCT kind FROM (
        SELECT kind FROM old_rows
        UNION
        SELECT kind FROM new_rows
    ) t ORDER BY kind
    ON CONFLICT (kind) DO NOTHING;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- ── Step 5: Attach triggers to jobs_hot ─────────────────────────────

DROP TRIGGER IF EXISTS trg_jobs_hot_dirty_keys_insert ON awa.jobs_hot;
CREATE TRIGGER trg_jobs_hot_dirty_keys_insert
    AFTER INSERT ON awa.jobs_hot
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION awa.mark_dirty_keys_insert();

DROP TRIGGER IF EXISTS trg_jobs_hot_dirty_keys_delete ON awa.jobs_hot;
CREATE TRIGGER trg_jobs_hot_dirty_keys_delete
    AFTER DELETE ON awa.jobs_hot
    REFERENCING OLD TABLE AS old_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION awa.mark_dirty_keys_delete();

DROP TRIGGER IF EXISTS trg_jobs_hot_dirty_keys_update ON awa.jobs_hot;
CREATE TRIGGER trg_jobs_hot_dirty_keys_update
    AFTER UPDATE ON awa.jobs_hot
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION awa.mark_dirty_keys_update();

-- Same dirty-key triggers on scheduled_jobs (promotion, cron inserts, etc.)

DROP TRIGGER IF EXISTS trg_scheduled_jobs_dirty_keys_insert ON awa.scheduled_jobs;
CREATE TRIGGER trg_scheduled_jobs_dirty_keys_insert
    AFTER INSERT ON awa.scheduled_jobs
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION awa.mark_dirty_keys_insert();

DROP TRIGGER IF EXISTS trg_scheduled_jobs_dirty_keys_delete ON awa.scheduled_jobs;
CREATE TRIGGER trg_scheduled_jobs_dirty_keys_delete
    AFTER DELETE ON awa.scheduled_jobs
    REFERENCING OLD TABLE AS old_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION awa.mark_dirty_keys_delete();

DROP TRIGGER IF EXISTS trg_scheduled_jobs_dirty_keys_update ON awa.scheduled_jobs;
CREATE TRIGGER trg_scheduled_jobs_dirty_keys_update
    AFTER UPDATE ON awa.scheduled_jobs
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION awa.mark_dirty_keys_update();

-- ── Step 6: Targeted recompute function ─────────────────────────────
--
-- Drains dirty keys and recomputes exact cached rows from base tables.
-- Uses the (queue, state) index for efficient per-queue counts.
-- Called frequently by the maintenance leader (every ~1-5s).

CREATE OR REPLACE FUNCTION awa.recompute_dirty_admin_metadata(
    p_batch_size INT DEFAULT 100
) RETURNS INT AS $$
DECLARE
    affected INT := 0;
    dirty_q TEXT;
    dirty_k TEXT;
    v_scheduled BIGINT;
    v_available BIGINT;
    v_running BIGINT;
    v_completed BIGINT;
    v_retryable BIGINT;
    v_failed BIGINT;
    v_cancelled BIGINT;
    v_waiting_external BIGINT;
    v_ref_count BIGINT;
BEGIN
    -- Drain dirty queues
    FOR dirty_q IN
        DELETE FROM awa.admin_dirty_queues
        WHERE queue IN (
            SELECT queue FROM awa.admin_dirty_queues
            ORDER BY touched_at
            LIMIT p_batch_size
            FOR UPDATE SKIP LOCKED
        )
        RETURNING queue
    LOOP
        -- Recompute exact counts for this queue from base tables
        -- Uses the (queue, state) index for efficient per-queue scan
        SELECT
            count(*) FILTER (WHERE state = 'scheduled'),
            count(*) FILTER (WHERE state = 'available'),
            count(*) FILTER (WHERE state = 'running'),
            count(*) FILTER (WHERE state = 'completed'),
            count(*) FILTER (WHERE state = 'retryable'),
            count(*) FILTER (WHERE state = 'failed'),
            count(*) FILTER (WHERE state = 'cancelled'),
            count(*) FILTER (WHERE state = 'waiting_external')
        INTO
            v_scheduled, v_available, v_running, v_completed,
            v_retryable, v_failed, v_cancelled, v_waiting_external
        FROM (
            SELECT state FROM awa.jobs_hot WHERE queue = dirty_q
            UNION ALL
            SELECT state FROM awa.scheduled_jobs WHERE queue = dirty_q
        ) AS jobs;

        -- Upsert the cached row
        IF v_scheduled + v_available + v_running + v_completed
           + v_retryable + v_failed + v_cancelled + v_waiting_external > 0
        THEN
            INSERT INTO awa.queue_state_counts (
                queue, scheduled, available, running, completed,
                retryable, failed, cancelled, waiting_external
            ) VALUES (
                dirty_q, v_scheduled, v_available, v_running, v_completed,
                v_retryable, v_failed, v_cancelled, v_waiting_external
            )
            ON CONFLICT (queue) DO UPDATE SET
                scheduled = EXCLUDED.scheduled,
                available = EXCLUDED.available,
                running = EXCLUDED.running,
                completed = EXCLUDED.completed,
                retryable = EXCLUDED.retryable,
                failed = EXCLUDED.failed,
                cancelled = EXCLUDED.cancelled,
                waiting_external = EXCLUDED.waiting_external;
        ELSE
            DELETE FROM awa.queue_state_counts WHERE queue = dirty_q;
        END IF;

        -- Upsert queue catalog
        SELECT count(*) INTO v_ref_count
        FROM (
            SELECT 1 FROM awa.jobs_hot WHERE queue = dirty_q
            UNION ALL
            SELECT 1 FROM awa.scheduled_jobs WHERE queue = dirty_q
        ) t;

        IF v_ref_count > 0 THEN
            INSERT INTO awa.job_queue_catalog (queue, ref_count)
            VALUES (dirty_q, v_ref_count)
            ON CONFLICT (queue) DO UPDATE SET ref_count = EXCLUDED.ref_count;
        ELSE
            DELETE FROM awa.job_queue_catalog WHERE queue = dirty_q;
        END IF;

        affected := affected + 1;
    END LOOP;

    -- Drain dirty kinds
    FOR dirty_k IN
        DELETE FROM awa.admin_dirty_kinds
        WHERE kind IN (
            SELECT kind FROM awa.admin_dirty_kinds
            ORDER BY touched_at
            LIMIT p_batch_size
            FOR UPDATE SKIP LOCKED
        )
        RETURNING kind
    LOOP
        SELECT count(*) INTO v_ref_count
        FROM (
            SELECT 1 FROM awa.jobs_hot WHERE kind = dirty_k
            UNION ALL
            SELECT 1 FROM awa.scheduled_jobs WHERE kind = dirty_k
        ) t;

        IF v_ref_count > 0 THEN
            INSERT INTO awa.job_kind_catalog (kind, ref_count)
            VALUES (dirty_k, v_ref_count)
            ON CONFLICT (kind) DO UPDATE SET ref_count = EXCLUDED.ref_count;
        ELSE
            DELETE FROM awa.job_kind_catalog WHERE kind = dirty_k;
        END IF;

        affected := affected + 1;
    END LOOP;

    RETURN affected;
END;
$$ LANGUAGE plpgsql;

-- ── Step 7: Keep refresh_admin_metadata() for full reconciliation ───

CREATE OR REPLACE FUNCTION awa.refresh_admin_metadata() RETURNS VOID AS $$
BEGIN
    -- Refresh queue_state_counts
    WITH current_counts AS (
        SELECT
            queue,
            count(*) FILTER (WHERE state = 'scheduled') AS scheduled,
            count(*) FILTER (WHERE state = 'available') AS available,
            count(*) FILTER (WHERE state = 'running') AS running,
            count(*) FILTER (WHERE state = 'completed') AS completed,
            count(*) FILTER (WHERE state = 'retryable') AS retryable,
            count(*) FILTER (WHERE state = 'failed') AS failed,
            count(*) FILTER (WHERE state = 'cancelled') AS cancelled,
            count(*) FILTER (WHERE state = 'waiting_external') AS waiting_external
        FROM (
            SELECT queue, state FROM awa.jobs_hot
            UNION ALL
            SELECT queue, state FROM awa.scheduled_jobs
        ) AS jobs
        GROUP BY queue
    )
    INSERT INTO awa.queue_state_counts (
        queue, scheduled, available, running, completed,
        retryable, failed, cancelled, waiting_external
    )
    SELECT * FROM current_counts
    ON CONFLICT (queue) DO UPDATE SET
        scheduled = EXCLUDED.scheduled,
        available = EXCLUDED.available,
        running = EXCLUDED.running,
        completed = EXCLUDED.completed,
        retryable = EXCLUDED.retryable,
        failed = EXCLUDED.failed,
        cancelled = EXCLUDED.cancelled,
        waiting_external = EXCLUDED.waiting_external;

    DELETE FROM awa.queue_state_counts
    WHERE scheduled = 0 AND available = 0 AND running = 0
      AND completed = 0 AND retryable = 0 AND failed = 0
      AND cancelled = 0 AND waiting_external = 0
      AND queue NOT IN (
          SELECT DISTINCT queue FROM awa.jobs_hot
          UNION
          SELECT DISTINCT queue FROM awa.scheduled_jobs
      );

    -- Refresh catalogs
    WITH current_kinds AS (
        SELECT kind, count(*) AS ref_count
        FROM (
            SELECT kind FROM awa.jobs_hot
            UNION ALL
            SELECT kind FROM awa.scheduled_jobs
        ) AS jobs
        GROUP BY kind
    )
    INSERT INTO awa.job_kind_catalog (kind, ref_count)
    SELECT * FROM current_kinds
    ON CONFLICT (kind) DO UPDATE SET ref_count = EXCLUDED.ref_count;

    DELETE FROM awa.job_kind_catalog
    WHERE ref_count <= 0
       OR kind NOT IN (
          SELECT DISTINCT kind FROM awa.jobs_hot
          UNION
          SELECT DISTINCT kind FROM awa.scheduled_jobs
      );

    WITH current_queues AS (
        SELECT queue, count(*) AS ref_count
        FROM (
            SELECT queue FROM awa.jobs_hot
            UNION ALL
            SELECT queue FROM awa.scheduled_jobs
        ) AS jobs
        GROUP BY queue
    )
    INSERT INTO awa.job_queue_catalog (queue, ref_count)
    SELECT * FROM current_queues
    ON CONFLICT (queue) DO UPDATE SET ref_count = EXCLUDED.ref_count;

    DELETE FROM awa.job_queue_catalog
    WHERE ref_count <= 0
       OR queue NOT IN (
          SELECT DISTINCT queue FROM awa.jobs_hot
          UNION
          SELECT DISTINCT queue FROM awa.scheduled_jobs
      );

    -- Clear dirty tables since we just did a full refresh
    TRUNCATE awa.admin_dirty_queues, awa.admin_dirty_kinds;
END;
$$ LANGUAGE plpgsql;

-- ── Step 8: Initial reconciliation and version ──────────────────────

SELECT awa.refresh_admin_metadata();

INSERT INTO awa.schema_version (version, description)
VALUES (6, 'Dirty-key statement triggers for deadlock-free admin metadata')
ON CONFLICT (version) DO NOTHING;

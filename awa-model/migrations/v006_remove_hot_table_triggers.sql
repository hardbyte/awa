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

-- ── Step 0: Fix concurrent UPDATE race in awa.jobs view trigger ─────
--
-- The v001 INSTEAD OF UPDATE trigger on awa.jobs implements UPDATE as
-- DELETE + INSERT to handle cross-table state transitions (hot ↔ scheduled).
-- The DELETE used only WHERE id = $1, so a concurrent UPDATE could delete
-- a row just re-inserted by another transaction (same id, different state).
-- Both callers saw success — violating at-most-once callback resolution.
--
-- Fix: the DELETE now checks state, run_lease, and callback_id from OLD,
-- so it only removes the exact version it was handed. If a concurrent
-- transaction already modified the row, the DELETE matches 0 rows and
-- RETURN NULL tells PostgreSQL to skip this row (0 rows in RETURNING).
--
-- Also changes rescue queries from awa.jobs view to awa.jobs_hot directly,
-- since FOR UPDATE on UNION ALL views is not reliably supported.

CREATE OR REPLACE FUNCTION awa.write_jobs_view() RETURNS trigger AS $$
DECLARE
    target_table TEXT;
    source_table TEXT;
    rows_deleted INTEGER;
BEGIN
    IF TG_OP = 'INSERT' THEN
        NEW.id := COALESCE(NEW.id, nextval('awa.jobs_id_seq'));
        NEW.queue := COALESCE(NEW.queue, 'default');
        NEW.args := COALESCE(NEW.args, '{}'::jsonb);
        NEW.state := COALESCE(NEW.state, 'available'::awa.job_state);
        NEW.priority := COALESCE(NEW.priority, 2);
        NEW.attempt := COALESCE(NEW.attempt, 0);
        NEW.max_attempts := COALESCE(NEW.max_attempts, 25);
        NEW.run_at := COALESCE(NEW.run_at, now());
        NEW.created_at := COALESCE(NEW.created_at, now());
        NEW.errors := COALESCE(NEW.errors, '{}'::jsonb[]);
        NEW.metadata := COALESCE(NEW.metadata, '{}'::jsonb);
        NEW.tags := COALESCE(NEW.tags, '{}'::text[]);
        NEW.run_lease := COALESCE(NEW.run_lease, 0);
    END IF;

    IF TG_OP = 'DELETE' THEN
        source_table := CASE
            WHEN OLD.state IN ('scheduled'::awa.job_state, 'retryable'::awa.job_state)
                THEN 'awa.scheduled_jobs'
            ELSE 'awa.jobs_hot'
        END;
        EXECUTE format('DELETE FROM %s WHERE id = $1', source_table) USING OLD.id;
        RETURN OLD;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        source_table := CASE
            WHEN OLD.state IN ('scheduled'::awa.job_state, 'retryable'::awa.job_state)
                THEN 'awa.scheduled_jobs'
            ELSE 'awa.jobs_hot'
        END;
        -- Optimistic concurrency: only delete the exact version from OLD.
        -- If a concurrent transaction already modified this row (changing
        -- state, run_lease, or callback_id), the DELETE matches 0 rows
        -- and we return NULL — the caller sees 0 rows from RETURNING.
        EXECUTE format(
            'DELETE FROM %s WHERE id = $1 AND state = $2 AND run_lease = $3 AND callback_id IS NOT DISTINCT FROM $4',
            source_table
        ) USING OLD.id, OLD.state, OLD.run_lease, OLD.callback_id;
        GET DIAGNOSTICS rows_deleted = ROW_COUNT;

        IF rows_deleted = 0 THEN
            RETURN NULL;
        END IF;
    END IF;

    target_table := CASE
        WHEN NEW.state IN ('scheduled'::awa.job_state, 'retryable'::awa.job_state)
            THEN 'awa.scheduled_jobs'
        ELSE 'awa.jobs_hot'
    END;

    EXECUTE format(
        'INSERT INTO %s (
            id, kind, queue, args, state, priority, attempt, max_attempts,
            run_at, heartbeat_at, deadline_at, attempted_at, finalized_at,
            created_at, errors, metadata, tags, unique_key, unique_states,
            callback_id, callback_timeout_at, callback_filter, callback_on_complete,
            callback_on_fail, callback_transform, run_lease, progress
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8,
            $9, $10, $11, $12, $13,
            $14, $15, $16, $17, $18, $19,
            $20, $21, $22, $23,
            $24, $25, $26, $27
        )',
        target_table
    )
    USING
        NEW.id, NEW.kind, NEW.queue, NEW.args, NEW.state, NEW.priority, NEW.attempt,
        NEW.max_attempts, NEW.run_at, NEW.heartbeat_at, NEW.deadline_at, NEW.attempted_at,
        NEW.finalized_at, NEW.created_at, NEW.errors, NEW.metadata, NEW.tags,
        NEW.unique_key, NEW.unique_states, NEW.callback_id, NEW.callback_timeout_at,
        NEW.callback_filter, NEW.callback_on_complete, NEW.callback_on_fail,
        NEW.callback_transform, NEW.run_lease, NEW.progress;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

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
    -- Only mark dirty when queue, kind, or state actually changed.
    -- Heartbeat and progress-only updates don't affect admin caches.
    INSERT INTO awa.admin_dirty_queues (queue)
    SELECT DISTINCT queue FROM (
        SELECT o.queue FROM old_rows o JOIN new_rows n ON o.id = n.id
        WHERE o.queue IS DISTINCT FROM n.queue
           OR o.state IS DISTINCT FROM n.state
        UNION
        SELECT n.queue FROM old_rows o JOIN new_rows n ON o.id = n.id
        WHERE o.queue IS DISTINCT FROM n.queue
           OR o.state IS DISTINCT FROM n.state
    ) t ORDER BY queue
    ON CONFLICT (queue) DO NOTHING;

    INSERT INTO awa.admin_dirty_kinds (kind)
    SELECT DISTINCT kind FROM (
        SELECT o.kind FROM old_rows o JOIN new_rows n ON o.id = n.id
        WHERE o.kind IS DISTINCT FROM n.kind
        UNION
        SELECT n.kind FROM old_rows o JOIN new_rows n ON o.id = n.id
        WHERE o.kind IS DISTINCT FROM n.kind
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

-- ── Step 6: Cache table writers ──────────────────────────────────────
--
-- LOCKING CONTRACT: any function that mutates queue_state_counts,
-- job_kind_catalog, or job_queue_catalog MUST acquire advisory lock
-- 1098018130 (pg_advisory_xact_lock) before writing. This serializes
-- the two writer paths — targeted recompute and full refresh — and
-- prevents deadlocks on the cache table rows.
--
-- This lock is narrow and low-frequency:
--   - recompute: ~2s maintenance timer, one 100-key batch per call
--   - refresh: ~60s reconciliation timer, or once at migrate()
-- The protected tables are the same three shared cache tables.
-- Do NOT extend this lock to other paths.
--
-- Targeted recompute: drains dirty keys and recomputes exact cached
-- rows from base tables using the (queue, state) index.

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
    -- Serialize concurrent callers. Without this, two callers processing
    -- different dirty queues can deadlock on job_queue_catalog/job_kind_catalog
    -- upserts. Uses a blocking advisory lock (released at commit) so callers
    -- wait rather than skip, ensuring flush_dirty_admin_metadata() actually
    -- drains all keys.
    PERFORM pg_advisory_xact_lock(1098018130);

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

        -- Upsert queue catalog (ref_count derived from state counts already computed)
        v_ref_count := v_scheduled + v_available + v_running + v_completed
                     + v_retryable + v_failed + v_cancelled + v_waiting_external;

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
    -- Serialize against recompute_dirty_admin_metadata() — both functions
    -- upsert into queue_state_counts/job_kind_catalog/job_queue_catalog.
    -- Without this, concurrent callers deadlock on the cache table rows.
    PERFORM pg_advisory_xact_lock(1098018130);

    -- Clear dirty keys since a full refresh supersedes pending recomputes.
    TRUNCATE awa.admin_dirty_queues, awa.admin_dirty_kinds;

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

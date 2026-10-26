-- v046: Wait-free admin dirty-key marks (#492)
--
-- The v006 dirty-key triggers on jobs_hot / scheduled_jobs wrote
-- INSERT ... ON CONFLICT DO NOTHING into tables keyed by queue and kind.
-- That blocks on another transaction whenever the keyed row is uncommitted,
-- which the 2s drain and the TRUNCATE-based 60s refresh made routine, and a
-- trigger that can wait is a deadlock edge between otherwise unrelated job
-- transitions (ADR-045). The marks become append-only rows in tables with no
-- index or constraint, so the trigger INSERT never waits; the drain deletes the
-- visible rows it recounts, and the refresh deletes instead of truncating.
--
-- N-1 (0.6.x) binaries call recompute_dirty_admin_metadata(int) and
-- refresh_admin_metadata() by name and never read the dirty tables, so they
-- operate unchanged on this schema. The old admin_dirty_queues /
-- admin_dirty_kinds tables stay in place, unread, for a later contract
-- migration: dropping them here would take ACCESS EXCLUSIVE while released
-- trigger bodies are mid-statement. No lock here conflicts with job traffic;
-- the migration completes in milliseconds.

CREATE TABLE IF NOT EXISTS awa.admin_dirty_queue_marks (
    queue      TEXT        NOT NULL,
    touched_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS awa.admin_dirty_kind_marks (
    kind       TEXT        NOT NULL,
    touched_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Carry pending dirtiness across the switch so nothing waits for the next
-- full refresh. Marks that released trigger bodies commit into the old
-- tables after this copy are reconciled by refresh_admin_metadata().
INSERT INTO awa.admin_dirty_queue_marks (queue, touched_at)
SELECT queue, touched_at FROM awa.admin_dirty_queues;

INSERT INTO awa.admin_dirty_kind_marks (kind, touched_at)
SELECT kind, touched_at FROM awa.admin_dirty_kinds;

-- ── Trigger functions: plain inserts, no arbiter index, no waits ────────
--
-- These run inside every job transition. They must never reference a table
-- with a unique or exclusion constraint, never use ON CONFLICT, and never
-- UPDATE or DELETE a shared row: any of those can block on another
-- transaction and turn the trigger into a deadlock edge.

CREATE OR REPLACE FUNCTION awa.mark_dirty_keys_insert() RETURNS trigger AS $$
BEGIN
    INSERT INTO awa.admin_dirty_queue_marks (queue)
    SELECT DISTINCT queue FROM new_rows;

    INSERT INTO awa.admin_dirty_kind_marks (kind)
    SELECT DISTINCT kind FROM new_rows;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION awa.mark_dirty_keys_delete() RETURNS trigger AS $$
BEGIN
    INSERT INTO awa.admin_dirty_queue_marks (queue)
    SELECT DISTINCT queue FROM old_rows;

    INSERT INTO awa.admin_dirty_kind_marks (kind)
    SELECT DISTINCT kind FROM old_rows;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION awa.mark_dirty_keys_update() RETURNS trigger AS $$
BEGIN
    -- Only mark when queue, kind, or state actually changed. Heartbeat and
    -- progress-only updates don't affect admin caches.
    INSERT INTO awa.admin_dirty_queue_marks (queue)
    SELECT DISTINCT queue FROM (
        SELECT o.queue FROM old_rows o JOIN new_rows n ON o.id = n.id
        WHERE o.queue IS DISTINCT FROM n.queue
           OR o.state IS DISTINCT FROM n.state
        UNION
        SELECT n.queue FROM old_rows o JOIN new_rows n ON o.id = n.id
        WHERE o.queue IS DISTINCT FROM n.queue
           OR o.state IS DISTINCT FROM n.state
    ) t;

    INSERT INTO awa.admin_dirty_kind_marks (kind)
    SELECT DISTINCT kind FROM (
        SELECT o.kind FROM old_rows o JOIN new_rows n ON o.id = n.id
        WHERE o.kind IS DISTINCT FROM n.kind
        UNION
        SELECT n.kind FROM old_rows o JOIN new_rows n ON o.id = n.id
        WHERE o.kind IS DISTINCT FROM n.kind
    ) t;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- ── Drain: read visible keys, delete those rows, recompute ──────────────
--
-- LOCKING CONTRACT (unchanged from v006): every writer of queue_state_counts,
-- job_kind_catalog, or job_queue_catalog takes pg_advisory_xact_lock(1098018130)
-- first. The drain and the full refresh are the only writers.
--
-- Exactness: each statement runs under its own READ COMMITTED snapshot. A mark
-- the DELETE can see belongs to a transaction that committed before the
-- DELETE began, so the recount statements that follow see that transaction's
-- rows too. A mark the DELETE cannot see is left for the next drain.

CREATE OR REPLACE FUNCTION awa.recompute_dirty_admin_metadata(
    p_batch_size INT DEFAULT 100
) RETURNS INT AS $$
DECLARE
    affected INT := 0;
    dirty_queues TEXT[];
    dirty_kinds TEXT[];
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
    PERFORM pg_advisory_xact_lock(1098018130);

    -- Oldest-touched keys first so a hot queue cannot starve the rest.
    SELECT coalesce(array_agg(queue), '{}'::text[])
    INTO dirty_queues
    FROM (
        SELECT queue
        FROM awa.admin_dirty_queue_marks
        GROUP BY queue
        ORDER BY min(touched_at)
        LIMIT p_batch_size
    ) keys;

    DELETE FROM awa.admin_dirty_queue_marks WHERE queue = ANY(dirty_queues);

    FOREACH dirty_q IN ARRAY dirty_queues LOOP
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

        v_ref_count := v_scheduled + v_available + v_running + v_completed
                     + v_retryable + v_failed + v_cancelled + v_waiting_external;

        IF v_ref_count > 0 THEN
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

            INSERT INTO awa.job_queue_catalog (queue, ref_count)
            VALUES (dirty_q, v_ref_count)
            ON CONFLICT (queue) DO UPDATE SET ref_count = EXCLUDED.ref_count;
        ELSE
            DELETE FROM awa.queue_state_counts WHERE queue = dirty_q;
            DELETE FROM awa.job_queue_catalog WHERE queue = dirty_q;
        END IF;

        affected := affected + 1;
    END LOOP;

    SELECT coalesce(array_agg(kind), '{}'::text[])
    INTO dirty_kinds
    FROM (
        SELECT kind
        FROM awa.admin_dirty_kind_marks
        GROUP BY kind
        ORDER BY min(touched_at)
        LIMIT p_batch_size
    ) keys;

    DELETE FROM awa.admin_dirty_kind_marks WHERE kind = ANY(dirty_kinds);

    FOREACH dirty_k IN ARRAY dirty_kinds LOOP
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

-- ── Full reconciliation without TRUNCATE ────────────────────────────────
--
-- Visible marks are deleted up front: the recount statements below see every
-- transaction those marks refer to. Marks committed while the refresh runs
-- stay for the next drain. No ACCESS EXCLUSIVE lock is taken, so job
-- transitions never queue behind a refresh.

CREATE OR REPLACE FUNCTION awa.refresh_admin_metadata() RETURNS VOID AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(1098018130);

    DELETE FROM awa.admin_dirty_queue_marks;
    DELETE FROM awa.admin_dirty_kind_marks;

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
    WHERE queue NOT IN (
        SELECT DISTINCT queue FROM awa.jobs_hot
        UNION
        SELECT DISTINCT queue FROM awa.scheduled_jobs
    );

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
END;
$$ LANGUAGE plpgsql;

INSERT INTO awa.schema_version (version, description)
VALUES (46, 'Wait-free admin dirty-key marks')
ON CONFLICT (version) DO NOTHING;

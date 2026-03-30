-- v006: Remove statement-level triggers from jobs_hot
--
-- The AFTER UPDATE statement-level trigger on jobs_hot synchronously updates
-- queue_state_counts inside the same transaction. Under high concurrency
-- (many parallel batch completions), this creates a cross-table deadlock:
-- concurrent transactions hold row locks on different jobs_hot rows while
-- their triggers compete for the same queue_state_counts row.
--
-- Fix: drop the jobs_hot triggers entirely. Counter tables become eventually
-- consistent, refreshed periodically by the maintenance leader. The
-- scheduled_jobs triggers are kept (low throughput, no deadlock risk).
--
-- rebuild_admin_metadata() remains available for full reconciliation.
-- A new refresh_admin_metadata() function performs an incremental upsert
-- suitable for frequent calls without TRUNCATE.

-- Drop the three statement-level triggers on jobs_hot
DROP TRIGGER IF EXISTS trg_jobs_hot_admin_metadata_insert_stmt ON awa.jobs_hot;
DROP TRIGGER IF EXISTS trg_jobs_hot_admin_metadata_update_stmt ON awa.jobs_hot;
DROP TRIGGER IF EXISTS trg_jobs_hot_admin_metadata_delete_stmt ON awa.jobs_hot;

-- Also drop the legacy per-row trigger if it somehow still exists
DROP TRIGGER IF EXISTS trg_jobs_hot_admin_metadata ON awa.jobs_hot;

-- Incremental refresh: upsert current counts from jobs_hot + scheduled_jobs.
-- Unlike rebuild_admin_metadata() this does not TRUNCATE, so concurrent
-- readers always see a consistent (if slightly stale) snapshot.
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

    -- Remove stale queues that no longer have any jobs
    DELETE FROM awa.queue_state_counts
    WHERE scheduled = 0 AND available = 0 AND running = 0
      AND completed = 0 AND retryable = 0 AND failed = 0
      AND cancelled = 0 AND waiting_external = 0
      AND queue NOT IN (
          SELECT DISTINCT queue FROM awa.jobs_hot
          UNION
          SELECT DISTINCT queue FROM awa.scheduled_jobs
      );

    -- Refresh job_kind_catalog
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

    -- Refresh job_queue_catalog
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

-- Run an initial refresh to ensure counts are correct after trigger removal
SELECT awa.refresh_admin_metadata();

INSERT INTO awa.schema_version (version, description)
VALUES (6, 'Remove hot-table triggers, async counter refresh') ON CONFLICT (version) DO NOTHING;

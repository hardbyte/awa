CREATE TABLE IF NOT EXISTS awa.queue_state_counts (
    queue             TEXT PRIMARY KEY,
    scheduled         BIGINT NOT NULL DEFAULT 0,
    available         BIGINT NOT NULL DEFAULT 0,
    running           BIGINT NOT NULL DEFAULT 0,
    completed         BIGINT NOT NULL DEFAULT 0,
    retryable         BIGINT NOT NULL DEFAULT 0,
    failed            BIGINT NOT NULL DEFAULT 0,
    cancelled         BIGINT NOT NULL DEFAULT 0,
    waiting_external  BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS awa.job_kind_catalog (
    kind        TEXT PRIMARY KEY,
    ref_count   BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS awa.job_queue_catalog (
    queue       TEXT PRIMARY KEY,
    ref_count   BIGINT NOT NULL DEFAULT 0
);

CREATE OR REPLACE FUNCTION awa.apply_queue_state_delta(
    p_queue TEXT,
    p_state awa.job_state,
    p_delta BIGINT
) RETURNS VOID AS $$
BEGIN
    IF p_queue IS NULL OR p_delta = 0 THEN
        RETURN;
    END IF;

    INSERT INTO awa.queue_state_counts (queue)
    VALUES (p_queue)
    ON CONFLICT (queue) DO NOTHING;

    -- Clamp at zero because test/bulk-load paths may bypass triggers and
    -- then backfill or replay deltas manually. `awa.rebuild_admin_metadata()`
    -- can always be used to reconcile these advisory counters from source rows.
    UPDATE awa.queue_state_counts
    SET scheduled = GREATEST(0, scheduled + CASE WHEN p_state = 'scheduled' THEN p_delta ELSE 0 END),
        available = GREATEST(0, available + CASE WHEN p_state = 'available' THEN p_delta ELSE 0 END),
        running = GREATEST(0, running + CASE WHEN p_state = 'running' THEN p_delta ELSE 0 END),
        completed = GREATEST(0, completed + CASE WHEN p_state = 'completed' THEN p_delta ELSE 0 END),
        retryable = GREATEST(0, retryable + CASE WHEN p_state = 'retryable' THEN p_delta ELSE 0 END),
        failed = GREATEST(0, failed + CASE WHEN p_state = 'failed' THEN p_delta ELSE 0 END),
        cancelled = GREATEST(0, cancelled + CASE WHEN p_state = 'cancelled' THEN p_delta ELSE 0 END),
        waiting_external = GREATEST(0, waiting_external + CASE WHEN p_state = 'waiting_external' THEN p_delta ELSE 0 END)
    WHERE queue = p_queue;

    DELETE FROM awa.queue_state_counts
    WHERE queue = p_queue
      AND scheduled = 0
      AND available = 0
      AND running = 0
      AND completed = 0
      AND retryable = 0
      AND failed = 0
      AND cancelled = 0
      AND waiting_external = 0;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION awa.apply_job_kind_delta(
    p_kind TEXT,
    p_delta BIGINT
) RETURNS VOID AS $$
BEGIN
    IF p_kind IS NULL OR p_delta = 0 THEN
        RETURN;
    END IF;

    INSERT INTO awa.job_kind_catalog (kind, ref_count)
    VALUES (p_kind, GREATEST(p_delta, 0))
    ON CONFLICT (kind) DO UPDATE
    SET ref_count = GREATEST(0, awa.job_kind_catalog.ref_count + EXCLUDED.ref_count
                    + LEAST(p_delta, 0));

    DELETE FROM awa.job_kind_catalog
    WHERE kind = p_kind
      AND ref_count <= 0;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION awa.apply_job_queue_delta(
    p_queue TEXT,
    p_delta BIGINT
) RETURNS VOID AS $$
BEGIN
    IF p_queue IS NULL OR p_delta = 0 THEN
        RETURN;
    END IF;

    INSERT INTO awa.job_queue_catalog (queue, ref_count)
    VALUES (p_queue, GREATEST(p_delta, 0))
    ON CONFLICT (queue) DO UPDATE
    SET ref_count = GREATEST(0, awa.job_queue_catalog.ref_count + EXCLUDED.ref_count
                    + LEAST(p_delta, 0));

    DELETE FROM awa.job_queue_catalog
    WHERE queue = p_queue
      AND ref_count <= 0;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION awa.sync_job_admin_metadata() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM awa.apply_queue_state_delta(NEW.queue, NEW.state, 1);
        PERFORM awa.apply_job_kind_delta(NEW.kind, 1);
        PERFORM awa.apply_job_queue_delta(NEW.queue, 1);
        RETURN NEW;
    END IF;

    IF TG_OP = 'DELETE' THEN
        PERFORM awa.apply_queue_state_delta(OLD.queue, OLD.state, -1);
        PERFORM awa.apply_job_kind_delta(OLD.kind, -1);
        PERFORM awa.apply_job_queue_delta(OLD.queue, -1);
        RETURN OLD;
    END IF;

    IF OLD.queue IS DISTINCT FROM NEW.queue OR OLD.state IS DISTINCT FROM NEW.state THEN
        PERFORM awa.apply_queue_state_delta(OLD.queue, OLD.state, -1);
        PERFORM awa.apply_queue_state_delta(NEW.queue, NEW.state, 1);
    END IF;

    IF OLD.kind IS DISTINCT FROM NEW.kind THEN
        PERFORM awa.apply_job_kind_delta(OLD.kind, -1);
        PERFORM awa.apply_job_kind_delta(NEW.kind, 1);
    END IF;

    IF OLD.queue IS DISTINCT FROM NEW.queue THEN
        PERFORM awa.apply_job_queue_delta(OLD.queue, -1);
        PERFORM awa.apply_job_queue_delta(NEW.queue, 1);
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION awa.rebuild_admin_metadata() RETURNS VOID AS $$
BEGIN
    TRUNCATE awa.queue_state_counts, awa.job_kind_catalog, awa.job_queue_catalog;

    INSERT INTO awa.queue_state_counts (
        queue, scheduled, available, running, completed,
        retryable, failed, cancelled, waiting_external
    )
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
    GROUP BY queue;

    INSERT INTO awa.job_kind_catalog (kind, ref_count)
    SELECT kind, count(*) AS ref_count
    FROM (
        SELECT kind FROM awa.jobs_hot
        UNION ALL
        SELECT kind FROM awa.scheduled_jobs
    ) AS jobs
    GROUP BY kind;

    INSERT INTO awa.job_queue_catalog (queue, ref_count)
    SELECT queue, count(*) AS ref_count
    FROM (
        SELECT queue FROM awa.jobs_hot
        UNION ALL
        SELECT queue FROM awa.scheduled_jobs
    ) AS jobs
    GROUP BY queue;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_jobs_hot_admin_metadata ON awa.jobs_hot;
CREATE TRIGGER trg_jobs_hot_admin_metadata
    AFTER INSERT OR UPDATE OR DELETE ON awa.jobs_hot
    FOR EACH ROW
    EXECUTE FUNCTION awa.sync_job_admin_metadata();

DROP TRIGGER IF EXISTS trg_scheduled_jobs_admin_metadata ON awa.scheduled_jobs;
CREATE TRIGGER trg_scheduled_jobs_admin_metadata
    AFTER INSERT OR UPDATE OR DELETE ON awa.scheduled_jobs
    FOR EACH ROW
    EXECUTE FUNCTION awa.sync_job_admin_metadata();

SELECT awa.rebuild_admin_metadata();

CREATE INDEX IF NOT EXISTS idx_awa_jobs_hot_completed_finalized
    ON awa.jobs_hot (finalized_at DESC, queue)
    WHERE state = 'completed' AND finalized_at IS NOT NULL;

INSERT INTO awa.schema_version (version, description)
VALUES (4, 'Admin metadata cache tables') ON CONFLICT (version) DO NOTHING;

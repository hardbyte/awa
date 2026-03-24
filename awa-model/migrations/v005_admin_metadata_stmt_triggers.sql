CREATE OR REPLACE FUNCTION awa.sync_job_admin_metadata_stmt_insert() RETURNS trigger AS $$
BEGIN
    WITH queue_deltas AS (
        SELECT
            queue,
            count(*) FILTER (WHERE state = 'scheduled')::bigint AS scheduled_delta,
            count(*) FILTER (WHERE state = 'available')::bigint AS available_delta,
            count(*) FILTER (WHERE state = 'running')::bigint AS running_delta,
            count(*) FILTER (WHERE state = 'completed')::bigint AS completed_delta,
            count(*) FILTER (WHERE state = 'retryable')::bigint AS retryable_delta,
            count(*) FILTER (WHERE state = 'failed')::bigint AS failed_delta,
            count(*) FILTER (WHERE state = 'cancelled')::bigint AS cancelled_delta,
            count(*) FILTER (WHERE state = 'waiting_external')::bigint AS waiting_external_delta
        FROM new_rows
        GROUP BY queue
    )
    INSERT INTO awa.queue_state_counts (queue)
    SELECT queue FROM queue_deltas
    ON CONFLICT (queue) DO NOTHING;

    WITH queue_deltas AS (
        SELECT
            queue,
            count(*) FILTER (WHERE state = 'scheduled')::bigint AS scheduled_delta,
            count(*) FILTER (WHERE state = 'available')::bigint AS available_delta,
            count(*) FILTER (WHERE state = 'running')::bigint AS running_delta,
            count(*) FILTER (WHERE state = 'completed')::bigint AS completed_delta,
            count(*) FILTER (WHERE state = 'retryable')::bigint AS retryable_delta,
            count(*) FILTER (WHERE state = 'failed')::bigint AS failed_delta,
            count(*) FILTER (WHERE state = 'cancelled')::bigint AS cancelled_delta,
            count(*) FILTER (WHERE state = 'waiting_external')::bigint AS waiting_external_delta
        FROM new_rows
        GROUP BY queue
    )
    UPDATE awa.queue_state_counts qs
    SET scheduled = GREATEST(0, qs.scheduled + d.scheduled_delta),
        available = GREATEST(0, qs.available + d.available_delta),
        running = GREATEST(0, qs.running + d.running_delta),
        completed = GREATEST(0, qs.completed + d.completed_delta),
        retryable = GREATEST(0, qs.retryable + d.retryable_delta),
        failed = GREATEST(0, qs.failed + d.failed_delta),
        cancelled = GREATEST(0, qs.cancelled + d.cancelled_delta),
        waiting_external = GREATEST(0, qs.waiting_external + d.waiting_external_delta)
    FROM queue_deltas d
    WHERE qs.queue = d.queue;

    WITH kind_deltas AS (
        SELECT kind, count(*)::bigint AS delta
        FROM new_rows
        GROUP BY kind
    )
    INSERT INTO awa.job_kind_catalog (kind)
    SELECT kind FROM kind_deltas
    ON CONFLICT (kind) DO NOTHING;

    WITH kind_deltas AS (
        SELECT kind, count(*)::bigint AS delta
        FROM new_rows
        GROUP BY kind
    )
    UPDATE awa.job_kind_catalog k
    SET ref_count = GREATEST(0, k.ref_count + d.delta)
    FROM kind_deltas d
    WHERE k.kind = d.kind;

    WITH queue_deltas AS (
        SELECT queue, count(*)::bigint AS delta
        FROM new_rows
        GROUP BY queue
    )
    INSERT INTO awa.job_queue_catalog (queue)
    SELECT queue FROM queue_deltas
    ON CONFLICT (queue) DO NOTHING;

    WITH queue_deltas AS (
        SELECT queue, count(*)::bigint AS delta
        FROM new_rows
        GROUP BY queue
    )
    UPDATE awa.job_queue_catalog q
    SET ref_count = GREATEST(0, q.ref_count + d.delta)
    FROM queue_deltas d
    WHERE q.queue = d.queue;

    DELETE FROM awa.queue_state_counts
    WHERE queue IN (SELECT DISTINCT queue FROM new_rows)
      AND scheduled = 0
      AND available = 0
      AND running = 0
      AND completed = 0
      AND retryable = 0
      AND failed = 0
      AND cancelled = 0
      AND waiting_external = 0;

    DELETE FROM awa.job_kind_catalog
    WHERE kind IN (SELECT DISTINCT kind FROM new_rows)
      AND ref_count <= 0;

    DELETE FROM awa.job_queue_catalog
    WHERE queue IN (SELECT DISTINCT queue FROM new_rows)
      AND ref_count <= 0;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION awa.sync_job_admin_metadata_stmt_delete() RETURNS trigger AS $$
BEGIN
    WITH queue_deltas AS (
        SELECT
            queue,
            -count(*) FILTER (WHERE state = 'scheduled')::bigint AS scheduled_delta,
            -count(*) FILTER (WHERE state = 'available')::bigint AS available_delta,
            -count(*) FILTER (WHERE state = 'running')::bigint AS running_delta,
            -count(*) FILTER (WHERE state = 'completed')::bigint AS completed_delta,
            -count(*) FILTER (WHERE state = 'retryable')::bigint AS retryable_delta,
            -count(*) FILTER (WHERE state = 'failed')::bigint AS failed_delta,
            -count(*) FILTER (WHERE state = 'cancelled')::bigint AS cancelled_delta,
            -count(*) FILTER (WHERE state = 'waiting_external')::bigint AS waiting_external_delta
        FROM old_rows
        GROUP BY queue
    )
    INSERT INTO awa.queue_state_counts (queue)
    SELECT queue FROM queue_deltas
    ON CONFLICT (queue) DO NOTHING;

    WITH queue_deltas AS (
        SELECT
            queue,
            -count(*) FILTER (WHERE state = 'scheduled')::bigint AS scheduled_delta,
            -count(*) FILTER (WHERE state = 'available')::bigint AS available_delta,
            -count(*) FILTER (WHERE state = 'running')::bigint AS running_delta,
            -count(*) FILTER (WHERE state = 'completed')::bigint AS completed_delta,
            -count(*) FILTER (WHERE state = 'retryable')::bigint AS retryable_delta,
            -count(*) FILTER (WHERE state = 'failed')::bigint AS failed_delta,
            -count(*) FILTER (WHERE state = 'cancelled')::bigint AS cancelled_delta,
            -count(*) FILTER (WHERE state = 'waiting_external')::bigint AS waiting_external_delta
        FROM old_rows
        GROUP BY queue
    )
    UPDATE awa.queue_state_counts qs
    SET scheduled = GREATEST(0, qs.scheduled + d.scheduled_delta),
        available = GREATEST(0, qs.available + d.available_delta),
        running = GREATEST(0, qs.running + d.running_delta),
        completed = GREATEST(0, qs.completed + d.completed_delta),
        retryable = GREATEST(0, qs.retryable + d.retryable_delta),
        failed = GREATEST(0, qs.failed + d.failed_delta),
        cancelled = GREATEST(0, qs.cancelled + d.cancelled_delta),
        waiting_external = GREATEST(0, qs.waiting_external + d.waiting_external_delta)
    FROM queue_deltas d
    WHERE qs.queue = d.queue;

    WITH kind_deltas AS (
        SELECT kind, -count(*)::bigint AS delta
        FROM old_rows
        GROUP BY kind
    )
    INSERT INTO awa.job_kind_catalog (kind)
    SELECT kind FROM kind_deltas
    ON CONFLICT (kind) DO NOTHING;

    WITH kind_deltas AS (
        SELECT kind, -count(*)::bigint AS delta
        FROM old_rows
        GROUP BY kind
    )
    UPDATE awa.job_kind_catalog k
    SET ref_count = GREATEST(0, k.ref_count + d.delta)
    FROM kind_deltas d
    WHERE k.kind = d.kind;

    WITH queue_deltas AS (
        SELECT queue, -count(*)::bigint AS delta
        FROM old_rows
        GROUP BY queue
    )
    INSERT INTO awa.job_queue_catalog (queue)
    SELECT queue FROM queue_deltas
    ON CONFLICT (queue) DO NOTHING;

    WITH queue_deltas AS (
        SELECT queue, -count(*)::bigint AS delta
        FROM old_rows
        GROUP BY queue
    )
    UPDATE awa.job_queue_catalog q
    SET ref_count = GREATEST(0, q.ref_count + d.delta)
    FROM queue_deltas d
    WHERE q.queue = d.queue;

    DELETE FROM awa.queue_state_counts
    WHERE queue IN (SELECT DISTINCT queue FROM old_rows)
      AND scheduled = 0
      AND available = 0
      AND running = 0
      AND completed = 0
      AND retryable = 0
      AND failed = 0
      AND cancelled = 0
      AND waiting_external = 0;

    DELETE FROM awa.job_kind_catalog
    WHERE kind IN (SELECT DISTINCT kind FROM old_rows)
      AND ref_count <= 0;

    DELETE FROM awa.job_queue_catalog
    WHERE queue IN (SELECT DISTINCT queue FROM old_rows)
      AND ref_count <= 0;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION awa.sync_job_admin_metadata_stmt_update() RETURNS trigger AS $$
BEGIN
    WITH state_deltas AS (
        SELECT queue, state, sum(delta)::bigint AS delta
        FROM (
            SELECT queue, state, -1::bigint AS delta FROM old_rows
            UNION ALL
            SELECT queue, state, 1::bigint AS delta FROM new_rows
        ) deltas
        GROUP BY queue, state
        HAVING sum(delta) <> 0
    ),
    queue_deltas AS (
        SELECT
            queue,
            sum(CASE WHEN state = 'scheduled' THEN delta ELSE 0 END)::bigint AS scheduled_delta,
            sum(CASE WHEN state = 'available' THEN delta ELSE 0 END)::bigint AS available_delta,
            sum(CASE WHEN state = 'running' THEN delta ELSE 0 END)::bigint AS running_delta,
            sum(CASE WHEN state = 'completed' THEN delta ELSE 0 END)::bigint AS completed_delta,
            sum(CASE WHEN state = 'retryable' THEN delta ELSE 0 END)::bigint AS retryable_delta,
            sum(CASE WHEN state = 'failed' THEN delta ELSE 0 END)::bigint AS failed_delta,
            sum(CASE WHEN state = 'cancelled' THEN delta ELSE 0 END)::bigint AS cancelled_delta,
            sum(CASE WHEN state = 'waiting_external' THEN delta ELSE 0 END)::bigint AS waiting_external_delta
        FROM state_deltas
        GROUP BY queue
    )
    INSERT INTO awa.queue_state_counts (queue)
    SELECT queue FROM queue_deltas
    ON CONFLICT (queue) DO NOTHING;

    WITH state_deltas AS (
        SELECT queue, state, sum(delta)::bigint AS delta
        FROM (
            SELECT queue, state, -1::bigint AS delta FROM old_rows
            UNION ALL
            SELECT queue, state, 1::bigint AS delta FROM new_rows
        ) deltas
        GROUP BY queue, state
        HAVING sum(delta) <> 0
    ),
    queue_deltas AS (
        SELECT
            queue,
            sum(CASE WHEN state = 'scheduled' THEN delta ELSE 0 END)::bigint AS scheduled_delta,
            sum(CASE WHEN state = 'available' THEN delta ELSE 0 END)::bigint AS available_delta,
            sum(CASE WHEN state = 'running' THEN delta ELSE 0 END)::bigint AS running_delta,
            sum(CASE WHEN state = 'completed' THEN delta ELSE 0 END)::bigint AS completed_delta,
            sum(CASE WHEN state = 'retryable' THEN delta ELSE 0 END)::bigint AS retryable_delta,
            sum(CASE WHEN state = 'failed' THEN delta ELSE 0 END)::bigint AS failed_delta,
            sum(CASE WHEN state = 'cancelled' THEN delta ELSE 0 END)::bigint AS cancelled_delta,
            sum(CASE WHEN state = 'waiting_external' THEN delta ELSE 0 END)::bigint AS waiting_external_delta
        FROM state_deltas
        GROUP BY queue
    )
    UPDATE awa.queue_state_counts qs
    SET scheduled = GREATEST(0, qs.scheduled + d.scheduled_delta),
        available = GREATEST(0, qs.available + d.available_delta),
        running = GREATEST(0, qs.running + d.running_delta),
        completed = GREATEST(0, qs.completed + d.completed_delta),
        retryable = GREATEST(0, qs.retryable + d.retryable_delta),
        failed = GREATEST(0, qs.failed + d.failed_delta),
        cancelled = GREATEST(0, qs.cancelled + d.cancelled_delta),
        waiting_external = GREATEST(0, qs.waiting_external + d.waiting_external_delta)
    FROM queue_deltas d
    WHERE qs.queue = d.queue;

    WITH kind_deltas AS (
        SELECT kind, sum(delta)::bigint AS delta
        FROM (
            SELECT kind, -1::bigint AS delta FROM old_rows
            UNION ALL
            SELECT kind, 1::bigint AS delta FROM new_rows
        ) deltas
        GROUP BY kind
        HAVING sum(delta) <> 0
    )
    INSERT INTO awa.job_kind_catalog (kind)
    SELECT kind FROM kind_deltas
    ON CONFLICT (kind) DO NOTHING;

    WITH kind_deltas AS (
        SELECT kind, sum(delta)::bigint AS delta
        FROM (
            SELECT kind, -1::bigint AS delta FROM old_rows
            UNION ALL
            SELECT kind, 1::bigint AS delta FROM new_rows
        ) deltas
        GROUP BY kind
        HAVING sum(delta) <> 0
    )
    UPDATE awa.job_kind_catalog k
    SET ref_count = GREATEST(0, k.ref_count + d.delta)
    FROM kind_deltas d
    WHERE k.kind = d.kind;

    WITH queue_deltas AS (
        SELECT queue, sum(delta)::bigint AS delta
        FROM (
            SELECT queue, -1::bigint AS delta FROM old_rows
            UNION ALL
            SELECT queue, 1::bigint AS delta FROM new_rows
        ) deltas
        GROUP BY queue
        HAVING sum(delta) <> 0
    )
    INSERT INTO awa.job_queue_catalog (queue)
    SELECT queue FROM queue_deltas
    ON CONFLICT (queue) DO NOTHING;

    WITH queue_deltas AS (
        SELECT queue, sum(delta)::bigint AS delta
        FROM (
            SELECT queue, -1::bigint AS delta FROM old_rows
            UNION ALL
            SELECT queue, 1::bigint AS delta FROM new_rows
        ) deltas
        GROUP BY queue
        HAVING sum(delta) <> 0
    )
    UPDATE awa.job_queue_catalog q
    SET ref_count = GREATEST(0, q.ref_count + d.delta)
    FROM queue_deltas d
    WHERE q.queue = d.queue;

    DELETE FROM awa.queue_state_counts
    WHERE queue IN (
        SELECT queue FROM old_rows
        UNION
        SELECT queue FROM new_rows
    )
      AND scheduled = 0
      AND available = 0
      AND running = 0
      AND completed = 0
      AND retryable = 0
      AND failed = 0
      AND cancelled = 0
      AND waiting_external = 0;

    DELETE FROM awa.job_kind_catalog
    WHERE kind IN (
        SELECT kind FROM old_rows
        UNION
        SELECT kind FROM new_rows
    )
      AND ref_count <= 0;

    DELETE FROM awa.job_queue_catalog
    WHERE queue IN (
        SELECT queue FROM old_rows
        UNION
        SELECT queue FROM new_rows
    )
      AND ref_count <= 0;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_jobs_hot_admin_metadata ON awa.jobs_hot;
DROP TRIGGER IF EXISTS trg_scheduled_jobs_admin_metadata ON awa.scheduled_jobs;

DROP TRIGGER IF EXISTS trg_jobs_hot_admin_metadata_insert_stmt ON awa.jobs_hot;
CREATE TRIGGER trg_jobs_hot_admin_metadata_insert_stmt
    AFTER INSERT ON awa.jobs_hot
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION awa.sync_job_admin_metadata_stmt_insert();

DROP TRIGGER IF EXISTS trg_jobs_hot_admin_metadata_delete_stmt ON awa.jobs_hot;
CREATE TRIGGER trg_jobs_hot_admin_metadata_delete_stmt
    AFTER DELETE ON awa.jobs_hot
    REFERENCING OLD TABLE AS old_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION awa.sync_job_admin_metadata_stmt_delete();

DROP TRIGGER IF EXISTS trg_jobs_hot_admin_metadata_update_stmt ON awa.jobs_hot;
CREATE TRIGGER trg_jobs_hot_admin_metadata_update_stmt
    AFTER UPDATE ON awa.jobs_hot
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION awa.sync_job_admin_metadata_stmt_update();

DROP TRIGGER IF EXISTS trg_scheduled_jobs_admin_metadata_insert_stmt ON awa.scheduled_jobs;
CREATE TRIGGER trg_scheduled_jobs_admin_metadata_insert_stmt
    AFTER INSERT ON awa.scheduled_jobs
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION awa.sync_job_admin_metadata_stmt_insert();

DROP TRIGGER IF EXISTS trg_scheduled_jobs_admin_metadata_delete_stmt ON awa.scheduled_jobs;
CREATE TRIGGER trg_scheduled_jobs_admin_metadata_delete_stmt
    AFTER DELETE ON awa.scheduled_jobs
    REFERENCING OLD TABLE AS old_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION awa.sync_job_admin_metadata_stmt_delete();

DROP TRIGGER IF EXISTS trg_scheduled_jobs_admin_metadata_update_stmt ON awa.scheduled_jobs;
CREATE TRIGGER trg_scheduled_jobs_admin_metadata_update_stmt
    AFTER UPDATE ON awa.scheduled_jobs
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION awa.sync_job_admin_metadata_stmt_update();

SELECT awa.rebuild_admin_metadata();

INSERT INTO awa.schema_version (version, description)
VALUES (5, 'Statement-level admin metadata triggers') ON CONFLICT (version) DO NOTHING;

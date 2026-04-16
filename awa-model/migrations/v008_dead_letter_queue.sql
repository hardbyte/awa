-- Migration v008: First-class Dead Letter Queue (DLQ) support.
--
-- Introduces awa.jobs_dlq — a separate physical table for permanently-failed
-- jobs. Moving failed jobs into the DLQ keeps jobs_hot free of poison rows
-- that would otherwise bloat the claim-path indexes (ADR-012 hot/cold split)
-- and complicate depth metrics. DLQ rows are never claimed.
--
-- The table mirrors jobs_hot's schema exactly, plus three DLQ-specific
-- columns. We deliberately do NOT UNION the DLQ into the awa.jobs view:
-- DLQ is a separate operator-facing namespace, accessed through dedicated
-- APIs (list_dlq / retry_from_dlq / purge_dlq / dlq_depth). Keeping the
-- existing view stable preserves the semantics of retry_failed_by_kind,
-- discard_failed, list_jobs, and every existing write-through trigger.

CREATE TABLE IF NOT EXISTS awa.jobs_dlq (
    id                  BIGINT      NOT NULL PRIMARY KEY,
    kind                TEXT        NOT NULL,
    queue               TEXT        NOT NULL DEFAULT 'default',
    args                JSONB       NOT NULL DEFAULT '{}',
    state               awa.job_state NOT NULL,
    priority            SMALLINT    NOT NULL DEFAULT 2,
    attempt             SMALLINT    NOT NULL DEFAULT 0,
    max_attempts        SMALLINT    NOT NULL DEFAULT 25,
    run_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    heartbeat_at        TIMESTAMPTZ,
    deadline_at         TIMESTAMPTZ,
    attempted_at        TIMESTAMPTZ,
    finalized_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    errors              JSONB[]     DEFAULT '{}',
    metadata            JSONB       NOT NULL DEFAULT '{}',
    tags                TEXT[]      NOT NULL DEFAULT '{}',
    unique_key          BYTEA,
    unique_states       BIT(8),
    callback_id         UUID,
    callback_timeout_at TIMESTAMPTZ,
    callback_filter     TEXT,
    callback_on_complete TEXT,
    callback_on_fail    TEXT,
    callback_transform  TEXT,
    run_lease           BIGINT      NOT NULL DEFAULT 0,
    progress            JSONB,

    -- DLQ-specific metadata
    dlq_reason          TEXT        NOT NULL DEFAULT '',
    dlq_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    original_run_lease  BIGINT      NOT NULL DEFAULT 0,

    CONSTRAINT jobs_dlq_state_check CHECK (state = 'failed'),
    CONSTRAINT jobs_dlq_priority_in_range CHECK (priority BETWEEN 1 AND 4),
    CONSTRAINT jobs_dlq_max_attempts_range CHECK (max_attempts BETWEEN 1 AND 1000),
    CONSTRAINT jobs_dlq_queue_name_length CHECK (length(queue) <= 200),
    CONSTRAINT jobs_dlq_kind_length CHECK (length(kind) <= 200),
    CONSTRAINT jobs_dlq_tags_count CHECK (cardinality(tags) <= 20)
);

-- Operator-facing indexes: DLQ browsing is filtered by queue/kind and sorted
-- by recency. No dequeue index because DLQ rows are never claimed.
CREATE INDEX IF NOT EXISTS idx_awa_jobs_dlq_queue_dlq_at
    ON awa.jobs_dlq (queue, dlq_at DESC);

CREATE INDEX IF NOT EXISTS idx_awa_jobs_dlq_kind_dlq_at
    ON awa.jobs_dlq (kind, dlq_at DESC);

CREATE INDEX IF NOT EXISTS idx_awa_jobs_dlq_dlq_at
    ON awa.jobs_dlq USING BRIN (dlq_at) WITH (pages_per_range = 32);

CREATE INDEX IF NOT EXISTS idx_awa_jobs_dlq_tags
    ON awa.jobs_dlq USING GIN (tags)
    WHERE tags IS NOT NULL AND tags != '{}';

-- Atomic, lease-guarded move from jobs_hot to jobs_dlq.
--
-- Mirrors the existing terminal-failure guard (state='running' AND
-- run_lease = caller's lease): stale completions or concurrent rescues
-- naturally fail the CTE and the caller sees rows_affected = 0.
--
-- Runs in a single statement so the DELETE and INSERT commit together.
-- Returns the new jobs_dlq row on success, NULL otherwise.
--
-- `p_progress` lets the caller preserve the handler's final progress snapshot
-- (otherwise checkpoint visibility would be lost when a poison job is DLQ'd).
CREATE OR REPLACE FUNCTION awa.move_to_dlq_guarded(
    p_job_id     BIGINT,
    p_run_lease  BIGINT,
    p_reason     TEXT,
    p_error      JSONB,
    p_progress   JSONB
) RETURNS SETOF awa.jobs_dlq AS $$
    WITH moved AS (
        DELETE FROM awa.jobs_hot
        WHERE id = p_job_id
          AND state = 'running'
          AND run_lease = p_run_lease
        RETURNING *
    )
    INSERT INTO awa.jobs_dlq (
        id, kind, queue, args, state, priority, attempt, max_attempts,
        run_at, heartbeat_at, deadline_at, attempted_at, finalized_at,
        created_at, errors, metadata, tags, unique_key, unique_states,
        callback_id, callback_timeout_at, callback_filter, callback_on_complete,
        callback_on_fail, callback_transform, run_lease, progress,
        dlq_reason, dlq_at, original_run_lease
    )
    SELECT
        id,
        kind,
        queue,
        args,
        'failed'::awa.job_state,
        priority,
        attempt,
        max_attempts,
        run_at,
        NULL,
        NULL,
        attempted_at,
        now(),
        created_at,
        errors || p_error,
        metadata,
        tags,
        unique_key,
        unique_states,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        0,
        p_progress,
        p_reason,
        now(),
        p_run_lease
    FROM moved
    RETURNING *;
$$ LANGUAGE sql;

-- Move an already-failed job (currently in jobs_hot) to the DLQ.
-- Used by bulk admin moves where the caller doesn't own a run_lease.
-- Guarded by state = 'failed' rather than run_lease.
CREATE OR REPLACE FUNCTION awa.move_failed_to_dlq(
    p_job_id     BIGINT,
    p_reason     TEXT
) RETURNS SETOF awa.jobs_dlq AS $$
    WITH moved AS (
        DELETE FROM awa.jobs_hot
        WHERE id = p_job_id AND state = 'failed'
        RETURNING *
    )
    INSERT INTO awa.jobs_dlq (
        id, kind, queue, args, state, priority, attempt, max_attempts,
        run_at, heartbeat_at, deadline_at, attempted_at, finalized_at,
        created_at, errors, metadata, tags, unique_key, unique_states,
        callback_id, callback_timeout_at, callback_filter, callback_on_complete,
        callback_on_fail, callback_transform, run_lease, progress,
        dlq_reason, dlq_at, original_run_lease
    )
    SELECT
        id, kind, queue, args, 'failed'::awa.job_state, priority, attempt,
        max_attempts, run_at, NULL, NULL, attempted_at, COALESCE(finalized_at, now()),
        created_at, errors, metadata, tags, unique_key, unique_states,
        NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL,
        p_reason, now(), run_lease
    FROM moved
    RETURNING *;
$$ LANGUAGE sql;

INSERT INTO awa.schema_version (version, description)
VALUES (8, 'First-class Dead Letter Queue (awa.jobs_dlq)')
ON CONFLICT (version) DO NOTHING;

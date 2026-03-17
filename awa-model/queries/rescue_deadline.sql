-- Rescue jobs that exceeded their hard deadline (runaway protection).
-- $1: max jobs to rescue per sweep
UPDATE awa.jobs
SET state = 'retryable',
    finalized_at = now(),
    errors = errors || jsonb_build_object(
        'error', 'hard deadline exceeded',
        'attempt', attempt,
        'at', now()
    )::jsonb
WHERE id IN (
    SELECT id FROM awa.jobs
    WHERE state = 'running'
      AND deadline_at IS NOT NULL
      AND deadline_at < now()
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
RETURNING *;

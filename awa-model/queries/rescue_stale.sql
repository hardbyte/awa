-- Rescue jobs with stale heartbeats (crash detection).
-- $1: staleness threshold interval (e.g., '90 seconds')
-- $2: max jobs to rescue per sweep
UPDATE awa.jobs
SET state = 'retryable',
    finalized_at = now(),
    errors = errors || jsonb_build_object(
        'error', 'heartbeat stale: worker presumed dead',
        'attempt', attempt,
        'at', now()
    )::jsonb
WHERE id IN (
    SELECT id FROM awa.jobs
    WHERE state = 'running'
      AND heartbeat_at < now() - $1::interval
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
RETURNING *;

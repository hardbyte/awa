-- Batch heartbeat update for in-flight jobs.
-- $1: array of job IDs
UPDATE awa.jobs
SET heartbeat_at = now()
WHERE id = ANY($1) AND state = 'running';

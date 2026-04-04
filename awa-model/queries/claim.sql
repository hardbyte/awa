-- Dequeue jobs using FOR UPDATE SKIP LOCKED with strict priority ordering.
--
-- $1: queue name
-- $2: batch limit
-- $3: deadline interval in seconds
--
-- Uses idx_awa_jobs_hot_dequeue (queue, priority, run_at, id) WHERE state = 'available'.
-- Cross-priority fairness is handled by the maintenance leader's priority aging
-- task, which periodically promotes the priority column for long-waiting jobs.
--
-- IMPORTANT: The selection MUST use a CTE, not a FROM-subquery. PostgreSQL's
-- planner can merge a FROM-subquery with the UPDATE target when both reference
-- the same table, which under concurrent load causes the LIMIT to be ignored
-- and ALL matching rows to be updated instead of just the batch.
WITH claimed AS (
    SELECT id
    FROM awa.jobs_hot
    WHERE state = 'available'
      AND queue = $1
      AND run_at <= now()
      AND NOT EXISTS (
          SELECT 1 FROM awa.queue_meta
          WHERE queue = $1 AND paused = TRUE
      )
    ORDER BY priority ASC, run_at ASC, id ASC
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE awa.jobs_hot
SET state = 'running',
    attempt = attempt + 1,
    run_lease = run_lease + 1,
    attempted_at = now(),
    heartbeat_at = now(),
    deadline_at = now() + make_interval(secs => $3)
FROM claimed
WHERE awa.jobs_hot.id = claimed.id
  AND awa.jobs_hot.state = 'available'
RETURNING awa.jobs_hot.*;

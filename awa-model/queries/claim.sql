-- Dequeue jobs using FOR UPDATE SKIP LOCKED with strict priority ordering.
--
-- $1: queue name
-- $2: batch limit
-- $3: deadline interval in seconds
--
-- Uses idx_awa_jobs_hot_dequeue (queue, priority, run_at, id) WHERE state = 'available'.
-- Cross-priority fairness is handled by the maintenance leader's priority aging
-- task, which periodically promotes the priority column for long-waiting jobs.
UPDATE awa.jobs_hot
SET state = 'running',
    attempt = attempt + 1,
    run_lease = run_lease + 1,
    attempted_at = now(),
    heartbeat_at = now(),
    deadline_at = now() + make_interval(secs => $3)
FROM (
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
) AS claimed
WHERE awa.jobs_hot.id = claimed.id
  AND awa.jobs_hot.state = 'available'
RETURNING awa.jobs_hot.*;

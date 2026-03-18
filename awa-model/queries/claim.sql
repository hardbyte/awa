-- Dequeue jobs using SKIP LOCKED with priority aging.
-- $1: queue name
-- $2: batch limit
-- $3: deadline interval (e.g., '5 minutes')
-- $4: priority aging interval in seconds
WITH claimed AS (
    SELECT id
    FROM awa.jobs
    WHERE state = 'available'
      AND queue = $1
      AND run_at <= now()
      AND NOT EXISTS (
          SELECT 1 FROM awa.queue_meta
          WHERE queue = $1 AND paused = TRUE
      )
    ORDER BY
      -- Priority aging: boost by 1 level per aging_interval of wait time
      GREATEST(1, priority - FLOOR(EXTRACT(EPOCH FROM (now() - run_at)) / $4)::int) ASC,
      run_at ASC,
      id ASC
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE awa.jobs
SET state = 'running',
    attempt = attempt + 1,
    attempted_at = now(),
    heartbeat_at = now(),
    deadline_at = now() + $3::interval
FROM claimed
WHERE awa.jobs.id = claimed.id
RETURNING awa.jobs.*;

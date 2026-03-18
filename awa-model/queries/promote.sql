-- Promote scheduled jobs that are now due.
UPDATE awa.jobs
SET state = 'available'
WHERE state = 'scheduled'
  AND run_at <= now()
RETURNING *;

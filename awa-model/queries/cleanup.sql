-- Clean up completed jobs older than retention period.
-- $1: completed retention interval (e.g., '24 hours')
-- $2: failed/cancelled retention interval (e.g., '72 hours')
-- $3: max rows to delete per sweep
DELETE FROM awa.jobs
WHERE id IN (
    SELECT id FROM awa.jobs
    WHERE (state = 'completed' AND finalized_at < now() - $1::interval)
       OR (state IN ('failed', 'cancelled') AND finalized_at < now() - $2::interval)
    LIMIT $3
);

-- Queue lag and depth statistics.
SELECT
    queue,
    count(*) FILTER (WHERE state = 'available') AS available,
    count(*) FILTER (WHERE state = 'running') AS running,
    count(*) FILTER (WHERE state = 'failed') AS failed,
    count(*) FILTER (WHERE state = 'completed'
        AND finalized_at > now() - interval '1 hour') AS completed_last_hour,
    EXTRACT(EPOCH FROM (now() - min(run_at) FILTER (WHERE state = 'available'))) AS lag_seconds
FROM awa.jobs
GROUP BY queue;

WITH targets AS MATERIALIZED (
    SELECT pid, backend_start::text, datid, datname, application_name, state,
           query_start::text, xact_start::text, wait_event_type, wait_event,
           query_id, left(query,4096) AS query_text
    FROM pg_stat_activity
    WHERE application_name = ANY($1::text[])
      AND backend_type = 'client backend'
      AND datname LIKE 'awa_footprint_%'
      AND pid <> pg_backend_pid()
), locks AS MATERIALIZED (
    SELECT locks.pid, locks.database, locks.relation, locks.mode,
           locks.granted, locks.fastpath
    FROM pg_locks AS locks
    JOIN targets ON targets.pid = locks.pid
    WHERE locks.locktype = 'relation'
), counts AS (
    SELECT pid,
           count(*) FILTER (WHERE granted) AS granted_rows,
           count(*) FILTER (WHERE granted AND fastpath) AS fastpath_rows,
           count(*) FILTER (WHERE granted AND NOT fastpath) AS nonfastpath_rows,
           count(*) FILTER (WHERE NOT granted) AS waiting_rows,
           count(*) FILTER (WHERE granted AND NOT fastpath AND mode IN
               ('AccessShareLock', 'RowShareLock', 'RowExclusiveLock')) AS nonfastpath_weak_rows,
           count(DISTINCT (database, relation)) FILTER (WHERE granted) AS distinct_relations
    FROM locks GROUP BY pid
), modes AS (
    SELECT pid, jsonb_agg(to_jsonb(grouped) - 'pid') AS mode_counts
    FROM (
        SELECT pid, mode, granted, fastpath, count(*) AS rows
        FROM locks GROUP BY pid, mode, granted, fastpath
    ) AS grouped GROUP BY pid
)
SELECT clock_timestamp()::text AS server_observed_at,
       COALESCE(jsonb_agg(to_jsonb(targets) || jsonb_build_object(
           'granted_rows', COALESCE(counts.granted_rows, 0),
           'fastpath_rows', COALESCE(counts.fastpath_rows, 0),
           'nonfastpath_rows', COALESCE(counts.nonfastpath_rows, 0),
           'waiting_rows', COALESCE(counts.waiting_rows, 0),
           'nonfastpath_weak_rows', COALESCE(counts.nonfastpath_weak_rows, 0),
           'distinct_relations', COALESCE(counts.distinct_relations, 0),
           'mode_counts', COALESCE(modes.mode_counts, '[]'::jsonb)
       )) FILTER (WHERE targets.pid IS NOT NULL), '[]'::jsonb) AS backends
FROM targets
LEFT JOIN counts USING (pid)
LEFT JOIN modes USING (pid)

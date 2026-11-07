WITH current_db AS (
    SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database()
),
relations AS MATERIALIZED (
    SELECT c.oid, c.relname::text AS relation
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = $1 AND c.relname::text = ANY($2::text[])
),
activity AS MATERIALIZED (
    SELECT pid, leader_pid, backend_start, application_name, usename, state,
           wait_event_type, wait_event, xact_start, query_start,
           EXTRACT(EPOCH FROM (clock_timestamp() - xact_start))::float8 AS tx_age_s,
           EXTRACT(EPOCH FROM (clock_timestamp() - query_start))::float8 AS query_age_s,
           left(query, 512) AS query,
           query IS NULL OR query = '<insufficient privilege>' AS visibility_limited
    FROM pg_catalog.pg_stat_activity
    WHERE datid = (SELECT oid FROM current_db)
),
locks AS MATERIALIZED (
    SELECT l.pid, r.oid AS relation_oid, r.relation, l.mode, l.granted, l.fastpath,
           l.virtualtransaction, l.waitstart
    FROM pg_catalog.pg_locks l
    JOIN relations r ON r.oid = l.relation
    LEFT JOIN activity a ON a.pid = l.pid
    WHERE l.database = (SELECT oid FROM current_db)
      AND l.locktype = 'relation'
      AND l.pid IS DISTINCT FROM pg_backend_pid()
    ORDER BY l.granted, a.xact_start NULLS LAST, a.query_start NULLS LAST,
             l.pid NULLS FIRST, r.relation, l.mode
    LIMIT 257 --256 retained rows plus an explicit overflow sentinel
),
interested AS MATERIALIZED (
    SELECT DISTINCT l.pid
    FROM locks l
    JOIN activity a ON a.pid = l.pid
    WHERE a.wait_event_type = 'Lock'
    ORDER BY l.pid
    LIMIT 32
),
blockers AS MATERIALIZED (
    SELECT i.pid, pg_catalog.pg_blocking_pids(i.pid) AS pids
    FROM interested i
),
lock_rows AS MATERIALIZED (
SELECT clock_timestamp() AS observed_at, l.*,
       a.leader_pid, a.backend_start, a.application_name, a.usename, a.state,
       a.wait_event_type, a.wait_event, a.xact_start, a.query_start,
       a.tx_age_s, a.query_age_s, a.query, a.visibility_limited,
       a.pid IS NULL AS activity_missing,
       b.pids[1:16] AS blocking_pids,
       b.pid IS NOT NULL AS blocking_pids_observed,
       COALESCE(cardinality(b.pids) > 16, false) AS blocking_pids_truncated
FROM locks l
LEFT JOIN activity a ON a.pid = l.pid
LEFT JOIN blockers b ON b.pid = l.pid
)
SELECT jsonb_build_object(
    'server_statement_at', statement_timestamp(),
    'server_observed_at', clock_timestamp(),
    'resolved_relations', COALESCE((SELECT jsonb_agg(to_jsonb(r)) FROM relations r), '[]'::jsonb),
    'locks', COALESCE((SELECT jsonb_agg(to_jsonb(l) ORDER BY l.granted, l.xact_start NULLS LAST, l.pid NULLS FIRST, l.relation, l.mode) FROM lock_rows l), '[]'::jsonb)
);

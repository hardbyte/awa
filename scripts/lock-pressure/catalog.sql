WITH rings AS (
    SELECT 'queue'::text AS family, 'ready_slot'::text AS partition_key,
           s.current_slot, s.generation AS ring_generation, s.slot_count,
           slots.slot, slots.generation AS slot_generation
    FROM awa.queue_ring_state s CROSS JOIN awa.queue_ring_slots slots WHERE s.singleton AND slots.generation>=0 AND slots.generation<=s.generation
    UNION ALL
    SELECT 'lease', 'lease_slot', s.current_slot, s.generation, s.slot_count,
           slots.slot, slots.generation
    FROM awa.lease_ring_state s CROSS JOIN awa.lease_ring_slots slots WHERE s.singleton AND slots.generation>=0 AND slots.generation<=s.generation
    UNION ALL
    SELECT 'claim', 'claim_slot', s.current_slot, s.generation, s.slot_count,
           slots.slot, slots.generation
    FROM awa.claim_ring_state s CROSS JOIN awa.claim_ring_slots slots WHERE s.singleton AND slots.generation>=0 AND slots.generation<=s.generation
)
SELECT clock_timestamp() AS observed_at, current_database() AS database_name,
       (SELECT oid::bigint FROM pg_database WHERE datname=current_database()) AS database_oid,
       pg_backend_pid() AS observer_pid,
       (SELECT backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid()) AS observer_backend_start,
       current_setting('application_name') AS observer_application,
       rings.*, rings.slot <> rings.current_slot AS sealed,
       ns.nspname AS schema_name, parent.relname AS parent_name,
       child.relname AS child_name, child.oid::bigint AS child_oid,
       pg_relation_filenode(child.oid)::bigint AS filenode,
       pg_get_partkeydef(parent.oid) AS partition_definition,
       pg_table_size(child.oid) AS table_bytes,
       pg_indexes_size(child.oid) AS index_bytes,
       pg_total_relation_size(child.oid) AS total_bytes,
       stats.n_live_tup AS estimated_live_rows,
       stats.n_dead_tup AS estimated_dead_rows, stats.last_analyze, stats.last_autoanalyze
FROM rings
JOIN pg_namespace ns ON ns.nspname='awa'
JOIN pg_class parent ON parent.relnamespace=ns.oid AND parent.relkind='p'
    AND pg_get_partkeydef(parent.oid)=format('LIST (%I)', rings.partition_key)
JOIN pg_inherits inheritance ON inheritance.inhparent=parent.oid
JOIN pg_class child ON child.oid=inheritance.inhrelid AND child.relkind='r'
    AND child.relnamespace=ns.oid AND child.relname=parent.relname || '_' || rings.slot::text
LEFT JOIN pg_stat_all_tables stats ON stats.relid=child.oid
ORDER BY rings.family, rings.slot, parent.relname;

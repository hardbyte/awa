-- v050: drop the redundant lane index on ready_entries partitions
--
-- Every ready partition carries `idx_<schema>_ready_<slot>_lane_shard` on
-- (queue, priority, enqueue_shard, lane_seq) next to its primary key on
-- (ready_slot, queue, priority, enqueue_shard, lane_seq). Every per-lane
-- lookup on ready_entries pins ready_slot (claims, prune and fold gates,
-- the maintenance lag gauge via ready_segments), so the primary key serves
-- the same scans. The extra index costs one btree insert per enqueued job
-- and one metapage image per rotation.
--
-- This file is the migration tail. `awa migrate` and
-- `awa migrate --sql --from 49 --to 50` run V23_UP first, which only
-- redefines the installer function so later installs omit the index; the
-- default substrate is not replayed. This tail drops the existing copies in
-- every discovered queue-storage schema, in slot order.
--
-- Locks: each `DROP INDEX` takes ACCESS EXCLUSIVE on its ready partition and
-- holds it until the migration transaction commits. No rows are scanned or
-- rewritten. When no lock_timeout is set, the tail waits at most 2 s for each
-- partition; a timeout (SQLSTATE 55P03) rolls the whole migration back and it
-- can be re-run.
-- Tracking: https://github.com/hardbyte/awa/issues/522

DO $$
DECLARE
    v_schema TEXT;
    v_parent_oid OID;
    v_slot INT;
    v_previous_lock_timeout TEXT := current_setting('lock_timeout');
BEGIN
    IF v_previous_lock_timeout = '0' THEN
        PERFORM set_config('lock_timeout', '2s', true);
    END IF;

    FOR v_schema IN
        SELECT n.nspname
        FROM pg_namespace AS n
        WHERE has_schema_privilege(current_user, n.oid, 'USAGE')
          AND EXISTS (
              SELECT 1 FROM pg_proc AS awa_p
              WHERE awa_p.pronamespace = n.oid
                AND awa_p.proname = 'claim_ready_runtime'
                AND oidvectortypes(awa_p.proargtypes)
                    = 'text, bigint, double precision, double precision'
          )
        ORDER BY n.nspname
    LOOP
        v_parent_oid := to_regclass(format('%I.ready_entries', v_schema));
        IF v_parent_oid IS NULL THEN
            CONTINUE;
        END IF;

        FOR v_slot IN
            SELECT substring(c.relname FROM '^ready_entries_([0-9]+)$')::int AS slot
            FROM pg_inherits AS i
            JOIN pg_class AS c ON c.oid = i.inhrelid
            WHERE i.inhparent = v_parent_oid
              AND c.relname ~ '^ready_entries_[0-9]+$'
            ORDER BY 1
        LOOP
            EXECUTE format(
                'DROP INDEX IF EXISTS %I.%I',
                v_schema,
                format('idx_%s_ready_%s_lane_shard', v_schema, v_slot)
            );
        END LOOP;
    END LOOP;

    PERFORM set_config('lock_timeout', v_previous_lock_timeout, true);
END;
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (50, 'Drop the redundant lane index on ready_entries partitions')
ON CONFLICT (version) DO NOTHING;

-- v050: drop the redundant lane index on ready_entries partitions
--
-- Every ready partition carries `idx_<schema>_ready_<slot>_lane_shard` on
-- (queue, priority, enqueue_shard, lane_seq) next to its primary key on
-- (ready_slot, queue, priority, enqueue_shard, lane_seq). A partition holds
-- one ready_slot value and every lane lookup in `claim_ready_runtime`
-- pins ready_slot, so the primary key serves the same scans. The extra
-- index costs one btree insert per enqueued job and one metapage image per
-- rotation.
--
-- This file is the migration tail. `awa migrate` and
-- `awa migrate --sql --from 49 --to 50` run V23_UP first so the installer no
-- longer recreates the index on the next `prepare_schema`; this tail drops
-- the existing copies in every discovered queue-storage schema.
--
-- `DROP INDEX` takes an ACCESS EXCLUSIVE lock on each ready partition until
-- the transaction commits, so in-flight enqueues and claims on that
-- partition wait for the drop and the drop waits for them. No rows are
-- scanned or rewritten. Use a bounded lock_timeout and a quiet window.
-- Tracking: https://github.com/hardbyte/awa/issues/522

DO $$
DECLARE
    v_schema TEXT;
    v_parent_oid OID;
    v_child_name TEXT;
    v_slot TEXT;
BEGIN
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
    LOOP
        v_parent_oid := to_regclass(format('%I.ready_entries', v_schema));
        IF v_parent_oid IS NULL THEN
            CONTINUE;
        END IF;

        FOR v_child_name IN
            SELECT c.relname
            FROM pg_inherits AS i
            JOIN pg_class AS c ON c.oid = i.inhrelid
            WHERE i.inhparent = v_parent_oid
        LOOP
            v_slot := substring(v_child_name FROM length('ready_entries') + 2);
            EXECUTE format(
                'DROP INDEX IF EXISTS %I.%I',
                v_schema,
                format('idx_%s_ready_%s_lane_shard', v_schema, v_slot)
            );
        END LOOP;
    END LOOP;
END;
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (50, 'Drop the redundant lane index on ready_entries partitions')
ON CONFLICT (version) DO NOTHING;

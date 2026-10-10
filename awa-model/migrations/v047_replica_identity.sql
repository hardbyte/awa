-- v047: Replica identity for every awa table without a primary key.
--
-- PostgreSQL refuses UPDATE and DELETE on a table that is in a logical
-- replication publication and has no replica identity (no primary key,
-- no REPLICA IDENTITY index, not FULL). A `CREATE PUBLICATION ... FOR ALL
-- TABLES` (Debezium's default) or `FOR TABLES IN SCHEMA awa` therefore made
-- the maintenance leader fail every tick with
--   cannot delete from table "queue_terminal_rollup_deltas" because it does
--   not have a replica identity and publishes deletes
-- and the same for the admin dirty-mark drain and the unique-claim release.
--
-- Choice per table:
--   * admin_dirty_queue_marks / admin_dirty_kind_marks: FULL. ADR-045 forbids
--     any index or constraint on these tables, and the rows are ~40 bytes.
--   * job_unique_claims: USING INDEX idx_awa_jobs_unique, the uniqueness
--     index that already exists; the old-row image carries only the key.
--   * queue_terminal_count_deltas (+ partitions) and
--     queue_terminal_rollup_deltas: FULL, set by install_queue_storage_substrate
--     (v023, re-run below) so fresh installs and custom schemas get it too.
--     The hot path only INSERTs into these; FULL changes nothing there.
--
-- N-1 (0.7.x) binaries are unaffected: replica identity only changes what
-- the WAL records for UPDATE/DELETE and no query, trigger, or function body
-- depends on it. Every ALTER TABLE ... REPLICA IDENTITY is a catalog-only
-- change that takes a momentary ACCESS EXCLUSIVE lock on its table and is
-- guarded so a re-run takes no lock at all. Expected wall time: milliseconds
-- on any data volume; the lock on the mark tables queues job transitions
-- until the migration transaction commits, so apply v047 on its own rather
-- than at the end of a long pending range if that window matters.

DO $$
BEGIN
    IF (SELECT relreplident FROM pg_class WHERE oid = 'awa.admin_dirty_queue_marks'::regclass) <> 'f' THEN
        ALTER TABLE awa.admin_dirty_queue_marks REPLICA IDENTITY FULL;
    END IF;

    IF (SELECT relreplident FROM pg_class WHERE oid = 'awa.admin_dirty_kind_marks'::regclass) <> 'f' THEN
        ALTER TABLE awa.admin_dirty_kind_marks REPLICA IDENTITY FULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_index
        WHERE indrelid = 'awa.job_unique_claims'::regclass
          AND indisreplident
    ) THEN
        ALTER TABLE awa.job_unique_claims REPLICA IDENTITY USING INDEX idx_awa_jobs_unique;
    END IF;
END
$$;

-- Custom queue-storage schemas that were prepared before this release and
-- are not re-prepared by `awa migrate`. Same relation set and setting as
-- install_queue_storage_substrate applies.
DO $$
DECLARE
    v_rel REGCLASS;
BEGIN
    FOR v_rel IN
        SELECT c.oid::regclass
        FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE has_schema_privilege(current_user, n.oid, 'USAGE')
          AND EXISTS (
              SELECT 1 FROM pg_proc AS p
              WHERE p.pronamespace = n.oid
                AND p.proname = 'claim_ready_runtime'
          )
          AND c.relkind IN ('r', 'p')
          AND c.relreplident <> 'f'
          AND c.relname ~ '^queue_terminal_(count_deltas(_[0-9]+)?|rollup_deltas)$'
        ORDER BY n.nspname, c.relname
    LOOP
        EXECUTE format('ALTER TABLE %s REPLICA IDENTITY FULL', v_rel);
    END LOOP;
END
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (47, 'Replica identity for tables without a primary key')
ON CONFLICT (version) DO NOTHING;

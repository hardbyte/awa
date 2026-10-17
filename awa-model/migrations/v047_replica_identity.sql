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
-- depends on it. The guarded identity ALTERs take ACCESS EXCLUSIVE locks;
-- the full version bundle also replays V23_UP, reinstalls the canonical
-- substrate and drops/recreates triggers on ring state and queue heads.
-- These locks can wait behind live transactions and are held until the
-- entire pending migration transaction commits. There is no fixed wall-time
-- guarantee. Use a bounded lock_timeout and an operational quiet window;
-- after a timeout, retry the complete generated bundle transaction.
-- External runners must use `awa migrate --sql --from 46 --to 47`, which
-- includes the current installer before this tail, and preserve the generated
-- cron preflight before replay; SQL export does not run Rust preflights.
-- Raw v047 alone omits installer refresh/replay.
-- The DDL role must own (or inherit ownership of) all installed substrates.
-- Tracking: https://github.com/hardbyte/awa/pull/509
-- Review: https://github.com/hardbyte/awa/pull/509#discussion_r4236544554
-- Released-artifact rehearsal tracking: https://github.com/hardbyte/awa/issues/427

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
        WHERE n.nspname IN (SELECT discovered.schema_name FROM awa.queue_storage_schemas() AS discovered)
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

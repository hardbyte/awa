-- Schema patch replica_identity: give every awa table a replica identity
-- (awa 0.7 migration v047, minus the schema_version row).
--
-- PostgreSQL rejects UPDATE and DELETE on a table that is in a logical
-- replication publication and has no primary key, replica-identity index or
-- FULL identity. Tables without one on 0.6:
--   * awa.admin_dirty_queue_marks, awa.admin_dirty_kind_marks: FULL. ADR-045
--     forbids any index or constraint on them.
--   * awa.job_unique_claims: USING INDEX idx_awa_jobs_unique.
--   * <schema>.queue_terminal_count_deltas and each of its partitions, in
--     every queue-storage schema: FULL. Partitions do not inherit the
--     parent's setting.
-- Tracking: https://github.com/hardbyte/awa/pull/509
--
-- Applied after the v023 definitions, whose installer sets the delta tables'
-- identity in schemas prepared later.
--
-- Live load: each ALTER takes ACCESS EXCLUSIVE on its table (and SHARE on
-- idx_awa_jobs_unique), held until the patch's transaction commits. Each lock
-- wait is bounded by the session's lock_timeout, or 300ms when none is set;
-- a timeout aborts the patch and leaves nothing changed. A table that already
-- has its identity is not altered, so a re-run locks nothing.
--
-- 0.6 delivery: the 0.6 series cannot add a migration version, so this ships as
-- an idempotent schema patch. `awa migrate` applies it when the schema is
-- exactly at v040 and the probe in migrations.rs reports it missing;
-- `awa migrate --sql` prints it as a repeatable R__ script for external
-- runners. Replica identity only changes what the WAL records for UPDATE and
-- DELETE, so released 0.6 binaries run the same against a patched or an
-- unpatched schema. Run it as the owner of the awa and queue-storage tables.

DO $$
DECLARE
    v_lock_timeout TEXT := current_setting('lock_timeout');
    v_target RECORD;
BEGIN
    IF v_lock_timeout = '0' THEN
        PERFORM set_config('lock_timeout', '300ms', true);
    END IF;

    FOR v_target IN
        SELECT c.oid::regclass AS relation, 'FULL' AS identity, 0 AS ord,
               n.nspname, c.relkind, c.relname
        FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE n.nspname = 'awa'
          AND c.relname IN ('admin_dirty_queue_marks', 'admin_dirty_kind_marks')
          AND c.relkind = 'r'
          AND c.relreplident <> 'f'
        UNION ALL
        SELECT c.oid::regclass, 'USING INDEX idx_awa_jobs_unique', 1,
               n.nspname, c.relkind, c.relname
        FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE n.nspname = 'awa'
          AND c.relname = 'job_unique_claims'
          AND to_regclass('awa.idx_awa_jobs_unique') IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM pg_index AS i
              WHERE i.indrelid = c.oid AND i.indisreplident
          )
        UNION ALL
        SELECT c.oid::regclass, 'FULL', 2, n.nspname, c.relkind, c.relname
        FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE has_schema_privilege(current_user, n.oid, 'USAGE')
          AND EXISTS (
              SELECT 1 FROM pg_proc AS awa_p
              WHERE awa_p.pronamespace = n.oid
                AND awa_p.proname = 'claim_ready_runtime'
                AND oidvectortypes(awa_p.proargtypes)
                    = 'text, bigint, double precision, double precision'
          )
          AND c.relkind IN ('r', 'p')
          AND c.relreplident <> 'f'
          AND c.relname ~ '^queue_terminal_count_deltas(_[0-9]+)?$'
        ORDER BY ord, nspname, relkind, relname
    LOOP
        BEGIN
            EXECUTE format('ALTER TABLE %s REPLICA IDENTITY %s', v_target.relation, v_target.identity);
        EXCEPTION WHEN lock_not_available THEN
            RAISE EXCEPTION 'replica_identity: could not lock % within lock_timeout %',
                v_target.relation, current_setting('lock_timeout')
                USING ERRCODE = 'lock_not_available',
                      HINT = 'Nothing was changed. Run awa migrate again, or raise lock_timeout for the migrating session.';
        END;
    END LOOP;

    PERFORM set_config('lock_timeout', v_lock_timeout, true);
END;
$$;

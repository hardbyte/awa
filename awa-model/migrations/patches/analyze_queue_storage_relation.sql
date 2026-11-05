-- Schema patch analyze_queue_storage_relation: planner statistics helper for
-- the queue-storage tables autovacuum never analyses (#631).
-- Counterpart of awa 0.7 migration v053, minus the schema_version row; v053
-- replaces this body with one that checks the schema through
-- awa.queue_storage_schemas(), which 0.6 does not have.
--
-- 0.6 delivery: the 0.6 series cannot add a migration version (0.7 owns v041
-- and above), so this ships as an idempotent schema patch. `awa migrate`
-- applies it when the schema is exactly at v040 and the probe in
-- migrations.rs reports it missing; `awa migrate --sql` prints it as a
-- repeatable R__ script for external runners.
--
-- Autovacuum does not analyse partitioned tables, so the ring parents have no
-- inherited statistics, and it never reaches the autoanalyze threshold on the
-- two small head tables (queue_claim_heads, queue_enqueue_heads). The
-- maintenance leader refreshes both on an interval. Runtimes usually connect
-- as a role that does not own the tables, and ANALYZE skips with a warning a
-- table its caller may not analyse, so the refresh runs through this
-- SECURITY DEFINER function owned by the role that applies the patch, which
-- owns the tables.
--
-- The function analyses one relation per call and the caller runs each call as
-- its own transaction, so the locks ANALYZE takes on a partitioned table's
-- children are held for that table rather than for the whole refresh.
--
-- Locking on a partitioned table, per the ANALYZE documentation: SKIP_LOCKED
-- applies when ANALYZE starts work on a relation, so a child that ring prune
-- holds ACCESS EXCLUSIVE is skipped during the per-child analysis, but
-- sampling the children for the parent's statistics can still wait for it.
-- That wait is bounded by the caller's lock_timeout and fails the call with
-- SQLSTATE 55P03, which the caller records as skipped_lock. SKIP_LOCKED also
-- skips every partition silently when the parent itself is locked, so the
-- function takes the parent's SHARE UPDATE EXCLUSIVE lock first and lets that
-- wait fail instead.
--
-- Definer boundary: only a head table or a partitioned table of a queue-storage
-- schema (the AWA-specific claim_ready_runtime sentinel) that is owned by the
-- owner of the awa schema is analysed, from quoted identifiers only. ANALYZE
-- writes statistics and nothing else.
--
-- Returns 'analyzed'; 'skipped_privilege' when the function owner may not
-- analyse the relation; or 'missing' when the relation does not exist.
-- Takes no lock on any queue-storage table and completes in milliseconds.
-- Releases before the patch never call it; a binary that runs before the patch
-- logs once that the function is missing and skips the refresh.

CREATE OR REPLACE FUNCTION awa.analyze_queue_storage_relation(
    p_schema   TEXT,
    p_relation TEXT
)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_relid       OID;
    v_relkind     "char";
    v_relowner    OID;
    v_may_analyze BOOLEAN;
BEGIN
    IF to_regprocedure(format(
        '%I.claim_ready_runtime(text,bigint,double precision,double precision)',
        p_schema
    )) IS NULL THEN
        RAISE EXCEPTION '% is not a queue-storage schema', p_schema
            USING ERRCODE = '22023';
    END IF;

    v_relid := to_regclass(format('%I.%I', p_schema, p_relation));
    IF v_relid IS NULL THEN
        RETURN 'missing';
    END IF;

    SELECT class.relkind, class.relowner INTO v_relkind, v_relowner
    FROM pg_class AS class WHERE class.oid = v_relid;
    IF NOT (
        v_relkind = 'p'
        OR p_relation IN ('queue_claim_heads', 'queue_enqueue_heads')
    ) THEN
        RAISE EXCEPTION '%.% is not a partitioned or head table of a queue-storage schema',
            p_schema, p_relation
            USING ERRCODE = '22023';
    END IF;

    IF v_relowner IS DISTINCT FROM (
        SELECT namespace.nspowner FROM pg_namespace AS namespace WHERE namespace.nspname = 'awa'
    ) THEN
        RAISE EXCEPTION '%.% is not owned by the awa schema owner', p_schema, p_relation
            USING ERRCODE = '42501';
    END IF;

    -- ANALYZE skips, with a warning rather than an error, a table the caller
    -- may not analyse: the owner, a MAINTAIN holder (PostgreSQL 17+), a
    -- member of pg_maintain, or the database owner may analyse it.
    SELECT
        pg_has_role(current_user, v_relowner, 'USAGE')
        OR pg_has_role(
            current_user,
            (SELECT database.datdba FROM pg_database AS database
             WHERE database.datname = current_database()),
            'USAGE'
        )
        OR CASE
            WHEN current_setting('server_version_num')::int >= 170000
                THEN has_table_privilege(v_relid, 'MAINTAIN')
            ELSE FALSE
        END
    INTO v_may_analyze;

    IF NOT v_may_analyze THEN
        RETURN 'skipped_privilege';
    END IF;

    IF v_relkind = 'p' THEN
        EXECUTE format(
            'LOCK TABLE ONLY %I.%I IN SHARE UPDATE EXCLUSIVE MODE', p_schema, p_relation
        );
        EXECUTE format('ANALYZE (SKIP_LOCKED) %I.%I', p_schema, p_relation);
    ELSE
        EXECUTE format('ANALYZE %I.%I', p_schema, p_relation);
    END IF;
    RETURN 'analyzed';
END
$$;

GRANT EXECUTE ON FUNCTION awa.analyze_queue_storage_relation(TEXT, TEXT) TO PUBLIC;
